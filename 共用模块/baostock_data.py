# -*- coding: utf-8 -*-
"""
Baostock 数据获取层 — 估值指标 + 行业分类
================================================
用于替换 stock_picker.py 中的硬编码因子和失效的 PB 计算

使用示例:
    from baostock_data import BaostockData
    bd = BaostockData()
    df_val = bd.get_valuation(["000001", "600000"])  # 获取估值指标
    df_ind = bd.get_industry(["000001", "600000"])  # 获取行业分类
"""

import os
import time
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Dict, List

import pandas as pd
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError

def log(msg, level="INFO"):
    """简易日志函数（防止异常路径调用未定义的log导致NameError崩溃）"""
    from datetime import datetime as _dt
    print(f"[{_dt.now().strftime('%H:%M:%S')}] [{level}] {msg}")


logger = logging.getLogger(__name__)

CACHE_TTL_HOURS = 24
BAOSTOCK_QUERY_TIMEOUT = 5  # Baostock 单只查询超时（秒）


def _query_with_timeout(query_func, timeout=BAOSTOCK_QUERY_TIMEOUT):
    """带超时的 Baostock 查询包装器，防止 SDK 阻塞"""
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(query_func)
        try:
            return future.result(timeout=timeout)
        except FutureTimeoutError:
            logger.warning(f"[baostock] 查询超时({timeout}s)，跳过")
            return None
        except Exception as e:
            logger.debug(f"[baostock] 查询异常: {e}")
            return None


class BaostockData:
    """
    Baostock 数据封装器（估值指标 + 行业分类）
    """

    def __init__(self, cache_dir: Optional[Path] = None):
        self.base_dir = Path(__file__).parent.parent
        self.cache_dir = cache_dir or (self.base_dir / "cache" / "baostock")
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self._logged_in = False

    # ── 登录管理 ──

    def _ensure_login(self) -> bool:
        """确保 Baostock 已登录（匿名即可）"""
        if self._logged_in:
            return True
        try:
            import baostock as bs
            lg = bs.login(user_id="anonymous", password="123456")
            if lg.error_code == "0":
                self._logged_in = True
                return True
            logger.warning(f"[baostock] 登录失败: {lg.error_msg}")
        except Exception as e:
            logger.warning(f"[baostock] 登录异常: {e}")
        return False

    def logout(self):
        if self._logged_in:
            try:
                import baostock as bs
                bs.logout()
            except Exception as e:
                log(f"操作异常: {e}", "WARN")
            self._logged_in = False

    # ── 缓存层 ──

    def _cache_path(self, name: str, date_str: str) -> Path:
        return self.cache_dir / f"{name}_{date_str}.parquet"

    def _load_cache(self, name: str) -> Optional[pd.DataFrame]:
        today = datetime.now().strftime("%Y%m%d")
        path = self._cache_path(name, today)
        if not path.exists():
            return None
        try:
            return pd.read_parquet(path)
        except Exception as e:
            log(f"操作异常: {e}", "WARN")
            return None

    def _save_cache(self, name: str, df: pd.DataFrame) -> None:
        today = datetime.now().strftime("%Y%m%d")
        path = self._cache_path(name, today)
        try:
            df.to_parquet(path, index=False)
        except Exception as e:
            logger.warning(f"[baostock] 缓存保存失败 {name}: {e}")

    # ── 估值指标 ──

    def get_valuation(self, codes: List[str]) -> pd.DataFrame:
        """
        批量获取股票估值指标（PE_TTM, PB_MRQ, PS_TTM, PCF_TTM）

        Returns
        -------
        pd.DataFrame
            列: [代码, peTTM, pbMRQ, psTTM, pcfNcfTTM, 日期]
        """
        # 1. 查缓存
        cache = self._load_cache("valuation")
        if cache is not None:
            cached_codes = set(cache["代码"].astype(str).str.zfill(6))
            missing = [c for c in codes if c.zfill(6) not in cached_codes]
            if not missing:
                return cache[cache["代码"].astype(str).str.zfill(6).isin([c.zfill(6) for c in codes])]
            codes = missing  # 只查缺失的
        else:
            cache = pd.DataFrame()

        if not self._ensure_login():
            return cache

        try:
            import baostock as bs
        except ImportError:
            logger.warning("[baostock] 未安装 baostock，跳过估值指标")
            return cache

        today = datetime.now().strftime("%Y-%m-%d")
        rows = []
        for code in codes:
            try:
                prefix = "sh" if code.startswith("6") else "sz"
                rs = _query_with_timeout(lambda: bs.query_history_k_data_plus(
                    f"{prefix}.{code}",
                    "date,code,peTTM,pbMRQ,psTTM,pcfNcfTTM",
                    start_date=today, end_date=today,
                    frequency="d"
                ))
                if rs is not None and rs.error_code == "0" and rs.next():
                    data = rs.get_row_data()
                    rows.append({
                        "代码": code.zfill(6),
                        "peTTM": float(data[2]) if data[2] else None,
                        "pbMRQ": float(data[3]) if data[3] else None,
                        "psTTM": float(data[4]) if data[4] else None,
                        "pcfNcfTTM": float(data[5]) if data[5] else None,
                        "日期": today,
                    })
            except Exception as e:
                logger.debug(f"[baostock] {code} 估值获取失败: {e}")
                continue

        if rows:
            df_new = pd.DataFrame(rows)
            df = pd.concat([cache, df_new], ignore_index=True)
            self._save_cache("valuation", df)
            return df[df["代码"].astype(str).str.zfill(6).isin([c.zfill(6) for c in codes])]
        return cache

    # ── 行业分类 ──

    def get_industry(self, codes: List[str]) -> pd.DataFrame:
        """
        批量获取股票行业分类

        Returns
        -------
        pd.DataFrame
            列: [代码, 行业, 行业代码]
        """
        # 1. 查缓存
        cache = self._load_cache("industry")
        if cache is not None:
            cached_codes = set(cache["代码"].astype(str).str.zfill(6))
            missing = [c for c in codes if c.zfill(6) not in cached_codes]
            if not missing:
                return cache[cache["代码"].astype(str).str.zfill(6).isin([c.zfill(6) for c in codes])]
            codes = missing
        else:
            cache = pd.DataFrame()

        if not self._ensure_login():
            return cache

        try:
            import baostock as bs
        except ImportError:
            logger.warning("[baostock] 未安装 baostock，跳过行业分类")
            return cache

        rows = []
        for code in codes:
            try:
                prefix = "sh" if code.startswith("6") else "sz"
                rs = _query_with_timeout(lambda: bs.query_stock_industry(
                    code=f"{prefix}.{code}",
                    date=datetime.now().strftime("%Y-%m-%d")
                ))
                if rs is not None and rs.error_code == "0" and rs.next():
                    data = rs.get_row_data()
                    # Baostock 字段: [updateDate, code, code_name, industry, industryClassification]
                    # data[3] 是行业分类，如 "C29橡胶和塑料制品业"
                    raw_industry = data[3] if len(data) > 3 else ""
                    # 清洗: 去掉前缀代码（如 "C29" → "橡胶和塑料制品业"）
                    import re
                    industry_clean = re.sub(r'^[A-Z]\d+', '', raw_industry) if raw_industry else ""
                    rows.append({
                        "代码": code.zfill(6),
                        "行业": industry_clean,
                        "行业代码": data[4] if len(data) > 4 else "",
                    })
            except Exception as e:
                logger.debug(f"[baostock] {code} 行业获取失败: {e}")
                continue

        if rows:
            df_new = pd.DataFrame(rows)
            df = pd.concat([cache, df_new], ignore_index=True)
            self._save_cache("industry", df)
            return df[df["代码"].astype(str).str.zfill(6).isin([c.zfill(6) for c in codes])]
        return cache

    # ── 便捷函数 ──

    def get_pb_for_codes(self, codes: List[str]) -> Dict[str, float]:
        """返回 {代码: PB} 字典，用于修复 PB 反向因子"""
        df = self.get_valuation(codes)
        if df.empty:
            return {}
        return {
            str(row["代码"]).zfill(6): float(row["pbMRQ"])
            for _, row in df.iterrows()
            if pd.notna(row.get("pbMRQ"))
        }

    def get_industry_map(self, codes: List[str]) -> Dict[str, str]:
        """返回 {代码: 行业名} 字典，用于替换硬编码行业"""
        df = self.get_industry(codes)
        if df.empty:
            return {}
        return {
            str(row["代码"]).zfill(6): str(row.get("行业", ""))
            for _, row in df.iterrows()
        }

    # ── 财务数据（季频盈利能力）──

    def get_profit_data(self, codes: List[str]) -> pd.DataFrame:
        """
        批量获取季频盈利能力（ROE + 总股本 + 流通股本）

        Returns
        -------
        pd.DataFrame
            列: [代码, roeAvg, total_share, liqa_share, 季度]
        """
        cache = self._load_cache("profit")
        if cache is not None:
            cached_codes = set(cache["代码"].astype(str).str.zfill(6))
            missing = [c for c in codes if c.zfill(6) not in cached_codes]
            if not missing:
                return cache[cache["代码"].astype(str).str.zfill(6).isin([c.zfill(6) for c in codes])]
            codes = missing
        else:
            cache = pd.DataFrame()

        if not self._ensure_login():
            return cache

        try:
            import baostock as bs
        except ImportError:
            logger.warning("[baostock] 未安装 baostock，跳过财务数据")
            return cache

        # 确定最近可用季度
        now = datetime.now()
        year, quarter = now.year, (now.month - 1) // 3 + 1
        # 当前季度可能还没披露完，回退一个季度
        quarter -= 1
        if quarter <= 0:
            year -= 1
            quarter = 4

        rows = []
        total = len(codes)
        for i, code in enumerate(codes):
            try:
                prefix = "sh" if code.startswith("6") else "sz"
                rs = _query_with_timeout(lambda: bs.query_profit_data(
                    code=f"{prefix}.{code}",
                    year=year, quarter=quarter
                ))
                if rs is not None and rs.error_code == "0":
                    data = rs.get_data()
                    if not data.empty:
                        r = data.iloc[0]
                        rows.append({
                            "代码": code.zfill(6),
                            "roeAvg": float(r.get("roeAvg", 0)) if pd.notna(r.get("roeAvg")) else 0,
                            "total_share": float(r.get("totalShare", 0)) if pd.notna(r.get("totalShare")) else 0,
                            "liqa_share": float(r.get("liqaShare", 0)) if pd.notna(r.get("liqaShare")) else 0,
                            "季度": f"{year}Q{quarter}",
                        })
            except Exception as e:
                logger.debug(f"[baostock] {code} 财务数据获取失败: {e}")
                continue
            # 每200只或最后一只打印进度
            if (i + 1) % 200 == 0 or (i + 1) == total:
                logger.info(f"[baostock] ROE查询进度: {i+1}/{total} ({(i+1)*100//total}%)")

        if rows:
            df_new = pd.DataFrame(rows)
            df = pd.concat([cache, df_new], ignore_index=True)
            self._save_cache("profit", df)
            return df[df["代码"].astype(str).str.zfill(6).isin([c.zfill(6) for c in codes])]
        return cache

    def get_market_cap(self, codes: List[str]) -> pd.DataFrame:
        """
        结合最新收盘价 + 季频股本数据计算市值

        Returns
        -------
        pd.DataFrame
            列: [代码, 总市值, 流通市值]
        """
        if not self._ensure_login():
            return pd.DataFrame()

        try:
            import baostock as bs
        except ImportError:
            return pd.DataFrame()

        # 1. 获取股本
        profit_df = self.get_profit_data(codes)
        if profit_df.empty:
            return pd.DataFrame()

        today = datetime.now().strftime("%Y-%m-%d")
        rows = []

        for _, prow in profit_df.iterrows():
            code = str(prow["代码"]).zfill(6)
            total_share = float(prow.get("total_share", 0) or 0)
            liqa_share = float(prow.get("liqa_share", 0) or 0)
            if total_share <= 0:
                continue

            try:
                prefix = "sh" if code.startswith("6") else "sz"
                rs = _query_with_timeout(lambda: bs.query_history_k_data_plus(
                    f"{prefix}.{code}",
                    "date,close",
                    start_date=today, end_date=today,
                    frequency="d"
                ))
                if rs is not None and rs.error_code == "0":
                    kdata = rs.get_data()
                    if not kdata.empty:
                        close = float(kdata.iloc[0].get("close", 0))
                        if close > 0:
                            rows.append({
                                "代码": code,
                                "总市值": close * total_share,
                                "流通市值": close * liqa_share if liqa_share > 0 else close * total_share,
                            })
            except Exception:
                continue

        return pd.DataFrame(rows) if rows else pd.DataFrame()

    # ── 便捷函数 ──

    def get_roe_for_codes(self, codes: List[str]) -> Dict[str, float]:
        """返回 {代码: ROE(%)} 字典，用于质量因子替补"""
        df = self.get_profit_data(codes)
        if df.empty:
            return {}
        return {
            str(row["代码"]).zfill(6): float(row["roeAvg"]) * 100  # 小数 → 百分比
            for _, row in df.iterrows()
            if pd.notna(row.get("roeAvg")) and float(row["roeAvg"]) > 0
        }

    def get_cap_for_codes(self, codes: List[str]) -> Dict[str, Dict[str, float]]:
        """返回 {代码: {"总市值": x, "流通市值": y}} 字典，用于规模因子替补"""
        df = self.get_market_cap(codes)
        if df.empty:
            return {}
        return {
            str(row["代码"]).zfill(6): {
                "总市值": float(row["总市值"]),
                "流通市值": float(row["流通市值"]),
            }
            for _, row in df.iterrows()
        }
