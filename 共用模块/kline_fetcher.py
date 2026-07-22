# -*- coding: utf-8 -*-
"""
统一K线获取层 — 多源自动降级 + 风控对抗
================================================
数据源优先级（按稳定性排序）：
  1. Baostock  — 免费、稳定、支持前复权日线
  2. AKShare   — 功能全，但频繁访问易限IP
  3. 新浪      — 简单直接，但接口可能改版
  4. 腾讯      — 兜底，返回字段格式较特殊

使用示例:
    from kline_fetcher import KlineFetcher
    df = KlineFetcher().get_kline("000001", days=120)
"""

import os
import time
import json
import random
import logging
import threading
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Dict, List

import pandas as pd
import requests

# ── 接入项目日志系统（确保K线请求/失败/熔断写入文件）──
try:
    from logger import get_logger
    _project_logger = get_logger()
    _HAS_PROJECT_LOGGER = True
except Exception:
    _HAS_PROJECT_LOGGER = False

# 标准logging作为fallback（仅控制台输出）
logger = logging.getLogger(__name__)


def _log(level: str, msg: str):
    """统一日志输出：写入项目日志系统 + 标准logging fallback"""
    if _HAS_PROJECT_LOGGER:
        try:
            if level == "DEBUG":
                _project_logger.debug(msg)
            elif level == "INFO":
                _project_logger.info(msg)
            elif level == "WARN":
                _project_logger.warn(msg)
            elif level == "ERROR":
                _project_logger.error(msg)
            return
        except Exception:
            pass
    # fallback: 标准logging
    getattr(logger, level.lower(), logger.info)(msg)

# ── 配置 ──
CACHE_TTL_HOURS = 24          # 缓存有效期（小时）
REQUEST_TIMEOUT = 30          # 请求超时（秒）
MAX_RETRIES = 3               # 每个源最大重试次数
RETRY_BACKOFF_BASE = 1.5      # 指数退避基数（秒）
RATE_LIMIT_SLEEP = (0.3, 0.8) # 请求间隔随机范围（秒）

# ── 熔断配置 ──
CIRCUIT_BREAKER_THRESHOLD = 5    # 连续失败 N 次后熔断
CIRCUIT_BREAKER_COOLDOWN = 120    # 熔断冷却时间（秒）
BATCH_PAUSE_EVERY = 50            # 每发起 N 次实际网络请求后暂停（不含缓存命中）
BATCH_PAUSE_SECONDS = 2.0         # 批次间暂停时间（秒）

# ── 登录态配置（优先从 config.json 读取，其次环境变量） ──
def _load_credentials():
    """从 config.json 加载凭证，支持 ${ENV} 占位符"""
    cfg_path = Path(__file__).parent.parent / "config.json"
    creds = {}
    if cfg_path.exists():
        try:
            with open(cfg_path, "r", encoding="utf-8") as f:
                cfg = json.load(f)
            raw = cfg.get("credentials", {})
            for k, v in raw.items():
                if isinstance(v, str) and v.startswith("${") and v.endswith("}"):
                    v = os.environ.get(v[2:-1], "")
                creds[k] = v
        except Exception as e:
            _log("WARN", f"[kline-凭证] 操作异常: {e}")
    return creds


_CREDS = _load_credentials()
XUEQIU_TOKEN = _CREDS.get("xueqiu_token") or os.environ.get("XUEQIU_TOKEN", "")
XUEQIU_U = _CREDS.get("xueqiu_u") or os.environ.get("XUEQIU_U", "")
THS_COOKIE_V = _CREDS.get("ths_cookie_v") or os.environ.get("THS_COOKIE_V", "")
JQDATA_TOKEN = _CREDS.get("jqdata_token") or os.environ.get("JQDATA_TOKEN", "")

_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:127.0) Gecko/20100101 Firefox/127.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
]


def _random_ua() -> str:
    return random.choice(_USER_AGENTS)


def _rate_limit() -> None:
    """随机睡眠，避免请求过于规律触发风控"""
    time.sleep(random.uniform(*RATE_LIMIT_SLEEP))


# ============================================================
#  全局熔断器（跨实例共享，线程安全）
# ============================================================

class _CircuitBreaker:
    """数据源级熔断器：某源连续失败 N 次后暂停一段时间（线程安全）"""

    def __init__(self, threshold=CIRCUIT_BREAKER_THRESHOLD,
                 cooldown=CIRCUIT_BREAKER_COOLDOWN):
        self._threshold = threshold
        self._cooldown = cooldown
        self._fail_counts = {}       # {source_name: int}
        self._trip_time = {}        # {source_name: float(timestamp)}
        self._total_since_pause = 0  # 批次计数器
        self._lock = threading.Lock()

    def is_open(self, source: str) -> bool:
        """检查某源是否已熔断"""
        with self._lock:
            if source in self._trip_time:
                elapsed = time.time() - self._trip_time[source]
                if elapsed < self._cooldown:
                    return True
                del self._trip_time[source]
                self._fail_counts.pop(source, None)
            return False

    def record_success(self, source: str):
        """记录成功，重置失败计数"""
        with self._lock:
            self._fail_counts.pop(source, None)
            self._total_since_pause += 1

    def record_failure(self, source: str):
        """记录失败，达到阈值时触发熔断"""
        with self._lock:
            self._fail_counts[source] = self._fail_counts.get(source, 0) + 1
            if self._fail_counts[source] >= self._threshold:
                self._trip_time[source] = time.time()
                _log("WARN",
                     f"[熔断] {source} 连续失败 {self._fail_counts[source]} 次，"
                     f"暂停 {self._cooldown} 秒"
                     )
            self._total_since_pause += 1

    def should_batch_pause(self) -> bool:
        """检查是否需要批次暂停"""
        with self._lock:
            if BATCH_PAUSE_EVERY > 0 and self._total_since_pause >= BATCH_PAUSE_EVERY:
                self._total_since_pause = 0
                return True
            return False

    def reset(self):
        """重置所有状态"""
        with self._lock:
            self._fail_counts.clear()
            self._trip_time.clear()
            self._total_since_pause = 0


_circuit_breaker = _CircuitBreaker()


def _retry_with_backoff(func, retries=MAX_RETRIES, backoff_base=RETRY_BACKOFF_BASE):
    """带指数退避的重试装饰器"""
    last_exc = None
    for attempt in range(retries):
        try:
            return func()
        except Exception as e:
            last_exc = e
            sleep_time = backoff_base * (2 ** attempt) + random.uniform(0, 1)
            _log("WARN", f"[kline] 请求失败({attempt + 1}/{retries}): {e}, {sleep_time:.1f}s后重试")
            time.sleep(sleep_time)
    raise last_exc


class KlineFetcher:
    """
    统一K线获取器
    """

    def __init__(self, cache_dir: Optional[Path] = None, base_dir: Optional[Path] = None):
        self.base_dir = base_dir or Path(__file__).parent.parent
        self.cache_dir = cache_dir or (self.base_dir / "cache" / "kline")
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self._last_request_time = 0.0
        self._baostock_logged_in = False

    # ================================================================
    #  公共接口
    # ================================================================

    def get_kline(self, code: str, days: int = 120, adjust: str = "qfq") -> Optional[pd.DataFrame]:
        """
        获取单只股票日K线，带多源自动降级

        Parameters
        ----------
        code : str
            6位数字代码，如 "000001"
        days : int
            获取最近多少个交易日
        adjust : str
            "qfq" 前复权 | "hfq" 后复权 | "none" 不复权

        Returns
        -------
        pd.DataFrame | None
            列: [日期, 开盘, 最高, 最低, 收盘, 成交量, 成交额]
            按日期升序排列
        """
        code = str(code).zfill(6)

        # 0. 批次暂停
        if _circuit_breaker.should_batch_pause():
            pause = BATCH_PAUSE_SECONDS + random.uniform(0, 1)
            _log("INFO", f"[熔断] 批次暂停 {pause:.1f}s（防风控，已发起 {_circuit_breaker._total_since_pause + BATCH_PAUSE_EVERY} 次网络请求）")
            time.sleep(pause)

        # 1. 查缓存
        df = self._load_cache(code, days, adjust)
        if df is not None and len(df) >= days * 0.8:
            _log("DEBUG", f"[kline] {code} 命中缓存 ({len(df)} 条)")
            return df

        # 2. 多源获取（按稳定性排序，跳过已熔断的源）
        # 优先级：efinance(东财专用，数据全) > 腾讯(无认证、速度快) > 新浪(稳定)
        #        > baostock(数据全但有登录开销) > MOOTDX(通达信TCP协议)
        #        > 雪球(需token) > akshare(需安装) > 同花顺(需akshare+cookie)
        sources = [
            ("efinance", self._fetch_efinance),
            ("tencent", self._fetch_tencent),
            ("sina", self._fetch_sina),
            ("sohu", self._fetch_sohu),
            ("baostock", self._fetch_baostock),
            ("jqdata", self._fetch_jqdata),
            ("mootdx", self._fetch_mootdx),
            ("xueqiu", self._fetch_xueqiu),
            ("akshare", self._fetch_akshare),
            ("ths", self._fetch_ths),
            ("netease", self._fetch_netease),
            ("stockapi", self._fetch_stockapi),
        ]

        for name, fetcher in sources:
            if _circuit_breaker.is_open(name):
                _log("DEBUG", f"[熔断] {name} 冷却中，跳过")
                continue
            try:
                _rate_limit()
                df = _retry_with_backoff(lambda: fetcher(code, days, adjust))
                if df is not None and len(df) >= 10:
                    df = self._standardize(df)
                    self._save_cache(code, days, adjust, df)
                    _circuit_breaker.record_success(name)
                    _log("INFO", f"[kline] {code} 从 {name} 获取成功 ({len(df)} 条)")
                    return df
                else:
                    _circuit_breaker.record_failure(name)
            except Exception as e:
                _circuit_breaker.record_failure(name)
                _log("WARN", f"[kline] {code} {name} 失败: {e}")
                continue

        _log("ERROR", f"[kline] {code} 所有数据源均失败")
        return None

    def get_latest_price(self, code: str) -> Optional[float]:
        """获取最新收盘价"""
        df = self.get_kline(code, days=5)
        if df is not None and len(df) > 0:
            return float(df.iloc[-1]["收盘"])
        return None

    # ================================================================
    #  缓存层
    # ================================================================

    def _cache_path(self, code: str, days: int, adjust: str) -> Path:
        return self.cache_dir / f"kline_{code}_{days}_{adjust}.parquet"

    def _load_cache(self, code: str, days: int, adjust: str) -> Optional[pd.DataFrame]:
        path = self._cache_path(code, days, adjust)
        if path.exists():
            try:
                mtime = datetime.fromtimestamp(path.stat().st_mtime)
                if (datetime.now() - mtime).total_seconds() > CACHE_TTL_HOURS * 3600:
                    pass  # 过期，继续检查其他缓存
                else:
                    return pd.read_parquet(path)
            except Exception as e:
                _log("WARN", f"[kline-缓存] 读取失败: {e}")

        # 精确天数缓存未命中，检查是否有其他天数的缓存可复用
        # 修复：小缓存只要行数够也能复用（之前 break 逻辑导致60天缓存无法被120天请求复用）
        for cached_days in [120, 90, 60, 30]:
            if cached_days == days:
                continue  # 已经检查过
            alt_path = self._cache_path(code, cached_days, adjust)
            if alt_path.exists():
                try:
                    mtime = datetime.fromtimestamp(alt_path.stat().st_mtime)
                    if (datetime.now() - mtime).total_seconds() > CACHE_TTL_HOURS * 3600:
                        continue
                    df = pd.read_parquet(alt_path)
                    # 只要缓存行数够（>=请求天数的80%），就复用
                    if len(df) >= days * 0.8:
                        return df.tail(days)  # 取最近N天
                except Exception:
                    pass
        return None

    def _save_cache(self, code: str, days: int, adjust: str, df: pd.DataFrame) -> None:
        path = self._cache_path(code, days, adjust)
        try:
            df.to_parquet(path, index=False)
        except Exception as e:
            _log("WARN", f"[kline] 缓存保存失败 {code}: {e}")

    # ================================================================
    #  字段标准化
    # ================================================================

    def _standardize(self, df: pd.DataFrame) -> pd.DataFrame:
        """将各数据源返回的字段统一为标准列名"""
        col_map = {
            # 常见变体 → 标准名
            "date": "日期", "Date": "日期", "trade_date": "日期",
            "open": "开盘", "Open": "开盘",
            "high": "最高", "High": "最高",
            "low": "最低", "Low": "最低",
            "close": "收盘", "Close": "收盘",
            "volume": "成交量", "Volume": "成交量", "vol": "成交量",
            "amount": "成交额", "Amount": "成交额", "amt": "成交额",
        }
        df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

        # 确保标准列存在
        std_cols = ["日期", "开盘", "最高", "最低", "收盘", "成交量"]
        for c in std_cols:
            if c not in df.columns:
                df[c] = 0.0

        # 类型转换
        for c in ["开盘", "最高", "最低", "收盘", "成交量", "成交额"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

        df["日期"] = pd.to_datetime(df["日期"]).dt.strftime("%Y-%m-%d")
        df = df.sort_values("日期").reset_index(drop=True)
        return df

    # ================================================================
    #  数据源 1: Baostock
    # ================================================================

    def _fetch_baostock(self, code: str, days: int, adjust: str) -> Optional[pd.DataFrame]:
        try:
            import baostock as bs
        except ImportError:
            return None

        # 登录（只做一次）
        if not self._baostock_logged_in:
            lg = bs.login(user_id="anonymous", password="123456")
            if lg.error_code != "0":
                _log("WARN", f"[baostock] 登录失败: {lg.error_msg}")
                return None
            self._baostock_logged_in = True

        prefix = "sh" if code.startswith("6") else "sz"
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=int(days * 1.5))).strftime("%Y-%m-%d")

        adjust_map = {"qfq": "2", "hfq": "1", "none": "3"}
        adjust_flag = adjust_map.get(adjust, "2")

        fields = "date,open,high,low,close,volume,amount"
        rs = bs.query_history_k_data_plus(
            f"{prefix}.{code}", fields,
            start_date=start_date, end_date=end_date,
            frequency="d", adjustflag=adjust_flag
        )

        if rs.error_code != "0":
            _log("WARN", f"[baostock] 查询失败: {rs.error_msg}")
            return None

        data = []
        while rs.next():
            data.append(rs.get_row_data())
        if not data:
            return None

        df = pd.DataFrame(data, columns=rs.fields)
        return df

    # ================================================================
    #  数据源 2: AKShare
    # ================================================================

    def _fetch_akshare(self, code: str, days: int, adjust: str) -> Optional[pd.DataFrame]:
        try:
            import akshare as ak
        except ImportError:
            return None

        # akshare 的 stock_zh_a_hist 接口
        adjust_map = {"qfq": "qfq", "hfq": "hfq", "none": ""}
        adj = adjust_map.get(adjust, "qfq")

        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=int(days * 1.5))).strftime("%Y%m%d")

        df = ak.stock_zh_a_hist(
            symbol=code, period="daily",
            start_date=start_date, end_date=end_date,
            adjust=adj
        )
        if df is None or len(df) == 0:
            return None
        return df

    # ================================================================
    #  数据源 3: 新浪
    # ================================================================

    def _fetch_sina(self, code: str, days: int, adjust: str) -> Optional[pd.DataFrame]:
        prefix = "sh" if code.startswith("6") else "sz"
        url = (
            "https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/"
            "CN_MarketData.getKLineData"
        )
        params = {
            "symbol": f"{prefix}{code}",
            "scale": 240,          # 240分钟 = 1交易日
            "ma": "no",
            "datalen": days,
        }
        headers = {"User-Agent": _random_ua()}
        resp = requests.get(url, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()

        text = resp.text
        if not text or text == "null":
            return None

        # 新浪返回的是类JSON格式（键没有用引号包裹）
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            # 尝试用 eval 解析（新浪接口的特殊格式）
            try:
                import ast
                data = ast.literal_eval(text)
            except Exception as e:
                _log("WARN", f"[sina] 解析失败: {e}")
                return None

        if not data or len(data) == 0:
            return None

        rows = []
        for item in data:
            rows.append({
                "日期": item.get("day"),
                "开盘": float(item.get("open", 0)),
                "最高": float(item.get("high", 0)),
                "最低": float(item.get("low", 0)),
                "收盘": float(item.get("close", 0)),
                "成交量": int(item.get("volume", 0)),
            })
        return pd.DataFrame(rows)

    # ================================================================
    #  数据源 4: 雪球（需登录态）
    # ================================================================

    def _fetch_xueqiu(self, code: str, days: int, adjust: str) -> Optional[pd.DataFrame]:
        """雪球K线 — 需要 xq_a_token cookie"""
        if not XUEQIU_TOKEN:
            return None

        market = "SH" if code.startswith("6") else "SZ"
        symbol = f"{market}{code}"
        # 雪球需要毫秒时间戳
        end_ts = int(datetime.now().timestamp() * 1000)
        url = (
            f"https://stock.xueqiu.com/v5/stock/chart/kline.json?"
            f"symbol={symbol}&begin={end_ts}&period=day&"
            f"type=before&count=-{days * 2}"
        )
        headers = {
            "User-Agent": _random_ua(),
            "Cookie": f"xq_a_token={XUEQIU_TOKEN}; u={XUEQIU_U}",
            "Referer": "https://xueqiu.com",
        }
        resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()

        data = resp.json()
        items = data.get("data", {}).get("item", [])
        if not items:
            return None

        rows = []
        for item in items:
            # 雪球格式: [timestamp, volume, open, high, low, close, chg, percent]
            if not isinstance(item, (list, tuple)) or len(item) < 6:
                continue
            ts = item[0] / 1000 if item[0] > 1e10 else item[0]
            date_str = datetime.fromtimestamp(ts).strftime("%Y-%m-%d")
            rows.append({
                "日期": date_str,
                "开盘": float(item[2]),
                "最高": float(item[3]),
                "最低": float(item[4]),
                "收盘": float(item[5]),
                "成交量": int(item[1]),
            })
        return pd.DataFrame(rows)

    # ================================================================
    #  数据源 5: 同花顺（需登录态）
    # ================================================================

    def _fetch_ths(self, code: str, days: int, adjust: str) -> Optional[pd.DataFrame]:
        """同花顺K线 — 通过 akshare 同花顺接口（需 cookie）"""
        if not THS_COOKIE_V:
            return None
        try:
            import akshare as ak
        except ImportError:
            return None

        # 设置同花顺 cookie（akshare 1.18+ 已移除 stock_ths_em 模块，用 try/except 保护）
        try:
            import akshare.stock.stock_ths_em as ths_em
            if hasattr(ths_em, "set_cookie"):
                ths_em.set_cookie(THS_COOKIE_V)
        except (ImportError, ModuleNotFoundError):
            pass  # 新版 akshare 无此模块，跳过 cookie 设置

        # 同花顺日K线接口
        try:
            prefix = "sh" if code.startswith("6") else "sz"
            df = ak.stock_zh_a_hist(
                symbol=code, period="daily",
                start_date=(datetime.now() - timedelta(days=int(days * 1.5))).strftime("%Y%m%d"),
                end_date=datetime.now().strftime("%Y%m%d"),
                adjust="qfq" if adjust == "qfq" else ""
            )
            if df is not None and len(df) > 0:
                return df
        except Exception as e:
            _log("WARN", f"[tencent] 获取失败 {code}: {e}")

        # 备用：同花顺实时行情历史数据
        try:
            prefix = "sh" if code.startswith("6") else "sz"
            df = ak.stock_zh_a_hist_tx(symbol=f"{prefix}{code}")
            if df is not None and len(df) > 0:
                return df.tail(days * 2)
        except Exception as e:
            _log("WARN", f"[akshare] 获取失败 {code}: {e}")

        return None

    # ================================================================
    #  数据源 6: 腾讯
    # ================================================================

    def _fetch_tencent(self, code: str, days: int, adjust: str) -> Optional[pd.DataFrame]:
        prefix = "sh" if code.startswith("6") else "sz"
        url = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
        params = {
            "param": f"{prefix}{code},day,,,{days},qfq",
        }
        headers = {"User-Agent": _random_ua()}
        resp = requests.get(url, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()

        data = resp.json()
        key = f"{prefix}{code}"
        if key not in data.get("data", {}):
            return None

        # 腾讯API: 正常股票返回 qfqday（前复权），新股/特殊品种返回 day（不复权）
        klines = data["data"][key].get("qfqday") or data["data"][key].get("day") or []
        if not klines:
            return None

        rows = []
        for item in klines:
            # 腾讯格式: [日期, 开盘, 收盘, 最高, 最低, 成交量]
            if not isinstance(item, (list, tuple)) or len(item) < 6:
                continue
            rows.append({
                "日期": item[0],
                "开盘": float(item[1]),
                "收盘": float(item[2]),
                "最高": float(item[3]),
                "最低": float(item[4]),
                # 成交量可能是 "1265317.000" 带小数点的字符串，先 float 再 int
                "成交量": int(float(item[5])),
            })
        df = pd.DataFrame(rows)
        # 腾讯没有成交额字段，用成交量 * 收盘价 估算
        df["成交额"] = df["成交量"] * df["收盘"]
        return df

    # ================================================================
    #  数据源 7: efinance（东方财富专用库）
    # ================================================================

    def _fetch_efinance(self, code: str, days: int, adjust: str) -> Optional[pd.DataFrame]:
        """efinance K线 — 东方财富专用库，数据完整、速度快"""
        try:
            import efinance as ef
        except ImportError:
            return None

        try:
            df = ef.stock.get_quote_history(code)
            if df is None or len(df) == 0:
                return None
            # 截取最近days条
            df = df.tail(days * 2)
            # 列名已经是中文：日期/开盘/收盘/最高/最低/成交量/成交额/涨跌幅
            # 确保标准列存在
            if "成交额" not in df.columns and "成交量" in df.columns and "收盘" in df.columns:
                df["成交额"] = df["成交量"] * df["收盘"]
            return df
        except Exception as e:
            _log("WARN", f"[efinance] 获取失败 {code}: {e}")
            return None

    # ================================================================
    #  数据源 8: 聚宽JQData（需token）
    # ================================================================

    def _fetch_jqdata(self, code: str, days: int, adjust: str) -> Optional[pd.DataFrame]:
        """聚宽JQData K线 — 需注册获取token，数据质量高"""
        if not JQDATA_TOKEN:
            return None
        try:
            import jqdatasdk as jq
        except ImportError:
            return None

        try:
            jq.auth(JQDATA_TOKEN, "")
            prefix = "XSHG" if code.startswith("6") else "XSHE"
            security = f"{code}.{prefix}"
            end_date = datetime.now().date()
            start_date = (datetime.now() - timedelta(days=int(days * 1.5))).date()
            df = jq.get_price(
                security, start_date=start_date, end_date=end_date,
                frequency="daily", fields=["open", "high", "low", "close", "volume", "money"],
                skip_paused=True, fq="pre" if adjust == "qfq" else None
            )
            if df is None or len(df) == 0:
                return None
            df = df.reset_index()
            df = df.rename(columns={
                "index": "日期", "open": "开盘", "high": "最高",
                "low": "最低", "close": "收盘", "volume": "成交量", "money": "成交额",
            })
            df["日期"] = pd.to_datetime(df["日期"]).dt.strftime("%Y-%m-%d")
            return df
        except Exception as e:
            _log("WARN", f"[jqdata] 获取失败 {code}: {e}")
            return None

    # ================================================================
    #  数据源 9: MOOTDX（通达信TCP协议）
    # ================================================================

    def _fetch_mootdx(self, code: str, days: int, adjust: str) -> Optional[pd.DataFrame]:
        """MOOTDX K线 — 通达信TCP协议直连，独立通道不依赖HTTP"""
        try:
            from mootdx.quotes import Quotes
        except ImportError:
            return None

        # 需要配置通达信安装目录才能使用（在线模式不稳定）
        tdxdir = _CREDS.get("mootdx_tdxdir", "")
        if not tdxdir or not Path(tdxdir).exists():
            return None

        try:
            client = Quotes.factory(market='std', tdxdir=tdxdir, multithread=True)
            # frequency=9 表示日线，offset=0, limit=days*2
            df = client.bars(symbol=code, frequency=9, offset=0, limit=days * 2)
            if df is None or len(df) == 0:
                return None
            # MOOTDX返回的列：date/open/close/high/low/volume/amount
            col_map = {
                "date": "日期", "open": "开盘", "close": "收盘",
                "high": "最高", "low": "最低", "volume": "成交量", "amount": "成交额",
            }
            df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
            if "成交额" not in df.columns and "成交量" in df.columns and "收盘" in df.columns:
                df["成交额"] = df["成交量"] * df["收盘"]
            return df
        except Exception as e:
            _log("WARN", f"[mootdx] 获取失败 {code}: {e}")
            return None

    # ================================================================
    #  数据源 10: 搜狐财经（历史K线）
    # ================================================================

    def _fetch_sohu(self, code: str, days: int, adjust: str) -> Optional[pd.DataFrame]:
        """搜狐财经K线 — 历史数据，JSON格式"""
        try:
            import requests
            from datetime import datetime, timedelta
            end = datetime.now().strftime("%Y%m%d")
            start = (datetime.now() - timedelta(days=int(days * 1.5))).strftime("%Y%m%d")
            url = f"https://q.stock.sohu.com/hisHq?code=cn_{code}&start={start}&end={end}&stat=1&order=D&period=d&rt=json"
            r = requests.get(url, timeout=15)
            if r.status_code != 200:
                return None
            data = r.json()
            if not data or data[0].get("status") != 0:
                return None
            hq = data[0].get("hq", [])
            if not hq:
                return None
            rows = []
            for item in hq:
                # 格式: [日期,开盘,收盘,涨跌额,涨跌幅,最低,最高,成交量,成交额,换手率]
                rows.append({
                    "日期": item[0],
                    "开盘": float(item[1]),
                    "收盘": float(item[2]),
                    "涨跌额": float(item[3]),
                    "涨跌幅": item[4],
                    "最低": float(item[5]),
                    "最高": float(item[6]),
                    "成交量": int(float(item[7])),
                    "成交额": float(item[8]),
                    "换手率": item[9],
                })
            df = pd.DataFrame(rows)
            df = df.sort_values("日期").reset_index(drop=True)
            return df
        except Exception as e:
            _log("WARN", f"[sohu] 获取失败 {code}: {e}")
            return None

    # ================================================================
    #  数据源 11: 网易财经（历史K线，CSV格式）
    # ================================================================

    def _fetch_netease(self, code: str, days: int, adjust: str) -> Optional[pd.DataFrame]:
        """网易财经K线 — CSV格式，部分时段可能502"""
        try:
            import requests
            from io import StringIO
            from datetime import datetime, timedelta
            prefix = "1" if code.startswith("6") else "0"
            end = datetime.now().strftime("%Y%m%d")
            start = (datetime.now() - timedelta(days=int(days * 1.5))).strftime("%Y%m%d")
            url = (
                f"http://quotes.money.163.com/service/chddata.html"
                f"?code={prefix}{code}&start={start}&end={end}"
                f"&fields=TCLOSE;HIGH;LOW;TOPEN;LCLOSE;CHG;PCHG;"
                f"TURNOVER;VOTURNOVER;VATURNOVER;TCAP;MCAP"
            )
            r = requests.get(url, timeout=30)
            if r.status_code != 200 or len(r.text) < 500:
                return None
            df = pd.read_csv(StringIO(r.text), encoding='gb2312')
            if len(df) == 0:
                return None
            # 列名映射
            col_map = {
                "日期": "日期", "股票代码": "_代码", "名称": "_名称",
                "收盘价": "收盘", "最高价": "最高", "最低价": "最低",
                "开盘价": "开盘", "前收盘": "昨收", "涨跌额": "涨跌额",
                "涨跌幅": "涨跌幅", "换手率": "换手率",
                "成交量": "成交量", "成交金额": "成交额",
                "总市值": "总市值", "流通市值": "流通市值",
            }
            df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
            df = df.sort_values("日期").reset_index(drop=True)
            return df
        except Exception as e:
            _log("WARN", f"[netease] 获取失败 {code}: {e}")
            return None

    # ================================================================
    #  数据源 12: StockAPI（日线行情）
    # ================================================================

    def _fetch_stockapi(self, code: str, days: int, adjust: str) -> Optional[pd.DataFrame]:
        """StockAPI K线 — 日线数据，SSL证书可能需忽略验证"""
        try:
            import requests
            from datetime import datetime, timedelta
            end = datetime.now().strftime("%Y-%m-%d")
            start = (datetime.now() - timedelta(days=int(days * 1.5))).strftime("%Y-%m-%d")
            url = "https://stockapi.com.cn/v1/base/day"
            params = {"code": code, "startDate": start.replace("-", ""), "endDate": end.replace("-", "")}
            r = requests.get(url, params=params, timeout=10, verify=False)
            if r.status_code != 200:
                return None
            data = r.json()
            if not isinstance(data, list) or len(data) == 0:
                return None
            rows = []
            for item in data:
                rows.append({
                    "日期": item.get("date", ""),
                    "开盘": float(item.get("open", 0)),
                    "收盘": float(item.get("close", 0)),
                    "最高": float(item.get("high", 0)),
                    "最低": float(item.get("low", 0)),
                    "成交量": int(float(item.get("volume", 0))),
                    "成交额": float(item.get("amount", 0)),
                })
            df = pd.DataFrame(rows)
            df = df.sort_values("日期").reset_index(drop=True)
            return df
        except Exception as e:
            _log("WARN", f"[stockapi] 获取失败 {code}: {e}")
            return None


# ================================================================
#  便捷函数（保持向后兼容）
# ================================================================

_fetcher_instance: Optional[KlineFetcher] = None


def get_kline(code: str, days: int = 120, adjust: str = "qfq", cache_dir: Optional[Path] = None) -> Optional[pd.DataFrame]:
    """兼容旧接口的全局函数"""
    global _fetcher_instance
    if _fetcher_instance is None:
        _fetcher_instance = KlineFetcher(cache_dir=cache_dir)
    return _fetcher_instance.get_kline(code, days, adjust)
