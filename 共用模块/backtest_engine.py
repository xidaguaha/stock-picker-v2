#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
回测引擎 v1.0 — 量化选股系统自我进化核心
====================================================
职责：
  1. 读取 history/ 中的所有历史选股快照
  2. 对每个快照，验证其后 1/3/5/10/20 个交易日的实际涨跌
  3. 计算：命中率、平均收益、夏普比率、最大回撤、IC 值
  4. 输出回测报告到 reports/
  5. 为因子调优提供数据基础

核心理念：
  系统'越用越聪明'的根基 —— 每一次选股结果都被精确验证，
  积累的验证数据反过来优化下一次选股参数。
"""

import pandas as pd
import numpy as np
import json
import os
import time
import requests
from datetime import datetime, timedelta
from pathlib import Path
from io import StringIO

# ── 日志 ──
try:
    from logger import get_logger
    _bt_logger = get_logger()
    _HAS_BT_LOGGER = True
except Exception:
    _HAS_BT_LOGGER = False


def log(msg: str, level: str = "INFO"):
    """统一日志输出"""
    from datetime import datetime as _dt
    if _HAS_BT_LOGGER:
        try:
            getattr(_bt_logger, level.lower(), _bt_logger.info)(msg)
            return
        except Exception:
            pass
    # fallback: 直接print
    print(f"[{_dt.now().strftime('%H:%M:%S')}] [{level}] {msg}")

# ============================================================
#  路径配置
# ============================================================
BASE_DIR    = Path(__file__).parent.parent  # 指向项目根目录
HISTORY_DIR = BASE_DIR / "history"
REPORTS_DIR = BASE_DIR / "reports"
PERF_DIR    = BASE_DIR / "performance"
SNAPSHOTS_DIR = BASE_DIR / "snapshots"
CACHE_DIR   = BASE_DIR / "cache"

for d in [REPORTS_DIR, PERF_DIR, SNAPSHOTS_DIR]:
    d.mkdir(parents=True, exist_ok=True)

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

# ============================================================
#  K线获取（统一接口，多源自动降级）
# ============================================================
from kline_fetcher import KlineFetcher

_kline_fetcher = KlineFetcher(cache_dir=CACHE_DIR)


def log(msg, level="INFO"):
    """简易日志函数（防止异常路径调用未定义的log导致NameError崩溃）"""
    from datetime import datetime as _dt
    print(f"[{_dt.now().strftime('%H:%M:%S')}] [{level}] {msg}")


def _fetch_kline_sina(code, days=120):
    """获取单只股票日K线（统一接口，多源自动降级）"""
    return _kline_fetcher.get_kline(code, days=days, adjust="qfq")


def _fetch_benchmark_kline(days=120, benchmark="000985"):
    """
    获取基准指数K线。

    Args:
        days: 获取天数
        benchmark: 基准指数代码
            - "000985": 中证全指（推荐，全市场等权）
            - "000300": 沪深300（大盘股）
            - "000905": 中证500（中盘股）

    Returns:
        DataFrame: K线数据
    """
    return _fetch_kline_sina(benchmark, days)


def _get_limit_up_threshold(code: str) -> float:
    """根据股票代码返回涨停阈值（百分比）"""
    code = str(code).zfill(6)
    if code.startswith(("688", "689")):      # 科创板
        return 0.198
    if code.startswith(("300", "301")):      # 创业板
        return 0.198
    if code.startswith(("8", "4")):          # 北交所
        return 0.298
    return 0.098                               # 主板


def _estimate_survivorship_bias(code, snapshot_date, hold_days, buy_price):
    """
    估算幸存者偏差对收益的影响。

    对于数据缺失的股票（可能已退市/ST/停牌），根据不同阶段估算持有期内收益。
    A股退市流程: 风险警示(ST) -> 退市整理 -> 摘牌，不同阶段跌幅不同。

    Returns:
        dict: {1: -0.05, 3: -0.15, ...} 估算的收益（通常为负）
    """
    code = str(code).zfill(6)
    # 分阶段估算:
    # - 正常退市整理期: 首日-44%涨跌幅限制，后续10%
    # - ST阶段: 通常已跌30-50%
    # - 突发停牌/退市: 直接按最悲观估算
    # 按持仓天数阶梯式恶化
    estimated_ret = {}
    for d in hold_days:
        if d <= 1:
            estimated_ret[d] = -0.05   # 1天: 突发利空跌停
        elif d <= 3:
            estimated_ret[d] = -0.15   # 3天: 连续跌停
        elif d <= 5:
            estimated_ret[d] = -0.25   # 5天: ST级别跌幅
        elif d <= 10:
            estimated_ret[d] = -0.40   # 10天: 退市整理级
        else:
            estimated_ret[d] = -0.55   # 20天: 接近摘牌
    return estimated_ret


def _count_trading_days(df, start_idx, target_days):
    """
    从 start_idx 开始，跳过停牌日，计算真正的 target_days 个交易日后的索引。
    停牌判定: 成交量为0 或 (开盘==收盘==最高==最低 且 成交量极小)
    """
    if start_idx >= len(df):
        return None
    traded = 0
    for i in range(start_idx, len(df)):
        row = df.iloc[i]
        vol = row.get("成交量", 0)
        # 成交量为0视为停牌; 价格全部相同且成交量极小也视为停牌
        if pd.isna(vol) or vol <= 0:
            continue
        open_p = row.get("开盘", 0)
        close_p = row.get("收盘", 0)
        high_p = row.get("最高", 0)
        low_p = row.get("最低", 0)
        if open_p == close_p == high_p == low_p and vol < 1000:
            continue
        traded += 1
        if traded >= target_days:
            return i
    return None


def _check_liquidity(code, buy_amount, daily_avg_amount=None):
    """
    检查股票流动性，判断是否可成交。

    Args:
        code: 股票代码
        buy_amount: 计划买入金额
        daily_avg_amount: 日均成交额（万元）

    Returns:
        tuple: (can_buy: bool, slippage: float)
            - can_buy: 是否可买入
            - slippage: 额外滑点（0-0.05）
    """
    if daily_avg_amount is None or daily_avg_amount <= 0:
        return True, 0.001  # 无数据时使用默认滑点

    # 计划买入金额占日均成交额的比例
    ratio = buy_amount / (daily_avg_amount * 10000)  # 转换为元

    if ratio > 0.1:
        # 占比超过10%，流动性不足
        return False, 0.0
    elif ratio > 0.05:
        # 占比5-10%，需要额外滑点
        extra_slippage = (ratio - 0.05) * 0.5  # 每1%额外0.5%滑点
        return True, min(extra_slippage, 0.03)
    else:
        return True, 0.0


# ============================================================
#  核心回测逻辑
# ============================================================

def load_historical_picks():
    """
    扫描 history/ 目录，加载所有历史选股快照。
    返回: [(snapshot_date, snapshot_path, df_picks), ...]
    df_picks 包含: 排名, 代码, 名称, 综合得分, 涨跌幅, 换手率, ...
    """
    picks = []
    if not HISTORY_DIR.exists():
        return picks

    for f in sorted(HISTORY_DIR.rglob("snapshot_*.csv")):
        try:
            df = pd.read_csv(f, encoding="utf-8-sig")
            if "代码" not in df.columns or len(df) == 0:
                continue
            # 提取日期
            fname = f.name
            date_str = fname.replace("snapshot_", "").split("_")[0]  # YYYYMMDD
            snapshot_date = datetime.strptime(date_str, "%Y%m%d")
            picks.append((snapshot_date, str(f), df))
        except Exception as e:
            log(f"  [回测] 跳过 {f.name}: {e}", "INFO")
            continue

    log(f"  [回测] 加载了 {len(picks)} 个历史快照", "INFO")
    return picks


# 交易成本参数（默认值，可从config.json覆盖）
_DEFAULT_TRADING_COSTS = {
    "commission_rate": 0.00025,  # 券商佣金 0.025% 双边
    "stamp_tax_rate": 0.0005,    # 印花税 0.05% 卖出单边
    "transfer_fee_rate": 0.00001,  # 过户费 0.001% 双边
    "slippage_rate": 0.001,      # 滑点 0.1%（普通股）
    "slippage_limit_up": 0.005,  # 涨停股滑点 0.5%
}

# 从config.json加载交易成本配置
def _load_trading_costs():
    """从config.json加载交易成本参数"""
    try:
        config_file = BASE_DIR / "config.json"
        if config_file.exists():
            import json
            cfg = json.loads(config_file.read_text(encoding="utf-8-sig"))
            costs = cfg.get("trading_costs", {})
            result = {}
            for k, v in _DEFAULT_TRADING_COSTS.items():
                val = costs.get(k, v)
                # BUG修复: 确保值类型为float，防止config中存了字符串导致后续算术运算崩溃
                result[k] = float(val) if not isinstance(val, (int, float)) else float(val)
            return result
    except Exception as e:
        log(f"操作异常: {e}", "WARN")
    return _DEFAULT_TRADING_COSTS

_TRADING_COSTS = _load_trading_costs()
COMMISSION_RATE = _TRADING_COSTS["commission_rate"]
STAMP_TAX_RATE = _TRADING_COSTS["stamp_tax_rate"]
TRANSFER_FEE_RATE = _TRADING_COSTS["transfer_fee_rate"]
SLIPPAGE_RATE = _TRADING_COSTS["slippage_rate"]
SLIPPAGE_LIMIT_UP = _TRADING_COSTS["slippage_limit_up"]

def _calc_transaction_cost(buy_price, sell_price, is_limit_up=False):
    """
    计算单次往返交易成本。

    Args:
        buy_price: 买入价
        sell_price: 卖出价
        is_limit_up: 是否涨停股

    Returns:
        float: 交易成本（占买入价的比例）
    """
    # 买入成本：佣金 + 过户费 + 滑点
    buy_cost = COMMISSION_RATE + TRANSFER_FEE_RATE
    if is_limit_up:
        buy_cost += SLIPPAGE_LIMIT_UP
    else:
        buy_cost += SLIPPAGE_RATE

    # 卖出成本：佣金 + 印花税 + 过户费 + 滑点
    sell_cost = COMMISSION_RATE + STAMP_TAX_RATE + TRANSFER_FEE_RATE + SLIPPAGE_RATE

    # 单次往返总成本（占买入价比例）
    if buy_price <= 0:
        return 0.0  # 无法成交，无成本
    total_cost = buy_cost + sell_cost * (sell_price / buy_price)
    return total_cost


def verify_pick_performance(code, snapshot_date, hold_days=[1, 3, 5, 10, 20], use_next_open=True):
    """
    验证单只股票在指定选股日之后的表现。

    Args:
        code: 股票代码
        snapshot_date: 选股日期（快照日期）
        hold_days: 持仓天数列表
        use_next_open: 是否使用次日开盘价作为买入价（True=修复前瞻偏差）

    Returns:
        dict: {1: 0.032, 3: 0.015, ...} 每档的涨跌幅（扣除交易成本后），None表示数据不足
    """
    # 只需要 max(hold_days)+10 天的数据，但请求60天以匹配缓存（大部分股票有60天缓存）
    _need_days = max(hold_days) + 10
    df = _fetch_kline_sina(code, days=max(_need_days, 60))
    if df is None or len(df) < _need_days:
        return None

    # 找到 snapshot_date 当天或之后的第一个交易日
    df = df.sort_values("日期")
    snapshot_date_dt = pd.Timestamp(snapshot_date) if not isinstance(snapshot_date, pd.Timestamp) else snapshot_date
    # 确保"日期"列为 Timestamp 类型（可能是字符串）
    df["日期"] = pd.to_datetime(df["日期"])

    # 找到快照日期当天的收盘价（如果当天是交易日）
    snapshot_close = None
    snapshot_idx = None
    for i, (_, row) in enumerate(df.iterrows()):
        if row["日期"].date() == snapshot_date_dt.date():
            snapshot_close = row["收盘"]
            snapshot_idx = i
            break

    if snapshot_close is None:
        # 快照日期不是交易日（周末/节假日），找最近的交易日
        for i, (_, row) in enumerate(df.iterrows()):
            if row["日期"] >= snapshot_date_dt:
                snapshot_close = row["收盘"]
                snapshot_date_dt = row["日期"]
                snapshot_idx = i
                break

    if snapshot_close is None or snapshot_close == 0 or snapshot_idx is None:
        return None

    results = {}
    for d in hold_days:
        if use_next_open:
            # 修复前瞻偏差：使用次日开盘价作为买入价
            buy_idx = snapshot_idx + 1
            if buy_idx >= len(df):
                results[d] = None
                continue

            buy_price = df.iloc[buy_idx]["开盘"]
            if buy_price == 0 or pd.isna(buy_price):
                results[d] = None
                continue

            # 检查是否涨停（根据板块动态判断阈值）
            limit_up_threshold = _get_limit_up_threshold(code)
            limit_up = snapshot_close > 0 and (buy_price / snapshot_close - 1) >= limit_up_threshold

            # 持仓到期日（跳过停牌日）
            sell_idx = _count_trading_days(df, buy_idx + 1, d)
            if sell_idx is not None and sell_idx < len(df):
                sell_price = df.iloc[sell_idx]["收盘"]
                if sell_price == 0 or pd.isna(sell_price):
                    results[d] = None
                    continue

                # 检查卖出日是否停牌（如果停牌则顺延到复牌日）
                actual_sell_idx = sell_idx
                while actual_sell_idx < len(df) - 1:
                    s_row = df.iloc[actual_sell_idx]
                    s_vol = s_row.get("成交量", 0)
                    if pd.isna(s_vol) or s_vol <= 0:
                        actual_sell_idx += 1
                        continue
                    break

                if actual_sell_idx != sell_idx:
                    sell_price = df.iloc[actual_sell_idx]["收盘"]

                # 计算毛收益
                gross_ret = sell_price / buy_price - 1

                # 扣除交易成本（涨停股买入滑点更大，ST/退市股额外惩罚）
                cost = _calc_transaction_cost(buy_price, sell_price, is_limit_up=limit_up)
                net_ret = gross_ret - cost

                results[d] = round(float(net_ret), 6)
            else:
                # 数据不足（如今天买入的股票没有后续数据），返回None而非估算值
                # 之前用 _estimate_survivorship_bias 填充固定-5%/-15%，导致回测结果完全失真
                results[d] = None
        else:
            # 旧逻辑：使用快照当日收盘价（有前瞻偏差）
            target_idx = snapshot_idx + d
            if target_idx < len(df):
                future_close = df.iloc[target_idx]["收盘"]
                ret = (future_close / snapshot_close - 1)
                results[d] = round(float(ret), 6)
            else:
                results[d] = None

    return results


def run_backtest(hold_days=[1, 3, 5, 10, 20], top_n=None, verbose=True):
    """
    主回测函数。

    Args:
        hold_days: 持仓天数列表
        top_n: 每个快照只验证前 N 只（None=全部）
        verbose: 是否打印进度

    Returns:
        dict: 完整回测结果
    """
    picks = load_historical_picks()
    if not picks:
        log("  [回测] 无历史数据，跳过回测", "INFO")
        return None

    all_results = []
    all_factor_scores = []  # 用于IC计算

    for snapshot_date, path, df in picks:
        n_verify = len(df) if top_n is None else min(top_n, len(df))

        for idx, row in df.head(n_verify).iterrows():
            code = row.get("代码", "")
            name = row.get("名称", "")
            score = row.get("综合得分", np.nan)
            chg_snapshot = row.get("涨跌幅", np.nan)  # 快照当日的涨跌幅

            perf = verify_pick_performance(str(code).zfill(6), snapshot_date, hold_days)
            if perf is None:
                continue

            result = {
                "快照日期": snapshot_date.strftime("%Y-%m-%d"),
                "代码": str(code).zfill(6),
                "名称": name,
                "综合得分": score,
                "当日涨跌": chg_snapshot,
            }
            for d in hold_days:
                result[f"持有{d}天收益"] = perf.get(d)

            all_results.append(result)

            # 收集因子得分用于IC计算
            factor_record = {
                "快照日期": snapshot_date.strftime("%Y-%m-%d"),
                "代码": str(code).zfill(6),
            }
            for d in hold_days:
                factor_record[f"未来{d}日收益"] = perf.get(d)
            for col in df.columns:
                if any(k in col for k in ["动量_", "趋势_", "量能_", "估值_", "规模_", "技术_", "概念_",
                                          "成长_", "机构_", "研报_", "景气_"]):
                    factor_record[col] = row.get(col, np.nan)
                if col == "综合得分":
                    factor_record[col] = score
            all_factor_scores.append(factor_record)

        if verbose and len(picks) <= 20:
            log("  [回测] {snapshot_date.strftime(", "INFO")

    if not all_results:
        log("  [回测] 无法验证任何选股结果（K线数据缺失）", "INFO")
        return None

    df_results = pd.DataFrame(all_results)
    df_factors = pd.DataFrame(all_factor_scores)

    # 将回测算出的未来收益写回 factor_scores 文件（免费覆盖TopN）
    # BUG修复: 使用原子写入（先写临时文件再rename），防止to_parquet失败损坏原文件
    try:
        FACTOR_SCORES_DIR_W = BASE_DIR / "data" / "factor_scores"
        if FACTOR_SCORES_DIR_W.exists() and len(df_factors) > 0:
            future_cols_w = [c for c in df_factors.columns if c.startswith("未来") and "收益" in c]
            for date_str in df_factors["快照日期"].unique():
                date_compact = str(date_str).replace("-", "")[:8]
                matching_files = sorted(FACTOR_SCORES_DIR_W.glob(f"factors_{date_compact}_*.parquet"))
                if not matching_files:
                    continue
                pf = matching_files[-1]
                df_fs = pd.read_parquet(pf)

                # 初始化未来收益列
                for col in future_cols_w:
                    if col not in df_fs.columns:
                        df_fs[col] = np.nan

                # 更新匹配的股票
                df_date = df_factors[df_factors["快照日期"] == date_str]
                updated = 0
                for _, fr in df_date.iterrows():
                    code = str(fr["代码"]).zfill(6)
                    mask = df_fs["代码"].astype(str).str.zfill(6) == code
                    if mask.any():
                        for col in future_cols_w:
                            val = fr[col]
                            if pd.notna(val):
                                df_fs.loc[mask, col] = val
                        updated += 1

                if updated > 0:
                    # 原子写入
                    import tempfile
                    tmp_fd, tmp_path = tempfile.mkstemp(suffix=".parquet", dir=str(pf.parent))
                    try:
                        df_fs.to_parquet(tmp_path, index=False)
                        os.close(tmp_fd)
                        tmp_fd = -1
                        if pf.exists():
                            pf.unlink()
                        os.rename(tmp_path, str(pf))
                    except Exception as write_err:
                        if tmp_fd >= 0:
                            try:
                                os.close(tmp_fd)
                            except Exception:
                                pass
                        if os.path.exists(tmp_path):
                            os.unlink(tmp_path)
                        raise write_err
                    if verbose:
                        log(f"  [回测] 写回 {pf.name}: 更新 {updated} 只股票的未来收益", "INFO")
    except Exception as e:
        if verbose:
            log(f"  [回测] 写回未来收益失败: {e}", "WARN")

    # ============================================================
    #  统计计算
    # ============================================================
    stats = {}
    for d in hold_days:
        col = f"持有{d}天收益"
        valid = df_results[col].dropna()
        if len(valid) == 0:
            stats[d] = None
            continue

        win_rate = (valid > 0).mean()
        mean_ret = valid.mean()
        median_ret = valid.median()
        max_ret = valid.max()
        min_ret = valid.min()
        std_ret = valid.std()

        # 夏普比率（假设无风险利率=3%）
        rf_daily = 0.03 / 252 * d
        excess = valid - rf_daily
        sharpe = excess.mean() / (excess.std() + 1e-9) * np.sqrt(252 / d)

        # 最大回撤
        cumret = (1 + valid).cumprod()
        peak = cumret.expanding().max()
        drawdown = (cumret / peak - 1)
        max_dd = drawdown.min()

        # 卡玛比率
        calmar = mean_ret * 252 / d / (abs(max_dd) + 1e-9)

        stats[d] = {
            "样本数": len(valid),
            "胜率": round(float(win_rate), 4),
            "平均收益": round(float(mean_ret), 4),
            "中位数收益": round(float(median_ret), 4),
            "最大收益": round(float(max_ret), 4),
            "最大亏损": round(float(min_ret), 4),
            "标准差": round(float(std_ret), 4),
            "夏普比率": round(float(sharpe), 4),
            "最大回撤": round(float(max_dd), 4),
            "卡玛比率": round(float(calmar), 4),
        }

    # ============================================================
    #  IC 分析（Spearman Rank Correlation）
    # ============================================================
    ic_analysis = {}
    factor_cols = [c for c in df_factors.columns
                   if any(k in c for k in ["动量_", "趋势_", "量能_", "估值_", "规模_", "技术_", "概念_"])]

    for d in [h for h in hold_days if h in [1, 5, 10]]:
        future_col = f"未来{d}日收益"
        if future_col not in df_factors.columns:
            continue

        valid_f = df_factors[factor_cols + [future_col]].dropna(subset=[future_col])
        if len(valid_f) < 10:
            continue

        ic_dict = {}
        for col in factor_cols:
            if valid_f[col].nunique() < 5:
                continue
            ic = valid_f[col].corr(valid_f[future_col], method="spearman")
            ic_dict[col] = round(float(ic), 4) if not np.isnan(ic) else 0.0

        # 按IC绝对值排序
        ic_analysis[f"持有{d}天"] = dict(
            sorted(ic_dict.items(), key=lambda x: abs(x[1]), reverse=True)
        )

    # ============================================================
    #  基准对比（沪深300）
    # ============================================================
    benchmark = _fetch_benchmark_kline(120)
    bench_comparison = {}
    if benchmark is not None and len(benchmark) > 20:
        # 确保"日期"列为 datetime 类型，避免 str vs Timestamp 比较报错
        benchmark["日期"] = pd.to_datetime(benchmark["日期"])
        for d in hold_days:
            col = f"持有{d}天收益"
            valid = df_results[col].dropna()
            if len(valid) == 0:
                continue

            # 计算基准同期收益
            bench_returns = []
            for _, row in df_results.iterrows():
                snap_date = pd.Timestamp(row["快照日期"])
                bench_row = benchmark[benchmark["日期"] >= snap_date]
                if len(bench_row) == 0:
                    continue
                bench_indices = benchmark.index.get_indexer_for([bench_row.index[0]])
                if bench_indices[0] < 0:
                    continue
                bench_idx = bench_indices[0]
                if bench_idx + d < len(benchmark):
                    bench_close = benchmark.iloc[bench_idx]["收盘"]
                    if bench_close > 0:
                        bench_ret = (benchmark.iloc[bench_idx + d]["收盘"] /
                                     bench_close - 1)
                        bench_returns.append(bench_ret)

            if bench_returns:
                bench_avg = np.mean(bench_returns)
                bench_win = (np.array(bench_returns) > 0).mean()
                bench_comparison[f"持有{d}天"] = {
                    "策略平均收益": stats[d]["平均收益"],
                    "基准平均收益": round(float(bench_avg), 4),
                    "超额收益": round(float(valid.mean() - bench_avg), 4),
                    "策略胜率": stats[d]["胜率"],
                    "基准胜率": round(float(bench_win), 4),
                }

    # ============================================================
    #  组装完整报告
    # ============================================================
    report = {
        "回测时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "快照数": len(picks),
        "验证记录数": len(df_results),
        "持仓天数": hold_days,
        "逐档统计": stats,
        "IC分析": ic_analysis,
        "基准对比": bench_comparison,
        "原始数据行数": len(df_results),
    }

    # 保存报告
    report_path = REPORTS_DIR / f"回测报告_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    # 保存详细结果 CSV
    detail_path = PERF_DIR / f"回测明细_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    df_results.to_csv(detail_path, index=False, encoding="utf-8-sig")

    # 保存因子得分（IC计算用）
    ic_path = PERF_DIR / f"因子得分_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    df_factors.to_csv(ic_path, index=False, encoding="utf-8-sig")

    log("\n  [回测] 报告已保存:", "INFO")
    print(f"    JSON: {report_path.name}")
    print(f"    明细: {detail_path.name}")

    return report


def format_backtest_report(report):
    """格式化回测报告为可读文本"""
    if report is None:
        return "无回测数据（需要积累至少1天的选股历史）"

    lines = []
    lines.append("")
    lines.append("╔" + "═" * 80 + "╗")
    lines.append("║  📊 回测报告 — 策略有效性验证")
    lines.append("╠" + "═" + "╗" + " " * 76 + "║")
    lines.append(f"║  ║  快照数: {report['快照数']:>4d}  │  验证记录: {report['验证记录数']:>5d} 条")
    lines.append("╠" + "═" + "╩" + "═" * 74 + "╣")

    # 逐档统计表
    lines.append("║  持仓天数 │ 样本数 │ 胜率    │ 平均收益 │ 夏普   │ 最大回撤")
    lines.append("╟" + "─" * 78 + "╢")

    stats = report.get("逐档统计", {})
    for d, s in stats.items():
        if s is None:
            continue
        lines.append(
            f"║  {d:>5d}天  │ {s['样本数']:>5d} │ "
            f"{s['胜率']:.1%}  │ {s['平均收益']:>+.1%}  │ "
            f"{s['夏普比率']:>+.2f} │ {s['最大回撤']:>+.1%}"
        )

    # IC分析
    ic = report.get("IC分析", {})
    if ic:
        lines.append("╠" + "═" * 78 + "╣")
        lines.append("║  📈 因子 IC 值（越高越有效）")
        lines.append("╟" + "─" * 78 + "╢")
        for period, factors in ic.items():
            lines.append(f"║  [{period}] Top 8 有效因子:")
            for i, (factor, ic_val) in enumerate(list(factors.items())[:8]):
                icon = "★★★" if abs(ic_val) > 0.1 else "★★ " if abs(ic_val) > 0.05 else "★  "
                lines.append(f"║    {icon} {factor:20s}  IC={ic_val:+.4f}")
            break  # 只显示第一个持有期

    # 基准对比
    bench = report.get("基准对比", {})
    if bench:
        lines.append("╠" + "═" * 78 + "╣")
        lines.append("║  📉 基准对比（沪深300）")
        lines.append("╟" + "─" * 78 + "╢")
        for period, cmp in bench.items():
            lines.append(f"║  [{period}] 策略: {cmp['策略平均收益']:>+.1%} | "
                         f"基准: {cmp['基准平均收益']:>+.1%} | "
                         f"超额: {cmp['超额收益']:>+.1%}")

    lines.append("╚" + "═" * 80 + "╝")
    return "\n".join(lines)


# ============================================================
#  全市场 IC 分析（基于 data/factor_scores/ 全部股票）
# ============================================================

def run_full_market_ic(hold_days=[1, 3, 5, 10], sample_size=None, verbose=True):
    """
    基于全市场因子得分数据做 IC 分析。
    直接使用 factor_scores 中已回填的"未来X日收益"列，不拉K线。

    数据来源: data/factor_scores/factors_*.parquet（每次运行全量~5000只存档）
    方法: 读取已回填的未来收益列，计算 Spearman IC。

    Args:
        hold_days: 持有天数列表
        sample_size: 每个快照的抽样数（None=全部）
        verbose: 是否打印进度

    Returns:
        dict: IC分析报告
    """
    FACTOR_SCORES_DIR = BASE_DIR / "data" / "factor_scores"

    if not FACTOR_SCORES_DIR.exists():
        if verbose:
            log("  [全市场IC] data/factor_scores/ 目录不存在，跳过", "WARN")
        return None

    parquet_files = sorted(FACTOR_SCORES_DIR.glob("factors_*.parquet"))
    if not parquet_files:
        if verbose:
            log("  [全市场IC] 无因子得分数据，跳过", "INFO")
        return None

    if verbose:
        log(f"\n  [全市场IC] 加载 {len(parquet_files)} 份因子得分文件...", "INFO")

    # 加载全部因子得分
    all_factors = []
    for pf in parquet_files:
        try:
            df = pd.read_parquet(pf)
            all_factors.append(df)
        except Exception as e:
            if verbose:
                print(f"    ⚠ 跳过 {pf.name}: {e}")

    if not all_factors:
        return None

    df_all = pd.concat(all_factors, ignore_index=True)
    df_all = df_all.drop_duplicates(subset=["快照日期", "代码"], keep="last")

    # 检查是否有已回填的未来收益列
    future_cols = {d: f"未来{d}日收益" for d in hold_days if f"未来{d}日收益" in df_all.columns}
    if not future_cols:
        if verbose:
            log("  [全市场IC] 因子数据中无已回填的未来收益列，请先运行 backfill_future_returns", "INFO")
        return None

    if verbose:
        log(f"  [全市场IC] 合并后: {df_all['代码'].nunique()} 只股票, {len(df_all)} 条记录", "INFO")
        log(f"  [全市场IC] 使用已回填的未来收益列: {list(future_cols.values())}", "INFO")

    # 识别因子列
    factor_cols = [c for c in df_all.columns
                   if any(k in c for k in ["动量_", "趋势_", "量能_", "估值_", "规模_", "技术_", "概念_"])]

    if not factor_cols:
        if verbose:
            log("  [全市场IC] 未找到因子列", "INFO")
        return None

    # 抽样
    if sample_size and len(df_all) > sample_size:
        df_all = df_all.sample(sample_size, random_state=42)

    # 直接用已回填的未来收益列计算IC，不拉K线
    from scipy.stats import spearmanr

    ic_results = {}
    total_samples = 0

    for d in hold_days:
        future_col = future_cols.get(d)
        if future_col is None:
            continue

        valid_mask = df_all[future_col].notna()
        df_valid = df_all[valid_mask]
        rets = df_valid[future_col].values
        total_samples = max(total_samples, len(df_valid))

        if len(rets) < 20:
            continue

        ic_dict = {}
        for col in factor_cols:
            scores = df_valid[col].values
            valid = ~np.isnan(scores)
            if valid.sum() < 10:
                continue
            if np.std(scores[valid]) < 1e-9:
                continue

            ic, _ = spearmanr(scores[valid], rets[valid])
            ic_dict[col] = round(float(ic), 6) if not np.isnan(ic) else 0.0

        ic_results[f"持有{d}天"] = dict(
            sorted(ic_dict.items(), key=lambda x: abs(x[1]), reverse=True)
        )

    if total_samples < 20:
        if verbose:
            log(f"  [全市场IC] 有效样本仅 {total_samples} 条 (<20)，数据不足", "INFO")
        return None

    if verbose:
        log(f"  [全市场IC] 总有效样本: {total_samples} 条", "INFO")

    # 组装报告
    date_groups = df_all["快照日期"].unique()
    report = {
        "分析类型": "全市场IC分析",
        "分析时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "快照日期数": len(date_groups),
        "分析股票数": df_all["代码"].nunique(),
        "总记录数": len(df_all),
        "有效样本数": total_samples,
        "因子数量": len(factor_cols),
        "IC分析": ic_results,
    }

    # 保存
    report_path = REPORTS_DIR / f"全市场IC_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    if verbose and ic_results:
        log(f"\n  [全市场IC] 报告已保存: {report_path.name}", "INFO")
        log(f"  [全市场IC] Top 10 有效因子 (持有{list(hold_days)[0]}天):", "INFO")
        for factor, ic_val in list(list(ic_results.values())[0].items())[:10]:
            stars = "★★★" if abs(ic_val) > 0.08 else "★★ " if abs(ic_val) > 0.04 else "★  "
            print(f"    {stars} {factor:20s} IC={ic_val:+.4f}")

    return report


def format_full_market_ic_report(report):
    """格式化全市场IC报告"""
    if report is None:
        return ""

    lines = []
    lines.append("")
    lines.append("╔" + "═" * 72 + "╗")
    lines.append("║  📊 全市场IC分析 — 基于全部股票因子得分")
    lines.append("╠" + "═" * 72 + "╣")
    lines.append(f"║  分析股票: {report['分析股票数']:>5d} 只  │  有效样本: {report['有效样本数']:>6d} 条")
    lines.append(f"║  快照天数: {report['快照日期数']:>5d} 天  │  分析因子: {report['因子数量']:>2d} 个")
    lines.append("╟" + "─" * 72 + "╢")

    ic = report.get("IC分析", {})
    for period, factors in ic.items():
        lines.append(f"║  [{period}] 有效因子 (Spearman IC):")
        for factor, ic_val in list(factors.items())[:10]:
            significance = "✓显著" if abs(ic_val) > 0.05 else "弱"
            lines.append(f"║    {factor:20s}  IC={ic_val:+.4f}  ({significance})")
        break

    lines.append("╚" + "═" * 72 + "╝")
    return "\n".join(lines)


# ============================================================
#  分层回测（Quantile Backtest）& 全样本验证
#  追加到 backtest_engine.py 的函数
#  使用方法：在 backtest_engine.py 末尾（if __name__ == "__main__": 之前）
#  插入以下内容
# ============================================================

# ────────────────────────────────────────────────────────
#  分层回测（Quantile Backtest）
#  将全市场股票按综合得分分为5层，验证每层未来收益是否单调
#  这是检验打分系统是否有效的最核心方法
# ────────────────────────────────────────────────────────

def run_quantile_backtest(hold_days=[1, 3, 5, 10, 20], n_quantiles=5, verbose=True):
    """
    分层回测：按综合得分将股票分为N层（五分位），验证每层未来收益。
    直接使用 factor_scores 中已回填的"未来X日收益"列，不拉K线。

    理想结果：
      - 第5层（高分）平均收益 > 第4层 > ... > 第1层（低分）
      - 说明打分越高，未来收益越好，打分系统有效

    Returns:
        dict: 分层回测报告，含每层的平均收益、胜率、样本数
    """
    FACTOR_SCORES_DIR = BASE_DIR / "data" / "factor_scores"
    if not FACTOR_SCORES_DIR.exists():
        if verbose:
            log("[分层回测] data/factor_scores/ 不存在，跳过", "WARN")
        return None

    parquet_files = sorted(FACTOR_SCORES_DIR.glob("factors_*.parquet"))
    if not parquet_files:
        if verbose:
            log("[分层回测] 无因子得分数据", "INFO")
        return None

    if verbose:
        log(f"\n[分层回测] 加载 {len(parquet_files)} 份因子得分文件...", "INFO")

    all_dfs = []
    for pf in parquet_files:
        try:
            all_dfs.append(pd.read_parquet(pf))
        except Exception as e:
            if verbose:
                print(f"  跳过 {pf.name}: {e}")

    if not all_dfs:
        return None

    df_all = pd.concat(all_dfs, ignore_index=True)
    df_all = df_all.drop_duplicates(subset=["快照日期", "代码"], keep="last")

    if "综合得分" not in df_all.columns:
        if verbose:
            log("[分层回测] 缺少", "INFO")
        return None

    # 检查已回填的未来收益列
    future_cols = {d: f"未来{d}日收益" for d in hold_days if f"未来{d}日收益" in df_all.columns}
    if not future_cols:
        if verbose:
            log("[分层回测] 无已回填的未来收益列，请先运行 backfill_future_returns", "INFO")
        return None

    if verbose:
        log(f"[分层回测] 合并: {df_all['代码'].nunique()} 只股票, {len(df_all)} 条记录", "INFO")
        log(f"[分层回测] 使用已回填的未来收益列: {list(future_cols.values())}", "INFO")

    # 按综合得分分层（五分位），直接用已回填的未来收益
    total_verified = 0
    quantile_results = {
        d: {q: {"returns": [], "wins": 0, "count": 0}
            for q in range(1, n_quantiles + 1)}
        for d in hold_days
    }

    for date_str in df_all["快照日期"].unique():
        group = df_all[df_all["快照日期"] == date_str].copy()
        if len(group) < n_quantiles * 10:
            continue

        # 按综合得分分层
        try:
            group["_quantile"] = pd.qcut(group["综合得分"], n_quantiles, labels=False, duplicates="drop") + 1
        except Exception:
            continue

        for d in hold_days:
            future_col = future_cols.get(d)
            if future_col is None:
                continue
            for _, row in group.iterrows():
                ret = row.get(future_col)
                if pd.isna(ret):
                    continue
                # BUG修复: qcut + duplicates="drop" 可能返回NaN（某行得分落在所有bins之外），
                # int(NaN)抛ValueError，导致该日期已累积的部分结果被异常中断
                q_raw = row["_quantile"]
                if pd.isna(q_raw):
                    continue
                quantile = int(q_raw)
                if quantile < 1 or quantile > n_quantiles:
                    continue  # duplicates="drop"可能产生超出范围的值
                qr = quantile_results[d][quantile]
                qr["returns"].append(float(ret))
                if ret > 0:
                    qr["wins"] += 1
                qr["count"] += 1
                total_verified = max(total_verified, total_verified + 1) if d == hold_days[0] else total_verified

    # 实际total_verified用第一个hold_days的总量
    total_verified = sum(quantile_results[hold_days[0]][q]["count"] for q in range(1, n_quantiles + 1))

    if total_verified < 50:
        if verbose:
            log(f"[分层回测] 有效样本不足 ({total_verified}条)", "INFO")
        return None

    # 汇总统计
    report = {
        "分析类型": "分层回测（Quantile Backtest）",
        "分析时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "分层数": n_quantiles,
        "持仓天数": hold_days,
        "有效样本数": total_verified,
        "分层结果": {},
        "打分有效性": {}
    }

    for d in hold_days:
        qt = quantile_results[d]
        layer_stats = {}
        avg_returns = []

        for q in range(1, n_quantiles + 1):
            r = qt[q]["returns"]
            if len(r) == 0:
                continue
            avg_ret = float(np.mean(r))
            win_rate = qt[q]["wins"] / qt[q]["count"] if qt[q]["count"] > 0 else 0
            layer_stats[f"第{q}层"] = {
                "平均收益": round(avg_ret, 6),
                "胜率": round(float(win_rate), 4),
                "样本数": qt[q]["count"]
            }
            avg_returns.append(avg_ret)

        report["分层结果"][f"持有{d}天"] = layer_stats

        # 检验单调性
        if len(avg_returns) == n_quantiles:
            monotonic = all(avg_returns[i] <= avg_returns[i + 1]
                           for i in range(len(avg_returns) - 1))
            top_bottom_spread = avg_returns[-1] - avg_returns[0]
            report["打分有效性"][f"持有{d}天"] = {
                "单调": monotonic,
                "层差": round(float(top_bottom_spread), 6),
                "最高层收益": round(float(avg_returns[-1]), 6),
                "最低层收益": round(float(avg_returns[0]), 6),
            }

    # 保存报告
    report_path = REPORTS_DIR / f"分层回测_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    if verbose:
        log(f"[分层回测] 完成，报告已保存: {report_path.name}", "INFO")
        _print_quantile_report(report)

    return report


def _print_quantile_report(report):
    """打印分层回测报告到终端"""
    print("\n  " + "=" * 60)
    print("  分层回测结果（综合得分越高，未来收益应越高）")
    print("  " + "=" * 60)
    for period, layers in report.get("分层结果", {}).items():
        print(f"\n  [{period}]")
        print(f"  {'层级':<10} {'平均收益':>12} {'胜率':>8} {'样本数':>8}")
        for layer, stats in layers.items():
            print(f"  {layer:<10} {stats['平均收益']:>+12.2%} {stats['胜率']:>7.1%} {stats['样本数']:>8d}")
        valid = report.get("打分有效性", {}).get(period, {})
        mono = "单调 OK" if valid.get("单调") else "非单调 FAIL"
        spread = valid.get("层差", 0)
        print(f"  有效性: {mono}  最高-最低层差={spread:+.2%}")


# ────────────────────────────────────────────────────────
#  全样本验证
#  不只验证TopN，而是验证所有有因子得分的股票
# ────────────────────────────────────────────────────────

def run_full_sample_validation(hold_days=[1, 3, 5, 10], sample_size=2000, verbose=True):
    """
    全样本验证：验证综合得分与未来收益的IC。
    直接使用 factor_scores 中已回填的"未来X日收益"列，不拉K线。

    与 run_backtest 的区别：
    - run_backtest: 只验证每次选出的TopN（约10只），拉K线
    - run_full_sample_validation: 验证所有已回填的股票（不拉K线），
      检验打分系统对全市场是否有效

    Returns:
        dict: 全样本验证报告
    """
    FACTOR_SCORES_DIR = BASE_DIR / "data" / "factor_scores"
    if not FACTOR_SCORES_DIR.exists():
        return None

    parquet_files = sorted(FACTOR_SCORES_DIR.glob("factors_*.parquet"))
    if not parquet_files:
        return None

    # 只取最近10个快照
    recent_files = parquet_files[-10:]

    all_dfs = []
    for pf in recent_files:
        try:
            all_dfs.append(pd.read_parquet(pf))
        except Exception as e:
            log(f"操作异常: {e}", "WARN")

    if not all_dfs:
        return None

    df_all = pd.concat(all_dfs, ignore_index=True)
    df_all = df_all.drop_duplicates(subset=["快照日期", "代码"], keep="last")

    if "综合得分" not in df_all.columns:
        return None

    # 检查已回填的未来收益列
    future_cols = {d: f"未来{d}日收益" for d in hold_days if f"未来{d}日收益" in df_all.columns}
    if not future_cols:
        if verbose:
            log("[全样本验证] 无已回填的未来收益列，请先运行 backfill_future_returns", "INFO")
        return None

    # 抽样
    if sample_size and len(df_all) > sample_size:
        df_all = df_all.sample(sample_size, random_state=42)

    if verbose:
        log(f"\n[全样本验证] 验证 {len(df_all)} 只股票的综合得分与未来收益相关性...", "INFO")
        log(f"[全样本验证] 使用已回填的未来收益列: {list(future_cols.values())}", "INFO")

    # 直接用已回填的未来收益列，不拉K线
    from scipy.stats import spearmanr
    results = {}

    for d in hold_days:
        future_col = future_cols.get(d)
        if future_col is None:
            continue

        valid = df_all["综合得分"].notna() & df_all[future_col].notna()
        scores = df_all.loc[valid, "综合得分"].values
        returns = df_all.loc[valid, future_col].values

        if len(scores) < 30:
            continue

        ic, pval = spearmanr(scores, returns)

        # 分层（五分位）平均收益
        df_pair = pd.DataFrame({"score": scores, "return": returns})
        try:
            df_pair["quantile"] = pd.qcut(df_pair["score"], 5, labels=False, duplicates="drop")
            quantile_avg = df_pair.groupby("quantile")["return"].mean()
            quantile_vals = [round(float(x), 6) for x in quantile_avg.values]
            monotonic = all(quantile_avg.iloc[i] <= quantile_avg.iloc[i + 1]
                            for i in range(len(quantile_avg) - 1))
        except Exception:
            quantile_vals = []
            monotonic = False

        results[f"持有{d}天"] = {
            "IC": round(float(ic), 4),
            "p值": round(float(pval), 4),
            "样本数": len(scores),
            "显著": pval < 0.05,
            "五分位平均收益": quantile_vals,
            "单调性": monotonic
        }

    if not results:
        return None

    report = {
        "分析类型": "全样本验证",
        "分析时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "验证样本数": len(df_all),
        "结果": results
    }

    report_path = REPORTS_DIR / f"全样本验证_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    if verbose:
        log(f"[全样本验证] 完成，报告: {report_path.name}", "INFO")
        for period, r in results.items():
            ic_str = f"IC={r['IC']:+.4f}" + (" 显著" if r["显著"] else " 不显著")
            mono_str = "单调OK" if r["单调性"] else "非单调FAIL"
            print(f"    {period}: {ic_str}  {mono_str}  样本={r['样本数']}")

    return report


# ============================================================
#  直接运行（本地调试）
# ============================================================
if __name__ == "__main__":
    print("=" * 60)
    print("  回测引擎 本地调试")
    print("=" * 60)

    # 1. TopN 验证
    report = run_backtest(hold_days=[1, 3, 5, 10])
    if report:
        print(format_backtest_report(report))
    else:
        print("需要积累选股历史数据后才能回测。")
        print("运行 stock_picker.py 几次，产生 history/ 目录下的快照文件。")

    # 2. 全市场IC分析（用已回填数据，不拉K线）
    print("\n" + "=" * 60)
    print("  全市场 IC 分析")
    print("=" * 60)
    ic_report = run_full_market_ic(hold_days=[1, 3, 5, 10])
    if ic_report:
        print(format_full_market_ic_report(ic_report))
    else:
        print("需要积累因子得分数据（data/factor_scores/）后才能做全市场IC分析。")


# ============================================================
#  未来收益回填 — 给 factor_scores 文件添加未来收益列
# ============================================================

def backfill_future_returns(hold_days=[1, 3, 5, 10], max_files=None,
                             sample_size=500, verbose=True, max_seconds=1200):
    """
    给 data/factor_scores/ 中的每个文件回填未来收益列。

    进化系统的 tune_weekly() 需要因子数据中包含"未来X日收益"列来计算IC。
    本函数读取每个 factor_scores 文件，用K线缓存计算未来收益，写回文件。

    Args:
        hold_days: 持有天数列表
        max_files: 最多处理多少个文件（None=全部）
        sample_size: 每个文件抽样多少只股票（IC计算不需要全量，300只足够）
        verbose: 打印进度

    Returns:
        dict: {"处理文件数": N, "更新记录数": N, "跳过文件数": N}
    """
    FACTOR_SCORES_DIR = BASE_DIR / "data" / "factor_scores"
    if not FACTOR_SCORES_DIR.exists():
        if verbose:
            print("  [回填] data/factor_scores/ 不存在")
        return {"处理文件数": 0, "更新记录数": 0, "跳过文件数": 0}

    parquet_files = sorted(FACTOR_SCORES_DIR.glob("factors_*.parquet"))
    if not parquet_files:
        return {"处理文件数": 0, "更新记录数": 0, "跳过文件数": 0}

    # 按快照日期去重：每个日期只处理最后一个文件（与 factor_tuner 的 dedup 逻辑一致）
    _seen_dates = {}
    for pf in parquet_files:
        try:
            _df_tmp = pd.read_parquet(pf, columns=["快照日期"])
            if len(_df_tmp) > 0:
                _d = str(_df_tmp["快照日期"].iloc[0])[:10]
                _seen_dates[_d] = pf  # 后面的覆盖前面的，保留最新
        except Exception:
            continue
    unique_files = list(_seen_dates.values())

    future_col_names = [f"未来{d}日收益" for d in hold_days]
    today = datetime.now().date()
    processed = 0
    updated_records = 0
    skipped = 0
    _start_time = time.time()

    for pf in unique_files:
        if max_files and processed >= max_files:
            break
        if time.time() - _start_time > max_seconds:
            if verbose:
                print(f"  [回填] 达到时间限制({max_seconds}s)，停止处理")
            break

        try:
            df = pd.read_parquet(pf)
        except Exception:
            skipped += 1
            continue

        # 已经充分回填的文件跳过（填充率>50%视为已完成）
        existing_cols = [c for c in future_col_names if c in df.columns]
        if len(existing_cols) == len(future_col_names):
            # 检查所有未来收益列的填充率，取最低值
            # BUG修复: 不能只检查第一列——未来1日收益可能已充分填充，
            # 但未来10日收益可能为空（run_backtest只写回Top10）
            min_fill_rate = min(df[col].notna().mean() for col in future_col_names)
            if min_fill_rate > 0.5:
                skipped += 1
                if verbose:
                    print(f"  [回填] 跳过 {pf.name}: 已充分回填 (最低填充率{min_fill_rate:.0%})")
                continue
            # 最低填充率低（如run_backtest只写了Top10），继续处理填充剩余股票
            if verbose:
                print(f"  [回填] {pf.name}: 列已存在但最低填充率仅{min_fill_rate:.0%}，继续补填")

        # --- 以下为文件核心处理逻辑，必须用try-except保护 ---
        # BUG修复: 之前整个处理逻辑无异常保护，verify_pick_performance
        # 或to_parquet崩溃会导致后续所有文件不被处理，且可能损坏当前文件
        try:
            # 从文件名或列中获取快照日期
            if "快照日期" not in df.columns:
                skipped += 1
                continue

            snap_date_str = str(df["快照日期"].iloc[0])[:10]
            snap_date = datetime.strptime(snap_date_str, "%Y-%m-%d")

            # 快照日期太近（未来数据不足）的跳过：需要 max(hold_days)+2 个交易日
            min_gap_days = max(hold_days) + 2  # 10交易日≈14自然日，+2天缓冲足够
            if (today - snap_date.date()).days < min_gap_days:
                skipped += 1
                if verbose:
                    print(f"  [回填] 跳过 {pf.name}: 快照日期 {snap_date_str} 太近，数据不足")
                continue

            # 初始化未来收益列
            for col in future_col_names:
                if col not in df.columns:
                    df[col] = np.nan

            # 抽样：按综合得分排序取TopN + 随机抽样，IC计算不需要全量
            if "综合得分" in df.columns and len(df) > sample_size:
                # 取Top一半 + 随机一半，保证IC分析有区分度
                top_n = sample_size // 2
                df_sorted = df.sort_values("综合得分", ascending=False)
                sample_idx = list(df_sorted.head(top_n).index)
                remaining = [i for i in df.index if i not in sample_idx]
                import random
                random.seed(42)
                sample_idx += random.sample(remaining, min(sample_size - top_n, len(remaining)))
                process_rows = df.loc[sample_idx]
            else:
                process_rows = df

            # 对抽样股票计算未来收益（并发拉K线，加速回填）
            filled = 0
            _pending = []  # (idx, code) 需要拉K线的
            for idx, row in process_rows.iterrows():
                code = str(row.get("代码", "")).zfill(6)
                if not code or len(code) != 6:
                    continue
                # 已有有效值的跳过
                if all(pd.notna(row.get(col)) for col in future_col_names):
                    filled += 1
                    continue
                _pending.append((idx, code))

            if _pending:
                from concurrent.futures import ThreadPoolExecutor, as_completed

                def _fetch_one(item):
                    idx, code = item
                    perf = verify_pick_performance(code, snap_date, hold_days)
                    return idx, perf

                # 并发3线程拉K线（回填不赶时间，避免触发API风控）
                with ThreadPoolExecutor(max_workers=3) as executor:
                    futures = {executor.submit(_fetch_one, item): item for item in _pending}
                    for future in as_completed(futures):
                        try:
                            idx, perf = future.result()
                            if perf is None:
                                continue
                            for d in hold_days:
                                ret = perf.get(d)
                                if ret is not None:
                                    df.at[idx, f"未来{d}日收益"] = ret
                            filled += 1
                        except Exception:
                            continue

            # 原子写入：先写临时文件，成功后再rename覆盖原文件
            # 防止to_parquet写到一半失败导致原文件损坏
            import tempfile
            tmp_fd, tmp_path = tempfile.mkstemp(suffix=".parquet", dir=str(pf.parent))
            try:
                df.to_parquet(tmp_path, index=False)
                os.close(tmp_fd)
                tmp_fd = -1  # 已关闭，不再需要close
                # Windows上rename需要先删除目标（os.rename不能覆盖）
                if pf.exists():
                    pf.unlink()
                os.rename(tmp_path, str(pf))
            except Exception as write_err:
                if tmp_fd >= 0:
                    try:
                        os.close(tmp_fd)
                    except Exception:
                        pass
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)
                raise write_err  # 抛出，让外层except处理

            processed += 1
            updated_records += filled

            if verbose:
                print(f"  [回填] {pf.name}: 快照={snap_date_str}, "
                      f"填充={filled}/{len(df)} 只")

        except Exception as proc_err:
            if verbose:
                print(f"  [回填] 处理 {pf.name} 异常: {proc_err}（跳过，不影响其他文件）")
            skipped += 1
            continue

    if verbose:
        print(f"  [回填] 完成: 处理={processed} 文件, 更新={updated_records} 记录, 跳过={skipped} 文件")

    return {"处理文件数": processed, "更新记录数": updated_records, "跳过文件数": skipped}
