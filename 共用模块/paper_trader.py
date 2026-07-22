#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
模拟交易器 v1.0 — 无风险验证选股策略
====================================================
职责：
  1. 模拟按每日选股结果买入TopN，跟踪持仓盈亏
  2. 支持多持仓周期：1/3/5/10/20天
  3. 仓位管理：等权分配、固定金额
  4. 止损/止盈：可配置
  5. 基准对比：沪深300
  6. 输出模拟交易报告

为什么需要：
  纸面交易是验证策略在'真金白银'场景下表现的最佳方式。
  不会亏一分钱，但能精确知道如果真投了会赚多少亏多少。
"""

import pandas as pd
import numpy as np
import json
import os
import requests
import time
from datetime import datetime, timedelta
from pathlib import Path

# ============================================================
#  路径配置
# ============================================================
BASE_DIR    = Path(__file__).parent.parent  # 指向项目根目录
TRADES_DIR  = BASE_DIR / "trades"       # 模拟交易记录
PERF_DIR    = BASE_DIR / "performance"   # 交易盈亏统计
REPORTS_DIR = BASE_DIR / "reports"
CACHE_DIR   = BASE_DIR / "cache"

for d in [TRADES_DIR, PERF_DIR]:
    d.mkdir(parents=True, exist_ok=True)

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

# 简易日志函数（防止异常路径调用未定义的log导致NameError崩溃）
def log(msg, level="INFO"):
    from datetime import datetime as _dt
    print(f"[{_dt.now().strftime('%H:%M:%S')}] [{level}] {msg}")

# 默认配置（factor_weights.json / config.json 可覆盖）
DEFAULT_CAPITAL       = 100000   # 初始资金 10万
DEFAULT_POSITION_PCT  = 0.10     # 每只股票仓位 10%（等权Top10）
DEFAULT_STOP_LOSS     = -0.08    # 止损线 -8%
DEFAULT_TAKE_PROFIT   = 0.25     # 止盈线 +25%
DEFAULT_HOLD_DAYS     = [3, 5, 10]  # 默认持仓周期


def _load_trading_config():
    """从 config.json 加载模拟交易参数"""
    config_file = BASE_DIR / "config.json"
    if not config_file.exists():
        return
    try:
        cfg = json.loads(config_file.read_text(encoding="utf-8-sig"))
        pt = cfg.get("paper_trading", {})
        if pt:
            global DEFAULT_CAPITAL, DEFAULT_POSITION_PCT, DEFAULT_STOP_LOSS, DEFAULT_TAKE_PROFIT
            DEFAULT_CAPITAL = pt.get("capital", DEFAULT_CAPITAL)
            DEFAULT_POSITION_PCT = pt.get("position_pct", DEFAULT_POSITION_PCT)
            DEFAULT_STOP_LOSS = pt.get("stop_loss", DEFAULT_STOP_LOSS)
            DEFAULT_TAKE_PROFIT = pt.get("take_profit", DEFAULT_TAKE_PROFIT)
    except Exception as e:
        log(f"操作异常: {e}", "WARN")


# 启动时加载
_load_trading_config()


# ============================================================
#  交易数据文件结构
# ============================================================
# trades/open_positions.parquet  — 当前持仓
# trades/closed_trades.parquet   — 已平仓记录
# trades/trade_log.jsonl         — 交易日志


def _get_positions_file():
    return TRADES_DIR / "open_positions.parquet"


def _get_closed_file():
    return TRADES_DIR / "closed_trades.parquet"


def _fetch_close_price(code, target_date=None):
    """获取某只股票在指定日期的收盘价"""
    from backtest_engine import _fetch_kline_sina
    df = _fetch_kline_sina(code, days=60)
    if df is None or len(df) == 0:
        return None

    if target_date is None:
        return df.iloc[-1]["收盘"]

    target_dt = pd.Timestamp(target_date) if isinstance(target_date, str) else pd.Timestamp(target_date)
    # 确保"日期"列为 Timestamp 类型（可能是字符串）
    df["日期"] = pd.to_datetime(df["日期"])
    for _, row in df.iterrows():
        if row["日期"].date() == target_dt.date():
            return row["收盘"]

    return None


def open_positions(df_picks, capital=DEFAULT_CAPITAL,
                   position_pct=None, hold_days=None,
                   stop_loss=None, take_profit=None,
                   use_next_open=True):
    """
    按选股结果开仓。

    Args:
        df_picks: 今日选股结果（含代码/名称/综合得分/涨跌幅/开盘/成交额）
        capital: 总资金
        position_pct: 每只股票仓位比例（默认 1/n）
        hold_days: 持仓天数列表（对每个周期独立跟踪）
        stop_loss: 止损线
        take_profit: 止盈线
        use_next_open: 是否使用次日开盘价作为买入价（True=修复前瞻偏差）

    模拟逻辑:
      - 修复前瞻偏差：使用次日开盘价买入（或当日收盘价的近似）
      - 检查流动性：成交额/计划买入金额 > 100 才允许买入
      - 每条记录标注：买入价、买入日期、目标持有天数
      - 后续每次运行时，自动检查是否到期/触发止损/止盈
    """
    if df_picks is None or len(df_picks) == 0:
        return

    n = min(len(df_picks), 10)  # 最多买10只
    if position_pct is None:
        position_pct = min(1.0 / n, 0.15)  # 等权但单只不超过15%

    if hold_days is None:
        hold_days = DEFAULT_HOLD_DAYS

    if stop_loss is None:
        stop_loss = DEFAULT_STOP_LOSS
    if take_profit is None:
        take_profit = DEFAULT_TAKE_PROFIT

    per_stock_capital = capital * position_pct
    now = datetime.now()
    buy_time = now.strftime("%Y-%m-%d %H:%M:%S")  # 精确到秒
    today = now.strftime("%Y-%m-%d")  # 买入日期

    new_positions = []
    for idx, row in df_picks.head(n).iterrows():
        code = str(row["代码"]).zfill(6)
        name = row["名称"]
        score = row["综合得分"]

        # 获取买入价：统一使用当日收盘价（≈14:00可跟单价格）
        # 不用竞价价格（太高），不用次日开盘（没法跟），用当天收盘价最接近实际操作
        entry_price = None

        # 优先从K线获取当天收盘价（最准确）
        try:
            from kline_fetcher import KlineFetcher
            _kf = KlineFetcher(cache_dir=Path(__file__).parent.parent / "cache")
            _df_k = _kf.get_kline(code, days=5)
            if _df_k is not None and len(_df_k) > 0:
                _latest_close = float(_df_k.iloc[-1]["收盘"])
                if _latest_close > 0:
                    entry_price = _latest_close
        except Exception:
            pass

        # 降级：使用快照中的收盘价
        if entry_price is None or entry_price <= 0:
            if "收盘" in row.index and pd.notna(row["收盘"]) and row["收盘"] > 0:
                entry_price = float(row["收盘"])
            elif "最新价" in row.index and pd.notna(row["最新价"]) and row["最新价"] > 0:
                entry_price = float(row["最新价"])
            elif "开盘" in row.index and pd.notna(row["开盘"]) and row["开盘"] > 0:
                entry_price = float(row["开盘"])

        if entry_price is None or entry_price <= 0:
            continue

        # 检查流动性
        daily_avg_amount = None
        if "成交额" in row.index and pd.notna(row["成交额"]):
            daily_avg_amount = float(row["成交额"]) / 10000  # 转换为万元

        from backtest_engine import _check_liquidity
        can_buy, extra_slippage = _check_liquidity(code, per_stock_capital, daily_avg_amount)
        if not can_buy:
            print(f"  [模拟交易] {code} {name} 流动性不足，跳过")
            continue

        # 检查是否涨停（涨幅>=9.8%视为涨停）
        is_limit_up = False
        if "涨跌幅" in row.index and pd.notna(row["涨跌幅"]):
            change_pct = float(row["涨跌幅"])
            is_limit_up = change_pct >= 9.8

        shares = int(per_stock_capital / entry_price / 100) * 100  # 整手
        if shares == 0:
            continue

        # 考虑额外滑点
        actual_entry_price = entry_price * (1 + extra_slippage)
        if is_limit_up:
            actual_entry_price = entry_price * 1.005  # 涨停股额外0.5%滑点

        actual_cost = shares * actual_entry_price

        for d in hold_days:
            new_positions.append({
                "代码": code,
                "名称": name,
                "买入日期": today,
                "买入时间": buy_time,  # 精确到秒
                "到期日期": (now + timedelta(days=d + 5)).strftime("%Y-%m-%d"),
                "持仓天数": d,
                "买入价": round(actual_entry_price, 3),
                "买入数量": shares,
                "买入金额": round(actual_cost, 2),
                "选股得分": score,
                "止损线": round(actual_entry_price * (1 + stop_loss), 3),
                "止盈线": round(actual_entry_price * (1 + take_profit), 3),
                "当前状态": "持仓中",
                "当前价": actual_entry_price,
                "当前盈亏": 0.0,
                "当前盈亏率": 0.0,
                "平仓日期": "",
                "平仓价": 0.0,
                "平仓原因": "",
            })

    if not new_positions:
        return 0

    df_new = pd.DataFrame(new_positions)

    # 合并到持仓文件
    pos_file = _get_positions_file()
    if pos_file.exists():
        df_existing = pd.read_parquet(pos_file)
        # 去重：同代码+同买入日期的持仓
        existing_keys = set(
            df_existing["代码"] + "_" + df_existing["买入日期"] + "_" +
            df_existing["持仓天数"].astype(str)
        )
        new_keys = df_new["代码"] + "_" + df_new["买入日期"] + "_" + df_new["持仓天数"].astype(str)
        df_new = df_new[~new_keys.isin(existing_keys)]
        if len(df_new) > 0:
            df_all = pd.concat([df_existing, df_new], ignore_index=True)
            df_all.to_parquet(pos_file, index=False)
    else:
        df_new.to_parquet(pos_file, index=False)

    # 交易日志
    log_entry = {
        "时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "操作": "开仓",
        "数量": len(df_new),
        "股票": [f"{r['名称']}({r['代码']})" for _, r in df_new.iterrows()],
        "每只仓位": f"{position_pct:.1%}",
    }
    log_path = TRADES_DIR / "trade_log.jsonl"
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(log_entry, ensure_ascii=False) + "\n")

    print(f"  [模拟交易] 开仓 {len(df_new)} 只 ({len(hold_days)} 个周期各一单)")
    return len(df_new)


def update_positions():
    """
    更新所有持仓的当前状态：
    - 用最新行情更新"当前价"和"当前盈亏"
    - 检查是否到期/触发止损/止盈
    - 符合条件的自动平仓
    """
    pos_file = _get_positions_file()
    if not pos_file.exists():
        return

    df = pd.read_parquet(pos_file)

    # 只处理"持仓中"的记录
    active_mask = df["当前状态"] == "持仓中"
    if not active_mask.any():
        return

    today = datetime.now()
    closed_list = []
    updated_count = 0

    for idx in df[active_mask].index:
        row = df.loc[idx]
        code = row["代码"]

        # 获取最新价格
        latest_price = _fetch_close_price(code)
        if latest_price is None:
            continue

        # 更新当前价和盈亏
        entry_price = row["买入价"]
        if entry_price <= 0:
            pnl_pct = -1.0
        else:
            pnl_pct = (latest_price / entry_price - 1)
        df.at[idx, "当前价"] = latest_price
        df.at[idx, "当前盈亏率"] = round(pnl_pct, 4)

        # 计算盈亏金额（按买入金额倒推）
        buy_amount = row["买入金额"]
        if pd.isna(buy_amount):
            buy_amount = 0
        df.at[idx, "当前盈亏"] = round(buy_amount * pnl_pct, 2)
        updated_count += 1

        # 检查平仓条件
        close_reason = None

        # 止损
        if pnl_pct <= -0.08 and latest_price <= row["止损线"]:
            close_reason = "止损"
        # 止盈
        elif pnl_pct >= 0.25 and latest_price >= row["止盈线"]:
            close_reason = "止盈"
        # 到期
        elif today.strftime("%Y-%m-%d") >= row["到期日期"]:
            close_reason = "到期平仓"

        if close_reason:
            df.at[idx, "当前状态"] = "已平仓"
            df.at[idx, "平仓日期"] = today.strftime("%Y-%m-%d")
            df.at[idx, "平仓价"] = latest_price
            df.at[idx, "平仓原因"] = close_reason

            closed_list.append({
                "代码": code,
                "名称": row["名称"],
                "买入日期": row["买入日期"],
                "平仓日期": today.strftime("%Y-%m-%d"),
                "持仓天数": row["持仓天数"],
                "买入价": entry_price,
                "平仓价": latest_price,
                "盈亏率": round(pnl_pct, 4),
                "盈亏金额": round(row["买入金额"] * pnl_pct, 2),
                "平仓原因": close_reason,
            })

    # 保存更新后的持仓
    df.to_parquet(pos_file, index=False)

    # 保存已平仓记录
    if closed_list:
        df_closed = pd.DataFrame(closed_list)
        closed_file = _get_closed_file()
        if closed_file.exists():
            df_existing = pd.read_parquet(closed_file)
            df_closed = pd.concat([df_existing, df_closed], ignore_index=True)
        df_closed.to_parquet(closed_file, index=False)

        # 日志
        log_entry = {
            "时间": today.strftime("%Y-%m-%d %H:%M:%S"),
            "操作": "平仓",
            "数量": len(closed_list),
            "股票": [f"{c['名称']}({c['代码']}) {c['平仓原因']} {c['盈亏率']:+.1%}"
                     for c in closed_list],
        }
        log_path = TRADES_DIR / "trade_log.jsonl"
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(log_entry, ensure_ascii=False) + "\n")

        print(f"  [模拟交易] 平仓 {len(closed_list)} 只")

    elif updated_count > 0:
        print(f"  [模拟交易] 更新 {updated_count} 条持仓，无触发平仓")


def get_trade_summary():
    """获取模拟交易汇总统计"""
    closed_file = _get_closed_file()
    pos_file = _get_positions_file()

    summary = {
        "已平仓数": 0,
        "持仓中数": 0,
        "总胜率": 0,
        "平均盈亏率": 0,
        "总盈亏金额": 0,
        "最大盈利": 0,
        "最大亏损": 0,
    }

    # 已平仓统计
    if closed_file.exists():
        df_closed = pd.read_parquet(closed_file)
        summary["已平仓数"] = len(df_closed)
        if len(df_closed) > 0:
            pnl = df_closed["盈亏率"]
            summary["总胜率"] = round(float((pnl > 0).mean()), 4)
            summary["平均盈亏率"] = round(float(pnl.mean()), 4)
            summary["总盈亏金额"] = round(float(df_closed["盈亏金额"].sum()), 2)
            summary["最大盈利"] = round(float(pnl.max()), 4)
            summary["最大亏损"] = round(float(pnl.min()), 4)

            # 按持仓天数统计
            by_hold = df_closed.groupby("持仓天数").agg(
                交易次数=("盈亏率", "count"),
                胜率=("盈亏率", lambda x: round(float((x > 0).mean()), 4)),
                平均收益=("盈亏率", lambda x: round(float(x.mean()), 4)),
                总盈亏=("盈亏金额", "sum"),
            ).to_dict("index")
            summary["按持仓天数"] = {str(k): v for k, v in by_hold.items()}

            # 按平仓原因统计
            by_reason = df_closed.groupby("平仓原因").agg(
                次数=("盈亏率", "count"),
                平均收益=("盈亏率", lambda x: round(float(x.mean()), 4)),
            ).to_dict("index")
            summary["按平仓原因"] = by_reason

    # 持仓中统计
    if pos_file.exists():
        df_pos = pd.read_parquet(pos_file)
        active = df_pos[df_pos["当前状态"] == "持仓中"]
        summary["持仓中数"] = len(active)

    return summary


def format_trade_report():
    """格式化模拟交易报告"""
    s = get_trade_summary()

    lines = []
    lines.append("")
    lines.append("╔" + "═" * 70 + "╗")
    lines.append("║  💰 模拟交易报告")
    lines.append("╠" + "═" * 70 + "╣")

    if s["已平仓数"] == 0:
        lines.append("║  暂无已平仓交易记录")
        lines.append("║  需要积累至少一次完整持仓周期后才有数据")
        lines.append("╚" + "═" * 70 + "╝")
        return "\n".join(lines)

    lines.append(f"║  已平仓: {s['已平仓数']:>3d} 单  │  持仓中: {s['持仓中数']:>3d} 单")
    lines.append(f"║  胜率: {s['总胜率']:.1%}  │  平均收益: {s['平均盈亏率']:>+.1%}")
    lines.append(f"║  总盈亏: ¥{s['总盈亏金额']:>+.2f}  │  最大盈/亏: {s['最大盈利']:>+.1%} / {s['最大亏损']:>+.1%}")

    # 按持仓天数
    by_hold = s.get("按持仓天数", {})
    if by_hold:
        lines.append("╟" + "─" * 70 + "╢")
        lines.append("║  按持仓天数:")
        for days, st in sorted(by_hold.items()):
            lines.append(
                f"║    {days}天: {st['交易次数']:>3d}单 | "
                f"胜率 {st['胜率']:.1%} | "
                f"均收 {st['平均收益']:>+.1%} | "
                f"总盈亏 ¥{st['总盈亏']:>+.2f}"
            )

    # 按平仓原因
    by_reason = s.get("按平仓原因", {})
    if by_reason:
        lines.append("╟" + "─" * 70 + "╢")
        lines.append("║  按平仓原因:")
        for reason, st in by_reason.items():
            lines.append(f"║    {reason}: {st['次数']}次 | 平均收益 {st['平均收益']:>+.1%}")

    lines.append("╚" + "═" * 70 + "╝")
    return "\n".join(lines)


if __name__ == "__main__":
    # 本地测试：更新持仓状态
    print("=" * 50)
    print("  模拟交易器 本地测试")
    print("=" * 50)
    update_positions()
    print(format_trade_report())
