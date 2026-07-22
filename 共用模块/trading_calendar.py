#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
中国A股交易日历 — 动态获取 + 硬编码兜底
====================================================
优先从akshare动态获取交易日历（覆盖所有年份），
网络不可用时回退到内置节假日表。

用法:
    from trading_calendar import is_trading_day, next_trading_day
    if is_trading_day(datetime(2027, 10, 1)):
        print("交易日")
"""
from datetime import datetime, date, timedelta
from pathlib import Path
import json

# 简易日志函数（防止异常路径调用未定义的log导致NameError崩溃）
def log(msg, level="INFO"):
    from datetime import datetime as _dt
    print(f"[{_dt.now().strftime('%H:%M:%S')}] [{level}] {msg}")

# ============================================================
#  缓存配置
# ============================================================
_CACHE_DIR = Path(__file__).parent.parent / "cache"
_CACHE_FILE = _CACHE_DIR / "trading_calendar.json"
_CACHE_MAX_AGE_DAYS = 7  # 缓存7天刷新

# ============================================================
#  硬编码兜底（akshare不可用时使用）
# ============================================================
_HOLIDAYS_FALLBACK = {
    2026: {
        (1, 1), (1, 2), (1, 3),
        (1, 28), (1, 29), (1, 30), (1, 31),
        (2, 1), (2, 2), (2, 3), (2, 4),
        (4, 4), (4, 5), (4, 6),
        (5, 1), (5, 2), (5, 3), (5, 4), (5, 5),
        (6, 19), (6, 20), (6, 21),
        (10, 1), (10, 2), (10, 3), (10, 4), (10, 5), (10, 6), (10, 7),
    },
}


# ============================================================
#  动态获取（akshare）
# ============================================================

def _load_trading_days_from_akshare():
    """从akshare获取全部交易日"""
    try:
        import akshare as ak
        df = ak.tool_trade_date_hist_sina()
        if df is not None and len(df) > 0:
            dates = set()
            for _, row in df.iterrows():
                d = str(row["trade_date"])[:10]
                dates.add(d)
            return dates
    except Exception as e:
        log(f"操作异常: {e}", "WARN")
    return None


def _load_trading_days_cache():
    """从缓存加载交易日"""
    if not _CACHE_FILE.exists():
        return None
    try:
        data = json.loads(_CACHE_FILE.read_text(encoding="utf-8"))
        cached_at = datetime.fromisoformat(data["cached_at"])
        if (datetime.now() - cached_at).days > _CACHE_MAX_AGE_DAYS:
            return None  # 缓存过期
        return set(data["trading_days"])
    except Exception as e:
        log(f"操作异常: {e}", "WARN")
        return None


def _save_trading_days_cache(trading_days):
    """保存交易日到缓存"""
    try:
        _CACHE_DIR.mkdir(parents=True, exist_ok=True)
        data = {
            "cached_at": datetime.now().isoformat(),
            "trading_days": sorted(trading_days),
        }
        _CACHE_FILE.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    except Exception as e:
        log(f"操作异常: {e}", "WARN")


def _get_trading_days():
    """获取交易日集合（缓存 → akshare → 硬编码）"""
    # 1. 尝试缓存
    days = _load_trading_days_cache()
    if days:
        return days, "cache"

    # 2. 尝试akshare
    days = _load_trading_days_from_akshare()
    if days:
        _save_trading_days_cache(days)
        return days, "akshare"

    # 3. 回退到硬编码（仅当年有效）
    return None, "none"


# ============================================================
#  公开接口
# ============================================================

_TRADING_DAYS = None
_SOURCE = None


def _ensure_loaded():
    global _TRADING_DAYS, _SOURCE
    if _TRADING_DAYS is None:
        _TRADING_DAYS, _SOURCE = _get_trading_days()


def is_trading_day(dt=None) -> bool:
    """
    判断是否为 A 股交易日。
    优先使用动态交易日历，回退到硬编码规则。
    """
    if dt is None:
        dt = datetime.now()
    d = dt.date() if isinstance(dt, datetime) else dt
    d_str = d.strftime("%Y-%m-%d")

    _ensure_loaded()

    if _TRADING_DAYS is not None:
        return d_str in _TRADING_DAYS

    # 回退: 简单规则（仅排除周末+硬编码节假日）
    if d.weekday() >= 5:
        return False
    holidays = _HOLIDAYS_FALLBACK.get(d.year, set())
    return (d.month, d.day) not in holidays


def next_trading_day(dt=None) -> date:
    """返回下一个交易日"""
    if dt is None:
        dt = datetime.now()
    d = dt.date() if isinstance(dt, datetime) else dt
    for i in range(1, 30):
        next_d = d + timedelta(days=i)
        if is_trading_day(next_d):
            return next_d
    return d + timedelta(days=1)


def trading_days_between(start: date, end: date) -> int:
    """计算两个日期之间的交易日数量"""
    count = 0
    d = start
    while d <= end:
        if is_trading_day(d):
            count += 1
        d += timedelta(days=1)
    return count


def get_calendar_source():
    """返回当前使用的日历数据源"""
    _ensure_loaded()
    return _SOURCE


def refresh_calendar():
    """强制刷新交易日历缓存"""
    global _TRADING_DAYS, _SOURCE
    _TRADING_DAYS = None
    _SOURCE = None
    if _CACHE_FILE.exists():
        _CACHE_FILE.unlink()
    _ensure_loaded()
    return _SOURCE


# 向后兼容
is_holiday = lambda dt=None: not is_trading_day(dt) if dt is not None else not is_trading_day()
is_workday_override = lambda dt=None: False  # akshare已处理调休


if __name__ == "__main__":
    print("=" * 50)
    print("  A股交易日历（动态版）")
    print("=" * 50)

    source = get_calendar_source()
    print(f"\n数据源: {source}")

    today = datetime.now().date()
    print(f"今天 {today}: {'交易日' if is_trading_day(today) else '非交易日'}")

    next_d = next_trading_day(today)
    print(f"下一交易日: {next_d}")

    # 测试节假日
    test_dates = [
        date(2026, 1, 1),   # 元旦
        date(2026, 1, 29),  # 春节
        date(2026, 5, 1),   # 劳动节
        date(2026, 10, 1),  # 国庆
        date(2026, 6, 24),  # 普通周三
    ]
    for d in test_dates:
        print(f"  {d}: {'交易日' if is_trading_day(d) else '非交易日'}")
