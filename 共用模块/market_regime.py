#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
市场环境检测模块 — 基于北向资金+大盘动量判断当前市场状态
============================================================
输出："bull" | "bear" | "sideways"，供选股引擎做权重切换。

数据源：
  - 同花顺北向资金净流入（hsgt_realtime）
  - 沪深300涨跌幅（akshare 或 sina）
  - 20日波动率（历史K线）
"""

import numpy as np
import json
from pathlib import Path
from datetime import datetime, timedelta

def log(msg, level="INFO"):
    """简易日志函数（防止异常路径调用未定义的log导致NameError崩溃）"""
    from datetime import datetime as _dt
    print(f"[{_dt.now().strftime('%H:%M:%S')}] [{level}] {msg}")


try:
    import akshare as ak
    HAS_AK = True
except ImportError:
    HAS_AK = False

_BASE = Path(__file__).parent.parent
_FACTOR_WEIGHTS_FILE = _BASE / "factor_weights.json"

# 三套权重配置：与 stock_picker.py 因子列名保持一致（硬编码作为 fallback）
_DEFAULT_REGIME_WEIGHTS = {
    "bull": {
        "动量_涨跌幅": 0.10, "动量_5日涨幅": 0.08,
        "趋势_均线位置": 0.08, "趋势_RSI强度": 0.05,
        "量能_换手率": 0.05, "量能_量比": 0.04,
        "估值_PE反向": 0.04, "估值_PB反向": 0.02,
        "规模_小市值": 0.02, "技术_MACD金叉": 0.08,
        "概念_热度": 0.06, "质量_盈利": 0.02,
        "成长_净利润同比": 0.08, "成长_营收同比": 0.05,
        "机构_北向资金": 0.04, "机构_基金持仓": 0.03,
        "研报_覆盖度": 0.02, "景气_行业": 0.02,
        "情绪_市场": 0.03, "情绪_板块": 0.03,
        "LLM_信号": 0.02, "ai_score": 0.04,
    },
    "bear": {
        "动量_涨跌幅": 0.06, "动量_5日涨幅": 0.05,
        "趋势_均线位置": 0.06, "趋势_RSI强度": 0.04,
        "量能_换手率": 0.04, "量能_量比": 0.03,
        "估值_PE反向": 0.10, "估值_PB反向": 0.05,
        "规模_小市值": 0.02, "技术_MACD金叉": 0.06,
        "概念_热度": 0.03, "质量_盈利": 0.06,
        "成长_净利润同比": 0.04, "成长_营收同比": 0.03,
        "机构_北向资金": 0.04, "机构_基金持仓": 0.04,
        "研报_覆盖度": 0.04, "景气_行业": 0.02,
        "情绪_市场": 0.02, "情绪_板块": 0.02,
        "LLM_信号": 0.02, "ai_score": 0.03,
    },
    "sideways": {
        "动量_涨跌幅": 0.09, "动量_5日涨幅": 0.07,
        "趋势_均线位置": 0.07, "趋势_RSI强度": 0.05,
        "量能_换手率": 0.05, "量能_量比": 0.04,
        "估值_PE反向": 0.07, "估值_PB反向": 0.03,
        "规模_小市值": 0.02, "技术_MACD金叉": 0.08,
        "概念_热度": 0.05, "质量_盈利": 0.03,
        "成长_净利润同比": 0.07, "成长_营收同比": 0.05,
        "机构_北向资金": 0.04, "机构_基金持仓": 0.04,
        "研报_覆盖度": 0.03, "景气_行业": 0.02,
        "情绪_市场": 0.02, "情绪_板块": 0.02,
        "LLM_信号": 0.02, "ai_score": 0.04,
    },
}


def _load_regime_weights():
    if _FACTOR_WEIGHTS_FILE.exists():
        try:
            data = json.loads(_FACTOR_WEIGHTS_FILE.read_text(encoding="utf-8"))
            rw = data.get("regime_weights")
            if rw and "bull" in rw:
                return rw
        except Exception as e:
            log(f"操作异常: {e}", "WARN")
    return _DEFAULT_REGIME_WEIGHTS


def get_regime_weights(regime: str):
    weights = _load_regime_weights()
    return weights.get(regime, weights["sideways"])


def _fetch_hs300_change():
    if HAS_AK:
        try:
            df = ak.index_zh_a_hist(symbol="000300", period="daily", start_date=(datetime.now() - timedelta(days=7)).strftime("%Y%m%d"), end_date=datetime.now().strftime("%Y%m%d"), adjust="")
            if df is not None and len(df) >= 2:
                return float(df.iloc[-1]["涨跌幅"])
        except Exception:
            pass
    try:
        import requests
        url = "https://hq.sinajs.cn/list=sh000300"
        r = requests.get(url, headers={"Referer": "https://finance.sina.com.cn"}, timeout=10)
        r.encoding = "gb2312"
        data = r.text.split("=")[1].strip('";').split(",")
        if len(data) > 3:
            return round((float(data[3]) - float(data[2])) / float(data[2]) * 100, 2)
    except Exception:
        pass
    return 0.0


def _fetch_northbound_net():
    try:
        from stock_picker import hsgt_realtime
        data = hsgt_realtime()
        if data:
            return data.get("净流入", 0)
    except Exception:
        pass
    return 0.0


def _calc_volatility():
    if not HAS_AK:
        return None
    try:
        end = datetime.now().strftime("%Y%m%d")
        start = (datetime.now() - timedelta(days=40)).strftime("%Y%m%d")
        df = ak.index_zh_a_hist(symbol="000300", period="daily", start_date=start, end_date=end, adjust="")
        if df is not None and len(df) >= 20:
            returns = df["涨跌幅"].tail(20).astype(float)
            return float(returns.std())
    except Exception:
        pass
    return None


def detect_market_regime():
    hs300 = _fetch_hs300_change()
    north = _fetch_northbound_net()
    vol = _calc_volatility()

    score = 0
    if hs300 > 1.5:
        score += 2
    elif hs300 > 0.5:
        score += 1
    elif hs300 < -1.5:
        score -= 2
    elif hs300 < -0.5:
        score -= 1

    if north > 50:
        score += 2
    elif north > 20:
        score += 1
    elif north < -50:
        score -= 2
    elif north < -20:
        score -= 1

    if vol is not None:
        if vol > 1.5:
            score = 0

    if score >= 2:
        return "bull"
    elif score <= -2:
        return "bear"
    return "sideways"


def get_market_regime_summary():
    regime = detect_market_regime()
    hs300 = _fetch_hs300_change()
    north = _fetch_northbound_net()
    return {
        "regime": regime,
        "hs300_change": hs300,
        "northbound_net": north,
        "weights": get_regime_weights(regime),
    }


if __name__ == "__main__":
    print("=" * 50)
    print("  市场环境检测")
    print("=" * 50)
    summary = get_market_regime_summary()
    print(f"\n市场环境: {summary['regime']}")
    print(f"沪深300涨跌: {summary['hs300_change']:+.2f}%")
    print(f"北向净流入: {summary['northbound_net']:.2f}亿")
    print(f"\n权重预览 (前5):")
    weights = summary['weights']
    for k, v in sorted(weights.items(), key=lambda x: -x[1])[:5]:
        print(f"  {k}: {v:.4f}")
