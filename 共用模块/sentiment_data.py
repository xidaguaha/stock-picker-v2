#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
舆情数据模块 — 基于akshare的市场情绪数据
====================================================
接入akshare获取市场情绪指数和板块热度，作为额外因子纳入打分。

数据源：
  - A股新闻情绪指数（index_news_sentiment_scope）
  - 概念板块热度排名（stock_board_concept_name_em）
"""

import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime

def log(msg, level="INFO"):
    """简易日志函数（防止异常路径调用未定义的log导致NameError崩溃）"""
    from datetime import datetime as _dt
    print(f"[{_dt.now().strftime('%H:%M:%S')}] [{level}] {msg}")


try:
    import akshare as ak
    HAS_AK = True
except ImportError:
    HAS_AK = False

BASE_DIR = Path(__file__).parent.parent  # 指向项目根目录
CACHE_DIR = BASE_DIR / "cache"


def fetch_market_sentiment():
    """
    获取A股新闻情绪指数。

    Returns:
        dict: {
            "score": float (0-100, 情绪分),
            "trend": str ("positive"/"neutral"/"negative"),
            "raw": 原始数据
        }
    """
    if not HAS_AK:
        return None

    try:
        df = ak.index_news_sentiment_scope()
        if df is None or len(df) == 0:
            return None

        # 取最新一条
        latest = df.iloc[-1]
        score = float(latest.get("数值", 50))

        # 归一化到0-100
        if score > 100:
            score = min(score / 10, 100)
        elif score < 0:
            score = max(score, 0)

        # 判断趋势
        if len(df) >= 5:
            recent = df["数值"].tail(5).mean()
            if recent > 60:
                trend = "positive"
            elif recent < 40:
                trend = "negative"
            else:
                trend = "neutral"
        else:
            trend = "neutral"

        return {
            "score": round(score, 2),
            "trend": trend,
            "date": str(latest.get("日期", datetime.now().strftime("%Y-%m-%d"))),
        }
    except Exception as e:
        log(f"操作异常: {e}", "WARN")
        return None


def fetch_hot_concepts(top_n=20):
    """
    获取当日热门概念板块。

    Returns:
        list: [{"name": "人工智能", "change_pct": 2.5, "rank": 1}, ...]
    """
    if not HAS_AK:
        return []

    try:
        df = ak.stock_board_concept_name_em()
        if df is None or len(df) == 0:
            return []

        results = []
        for _, row in df.head(top_n).iterrows():
            name = str(row.get("板块名称", ""))
            change = float(row.get("涨跌幅", 0) or 0)
            results.append({
                "name": name,
                "change_pct": round(change, 2),
            })
        return results
    except Exception:
        return []


def compute_sentiment_factor(df, concept_map=None):
    """
    计算舆情因子，添加到DataFrame。

    Args:
        df: 股票DataFrame（需含 概念_热度 列）
        concept_map: 概念映射dict

    Returns:
        df: 新增 情绪_市场 和 情绪_板块 列（0-1）
    """
    # 市场情绪因子
    sentiment = fetch_market_sentiment()
    if sentiment:
        market_score = sentiment["score"] / 100.0
        df["情绪_市场"] = market_score
    else:
        df["情绪_市场"] = 0.5  # 默认中性

    # 板块热度因子
    hot_concepts = fetch_hot_concepts(top_n=20)
    if hot_concepts and concept_map:
        hot_names = {c["name"]: c["change_pct"] for c in hot_concepts}

        board_scores = []
        for _, row in df.iterrows():
            code = str(row.get("代码", ""))
            concepts = concept_map.get(code, [])
            if isinstance(concepts, str):
                concepts = [concepts]

            # 计算该股票所属概念的平均热度
            heat = 0
            matched = 0
            for c in concepts:
                if c in hot_names:
                    heat += hot_names[c]
                    matched += 1

            if matched > 0:
                score = min(max(heat / matched / 10 + 0.5, 0), 1)
            else:
                score = 0.5

            board_scores.append(score)

        df["情绪_板块"] = board_scores
    else:
        df["情绪_板块"] = 0.5

    return df


if __name__ == "__main__":
    print("=" * 50)
    print("  舆情数据模块")
    print("=" * 50)

    sentiment = fetch_market_sentiment()
    if sentiment:
        print(f"\n市场情绪: {sentiment['score']} ({sentiment['trend']})")
    else:
        print("\n市场情绪: 无法获取")

    hot = fetch_hot_concepts(top_n=5)
    if hot:
        print(f"\n热门概念 Top5:")
        for i, c in enumerate(hot, 1):
            print(f"  {i}. {c['name']}: {c['change_pct']:+.2f}%")
