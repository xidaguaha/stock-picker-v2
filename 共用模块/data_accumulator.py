#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据积累器 v1.0 — 为大规模回测积累原始数据
====================================================
职责：
  1. 每次选股运行后保存完整原始数据（不仅是TopN）
  2. 增量合并历史数据，构建本地数据库
  3. 支持按日期查询原始行情、因子得分、概念映射
  4. 为回测引擎提供更丰富的数据维度

设计原则：
  每条数据都标注来源+抓取时间+是否盘中，绝不做假。
  积累的数据越多，回测越可靠，系统越'聪明'。
"""

import pandas as pd
import numpy as np
import json
import os
import time
from datetime import datetime
from pathlib import Path

# ============================================================
#  路径配置
# ============================================================
BASE_DIR      = Path(__file__).parent.parent  # 指向项目根目录
DATA_DIR      = BASE_DIR / "data"           # 累计数据数据库
RAW_QUOTES    = DATA_DIR / "raw_quotes"      # 原始行情（每次运行完整存档）
FACTOR_SCORES = DATA_DIR / "factor_scores"   # 因子得分（每次运行完整存档）
CONCEPT_MAPS  = DATA_DIR / "concept_maps"    # 概念映射
DAILY_PICKS   = DATA_DIR / "daily_picks"     # 每日TopN选股
INDEX_FILE    = DATA_DIR / "index.json"      # 数据目录索引

for d in [DATA_DIR, RAW_QUOTES, FACTOR_SCORES, CONCEPT_MAPS, DAILY_PICKS]:
    d.mkdir(parents=True, exist_ok=True)


def _load_index():
    """加载数据目录索引"""
    if INDEX_FILE.exists():
        return json.loads(INDEX_FILE.read_text(encoding="utf-8"))
    return {"versions": [], "total_runs": 0, "first_run": None, "last_run": None}


def _save_index(idx):
    """保存数据目录索引"""
    INDEX_FILE.write_text(json.dumps(idx, ensure_ascii=False, indent=2), encoding="utf-8")


def _now_ts():
    """精确到毫秒的时间戳"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def accumulate_raw_quotes(df_quotes, meta):
    """
    保存本次运行的完整原始行情数据。

    Args:
        df_quotes: 全量行情 DataFrame
        meta: 元数据 dict (版本/数据源/盘中状态/时间等)
    """
    if df_quotes is None or len(df_quotes) == 0:
        return

    now = datetime.now()
    ts = now.strftime("%Y%m%d_%H%M%S")

    # 保存完整行情 (parquet 格式，压缩且快速)
    quote_path = RAW_QUOTES / f"quotes_{ts}.parquet"
    df_quotes.to_parquet(quote_path, index=False)

    # 保存元数据
    meta_path = RAW_QUOTES / f"quotes_{ts}_meta.json"
    full_meta = {
        "保存时间": _now_ts(),
        "行情记录数": len(df_quotes),
        "盘中运行": meta.get("in_session", False),
        "数据源": meta.get("data_source", "未知"),
        "因子权重": meta.get("weights", {}),
        "热门概念": meta.get("hot_concepts", []),
    }
    meta_path.write_text(json.dumps(full_meta, ensure_ascii=False, indent=2), encoding="utf-8")

    # 更新索引
    idx = _load_index()
    idx["total_runs"] += 1
    idx["last_run"] = _now_ts()
    if idx["first_run"] is None:
        idx["first_run"] = _now_ts()
    _save_index(idx)

    print(f"  [数据积累] 原始行情: {quote_path.name} ({len(df_quotes)} 只)")


def accumulate_factor_scores(df_scored, meta):
    """
    保存本次运行的完整因子得分数据。

    这是回测IC分析和因子调优的核心数据源。
    """
    if df_scored is None or len(df_scored) == 0:
        return

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    # 提取因子相关列
    factor_cols = [c for c in df_scored.columns
                   if any(k in c for k in [
                       "代码", "名称", "综合得分", "排名",
                       "动量_", "趋势_", "量能_", "估值_", "规模_", "技术_", "概念_",
                       "涨跌幅", "换手率", "量比", "总市值", "市盈率动态", "行业",
                   ])]

    df_factors = df_scored[factor_cols].copy()

    # 添加元数据列
    df_factors["快照日期"] = datetime.now().strftime("%Y-%m-%d")
    df_factors["盘中运行"] = meta.get("in_session", False)
    df_factors["数据源"] = meta.get("data_source", "未知")

    path = FACTOR_SCORES / f"factors_{ts}.parquet"
    df_factors.to_parquet(path, index=False)

    print(f"  [数据积累] 因子得分: {path.name} ({len(df_factors)} 只, {len(factor_cols)-2} 个因子)")


def accumulate_concept_map(concept_map, meta):
    """保存概念映射数据"""
    if not concept_map:
        return

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    path = CONCEPT_MAPS / f"concepts_{ts}.json"
    data = {
        "保存时间": _now_ts(),
        "覆盖股票": len(concept_map),
        "数据源": meta.get("concept_source", "未知"),
        "盘中运行": meta.get("in_session", False),
        "概念映射": concept_map,
    }
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"  [数据积累] 概念映射: {path.name} ({len(concept_map)} 只)")


def accumulate_daily_picks(df_top_n, meta):
    """
    保存每日TopN选股结果（用于跨日对比和趋势分析）。
    增量追加到同一个 CSV，方便横向对比。
    """
    if df_top_n is None or len(df_top_n) == 0:
        return

    today = datetime.now().strftime("%Y-%m-%d")

    # 单日文件
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    daily_path = DAILY_PICKS / f"picks_{ts}.csv"

    save_cols = ["排名", "代码", "名称", "综合得分", "涨跌幅", "换手率", "量比",
                  "总市值", "市盈率动态", "行业"]
    available = [c for c in save_cols if c in df_top_n.columns]

    df_save = df_top_n[available].copy()
    df_save["选股日期"] = today
    df_save["盘中运行"] = meta.get("in_session", False)
    df_save["数据源"] = meta.get("data_source", "未知")

    df_save.to_csv(daily_path, index=False, encoding="utf-8-sig")

    # 追加到汇总文件
    summary_path = DAILY_PICKS / "_all_picks_summary.csv"
    if summary_path.exists():
        existing = pd.read_csv(summary_path, encoding="utf-8-sig")
        combined = pd.concat([existing, df_save], ignore_index=True)
        combined.to_csv(summary_path, index=False, encoding="utf-8-sig")
    else:
        df_save.to_csv(summary_path, index=False, encoding="utf-8-sig")


def get_accumulated_stats():
    """获取数据积累统计"""
    idx = _load_index()

    stats = {
        "总运行次数": idx.get("total_runs", 0),
        "首次运行": idx.get("first_run", "从未运行"),
        "最近运行": idx.get("last_run", "从未运行"),
    }

    # 统计各目录文件数
    for name, path in [("原始行情", RAW_QUOTES), ("因子得分", FACTOR_SCORES),
                        ("概念映射", CONCEPT_MAPS), ("每日选股", DAILY_PICKS)]:
        if path.exists():
            stats[name] = len(list(path.glob("*")))
        else:
            stats[name] = 0

    return stats


def build_historical_dataset():
    """
    合并所有历史因子得分数据，输出一个可用于
    IC分析和权重优化的完整数据集。
    """
    all_factors = []
    for f in sorted(FACTOR_SCORES.glob("factors_*.parquet")):
        try:
            df = pd.read_parquet(f)
            all_factors.append(df)
        except Exception:
            continue

    if not all_factors:
        return None

    combined = pd.concat(all_factors, ignore_index=True)
    combined = combined.drop_duplicates(subset=["快照日期", "代码"], keep="last")

    return combined


def format_accumulation_report():
    """格式化数据积累报告"""
    stats = get_accumulated_stats()

    lines = []
    lines.append("")
    lines.append("╔" + "═" * 60 + "╗")
    lines.append("║  📦 数据积累状态")
    lines.append("╠" + "═" * 60 + "╣")
    lines.append(f"║  累计运行: {stats['总运行次数']:>4d} 次")
    lines.append(f"║  首次运行: {stats['首次运行']}")
    lines.append(f"║  最近运行: {stats['最近运行']}")
    lines.append("╟" + "─" * 60 + "╢")
    lines.append(f"║  原始行情: {stats['原始行情']:>4d} 份")
    lines.append(f"║  因子得分: {stats['因子得分']:>4d} 份")
    lines.append(f"║  概念映射: {stats['概念映射']:>4d} 份")
    lines.append(f"║  每日选股: {stats['每日选股']:>4d} 份")
    lines.append("╚" + "═" * 60 + "╝")

    # 判断数据充足度
    total = stats["总运行次数"]
    if total < 10:
        lines.append("  ⚠ 数据积累不足（<10次），回测结果不可靠")
    elif total < 30:
        lines.append("  ℹ 数据积累中（10~30次），可进行初步回测")
    elif total < 100:
        lines.append("  ✓ 数据积累充足（30~100次），回测结果可信")
    else:
        lines.append("  ★ 数据积累丰富（>100次），可进行深度优化")

    return "\n".join(lines)


if __name__ == "__main__":
    print(format_accumulation_report())
