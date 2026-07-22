#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
LLM因子挖掘模块 — 用大模型从新闻中提取选股信号
====================================================
定期扫描热门板块/个股新闻，让LLM提取：
  - 政策方向信号（利好/利空）
  - 资金流向信号（主力流入/流出）
  - 板块轮动信号（轮入/轮出）
  - 催化剂信号（事件驱动）

输出结构化因子信号，纳入量化打分。
"""
import json
import numpy as np
from pathlib import Path
from datetime import datetime

def log(msg, level="INFO"):
    """简易日志函数（防止异常路径调用未定义的log导致NameError崩溃）"""
    from datetime import datetime as _dt
    print(f"[{_dt.now().strftime('%H:%M:%S')}] [{level}] {msg}")


BASE_DIR = Path(__file__).parent.parent  # 指向项目根目录
CACHE_DIR = BASE_DIR / "cache"
SIGNALS_FILE = CACHE_DIR / "llm_factor_signals.json"

# LLM分析的热门概念（从config或内置）
SCAN_CONCEPTS = [
    "人工智能", "AI", "芯片", "半导体", "算力", "机器人",
    "低空经济", "新能源", "光伏", "锂电池", "医药",
    "消费", "白酒", "银行", "券商", "地产",
]


def _load_ai_config():
    """加载AI配置"""
    config_path = BASE_DIR / "config.json"
    if config_path.exists():
        try:
            cfg = json.loads(config_path.read_text(encoding="utf-8-sig"))
            return cfg.get("ai_analysis", {})
        except Exception as e:
            log(f"操作异常: {e}", "WARN")
    return {}


def _call_llm(messages, cfg):
    """调用LLM（复用stock_ai_analysis的接口）"""
    try:
        from stock_ai_analysis import call_ai
        return call_ai(messages, cfg)
    except Exception as e:
        log(f"操作异常: {e}", "WARN")
        return None


def scan_market_patterns(concepts=None):
    """
    扫描市场模式，提取因子信号。

    Args:
        concepts: 要扫描的概念列表（None=使用默认）

    Returns:
        dict: {
            "scanned_at": "2026-06-24T16:00:00",
            "signals": {
                "政策方向": {"利好": ["AI", "芯片"], "利空": ["地产"]},
                "资金流向": {"流入": ["券商", "银行"], "流出": ["白酒"]},
                "板块轮动": {"轮入": ["新能源"], "轮出": ["半导体"]},
                "催化剂": [{"概念": "AI", "事件": "xxx", "强度": "强"}],
            },
            "scores": {"AI": 0.8, "芯片": 0.7, ...}
        }
    """
    if concepts is None:
        concepts = SCAN_CONCEPTS

    cfg = _load_ai_config()
    if not cfg.get("enabled"):
        return None

    # 构建prompt
    concept_str = "、".join(concepts[:10])
    prompt = f"""你是A股量化分析师。请分析当前A股市场中以下板块的最新动态：

板块：{concept_str}

请从以下维度分析，输出JSON格式：
{{
    "政策方向": {{"利好": ["板块名"], "利空": ["板块名"]}},
    "资金流向": {{"流入": ["板块名"], "流出": ["板块名"]}},
    "板块轮动": {{"轮入": ["板块名"], "轮出": ["板块名"]}},
    "催化剂": [{{"概念": "板块名", "事件": "简述", "强度": "强/中/弱"}}],
    "板块评分": {{"板块名": 0.0-1.0}}
}}

注意：
1. 基于最近一周的市场信息判断
2. 评分越高表示短期越看好
3. 只输出JSON，不要其他文字"""

    messages = [{"role": "user", "content": prompt}]
    result = _call_llm(messages, cfg)

    if result is None:
        return None

    # 解析JSON
    try:
        # 尝试提取JSON
        if "```json" in result:
            result = result.split("```json")[1].split("```")[0]
        elif "```" in result:
            result = result.split("```")[1].split("```")[0]

        signals = json.loads(result.strip())
    except Exception as e:
        log(f"操作异常: {e}", "WARN")
        return None

    # 构建返回
    output = {
        "scanned_at": datetime.now().isoformat(),
        "signals": signals,
        "scores": signals.get("板块评分", {}),
    }

    # 保存
    try:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        SIGNALS_FILE.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as e:
        log(f"操作异常: {e}", "WARN")

    return output


def load_signals():
    """加载已保存的因子信号"""
    if not SIGNALS_FILE.exists():
        return None
    try:
        data = json.loads(SIGNALS_FILE.read_text(encoding="utf-8"))
        # 检查是否过期（超过24小时）
        scanned_at = datetime.fromisoformat(data["scanned_at"])
        if (datetime.now() - scanned_at).total_seconds() > 86400:
            return None
        return data
    except Exception as e:
        log(f"操作异常: {e}", "WARN")
        return None


def compute_llm_factor(df, concept_map=None):
    """
    基于LLM信号计算因子。

    Args:
        df: 股票DataFrame
        concept_map: 概念映射dict

    Returns:
        df: 新增 LLM_信号 列（0-1）
    """
    signals = load_signals()
    if signals is None or "scores" not in signals:
        df["LLM_信号"] = 0.5
        return df

    scores = signals["scores"]
    if not scores:
        df["LLM_信号"] = 0.5
        return df

    llm_scores = []
    for _, row in df.iterrows():
        code = str(row.get("代码", ""))
        concepts = concept_map.get(code, []) if concept_map else []
        if isinstance(concepts, str):
            concepts = [concepts]

        # 匹配概念得分
        matched_scores = []
        for c in concepts:
            if c in scores:
                matched_scores.append(scores[c])

        if matched_scores:
            llm_scores.append(float(np.mean(matched_scores)))
        else:
            llm_scores.append(0.5)

    df["LLM_信号"] = llm_scores
    return df


if __name__ == "__main__":
    print("=" * 50)
    print("  LLM因子挖掘模块")
    print("=" * 50)

    # 检查现有信号
    signals = load_signals()
    if signals:
        print(f"\n现有信号 (扫描于 {signals['scanned_at']}):")
        for k, v in signals.get("scores", {}).items():
            print(f"  {k}: {v}")
    else:
        print("\n无现有信号")

    # 尝试扫描
    print("\n尝试扫描市场模式...")
    result = scan_market_patterns()
    if result:
        print(f"\n扫描完成:")
        for k, v in result.get("scores", {}).items():
            print(f"  {k}: {v}")
    else:
        print("扫描失败（AI未配置或网络问题）")
