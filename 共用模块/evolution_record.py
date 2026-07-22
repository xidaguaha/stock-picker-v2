"""
evolution_record.py — 迭代变更记录模块

每次因子权重调整、因子淘汰/激活，都写入结构化 JSON 记录，
方便后续回溯：为什么调、调了什么、调完效果如何。

记录目录：evolution_history/
每条记录：{timestamp}_{tier}.json
"""

import json
import os
from datetime import datetime
from pathlib import Path

def log(msg, level="INFO"):
    """简易日志函数（防止异常路径调用未定义的log导致NameError崩溃）"""
    from datetime import datetime as _dt
    print(f"[{_dt.now().strftime('%H:%M:%S')}] [{level}] {msg}")


BASE_DIR = Path(__file__).parent.parent  # 指向项目根目录
RECORD_DIR = BASE_DIR / "evolution_history"
RECORD_DIR.mkdir(exist_ok=True)


def record_weight_change(tier, before_weights, after_weights, reason, backtest_summary=None):
    """
    记录一次权重调整。

    Args:
        tier: "daily" | "weekly" | "monthly"
        before_weights: dict 调整前的权重
        after_weights: dict 调整后的权重
        reason: str 调整原因（如 "IC分析显示量能因子失效"）
        backtest_summary: dict 可选，调整前的回测表现（用于对比）
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    record = {
        "timestamp": timestamp,
        "tier": tier,
        "reason": reason,
        "before": before_weights,
        "after": after_weights,
        "changes": {
            k: round(after_weights.get(k, 0) - before_weights.get(k, 0), 4)
            for k in set(list(before_weights.keys()) + list(after_weights.keys()))
            if abs(after_weights.get(k, 0) - before_weights.get(k, 0)) > 0.001
        },
        "backtest_before": backtest_summary,
        "config_snapshot": _load_config_snapshot()
    }

    filename = f"{timestamp}_{tier}.json"
    filepath = RECORD_DIR / filename
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(record, f, ensure_ascii=False, indent=2)

    # 同时追加到变更日志（人类可读）
    log_path = RECORD_DIR / "changelog.txt"
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"\n[{timestamp}] [{tier.upper()}]\n")
        f.write(f"  原因: {reason}\n")
        for k, v in record["changes"].items():
            direction = "+" if v > 0 else ""
            f.write(f"  {k}: {before_weights.get(k, 0):.3f} → {after_weights.get(k, 0):.3f} ({direction}{v:.3f})\n")
        f.write("-" * 60 + "\n")

    return str(filepath)


def record_factor_elimination(tier, eliminated_factors, before_weights, after_weights, reason):
    """记录因子淘汰"""
    return record_weight_change(
        tier=tier,
        before_weights=before_weights,
        after_weights=after_weights,
        reason=f"淘汰因子: {', '.join(eliminated_factors)}。{reason}"
    )


def get_evolution_history(limit=20):
    """读取最近的迭代历史"""
    files = sorted(RECORD_DIR.glob("*.json"), reverse=True)
    history = []
    for f in files[:limit]:
        try:
            with open(f, encoding="utf-8") as fp:
                history.append(json.load(fp))
        except Exception as e:
            log(f"操作异常: {e}", "WARN")
    return history


def _load_config_snapshot():
    """读取当前 config.json 的快照（不含隐私字段）"""
    config_path = BASE_DIR / "config.json"
    if not config_path.exists():
        return {}
    with open(config_path, encoding="utf-8") as f:
        cfg = json.load(f)
    # 脱敏
    if "feishu_webhook" in cfg:
        cfg["feishu_webhook"] = cfg["feishu_webhook"][:40] + "..."
    if "ai_analysis" in cfg and "api_key" in cfg["ai_analysis"]:
        cfg["ai_analysis"]["api_key"] = "***"
    return cfg


if __name__ == "__main__":
    # 测试
    before = {"momentum": 0.18, "volume": 0.11, "technical": 0.22}
    after  = {"momentum": 0.20, "volume": 0.09, "technical": 0.22}
    path = record_weight_change("weekly", before, after, "IC分析：动量因子IC上升，量能因子IC下降")
    print(f"记录已写入: {path}")
    print("最近历史:")
    for h in get_evolution_history(3):
        print(f"  {h['timestamp']} [{h['tier']}] {h['reason'][:50]}")
