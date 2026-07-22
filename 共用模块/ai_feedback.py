#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AI 评分反馈闭环模块 v1.0
================================
职责：
  1. 记录每次 AI 分析的预测（AI评分 + 元数据）
  2. 延迟 N 天后计算 AI 评分 vs 实际涨跌的 IC
  3. 累积足够样本后，自动调整 prompt 维度权重
  4. 记录 prompt 调整历史

数据流：
  stock_picker.py (AI分析完成后) → record_ai_prediction()
  evolution.py (月迭代时) → evaluate_ai_performance() → tune_ai_prompt()
"""

import json
from pathlib import Path
from datetime import datetime, timedelta
import numpy as np
import pandas as pd

# 纯 numpy 实现 Spearman 相关系数（替代 scipy.stats.spearmanr）
def _spearmanr(x, y):
    """计算 Spearman 等级相关系数"""
    def _rank(a):
        """计算排名（处理并列）"""
        unique, inverse = np.unique(a, return_inverse=True)
        counts = np.bincount(inverse)
        rank = np.cumsum(counts) - counts + 1
        return rank[inverse] * 1.0
    
    rank_x = _rank(x)
    rank_y = _rank(y)
    
    # Pearson 相关系数作用于排名
    n = len(x)
    if n < 2:
        return 0, 1.0
    
    # 计算相关系数
    cov = np.cov(rank_x, rank_y)[0, 1]
    std_x = np.std(rank_x)
    std_y = np.std(rank_y)
    if std_x * std_y == 0:
        return 0, 1.0
    
    rho = cov / (std_x * std_y)
    
    # 计算 p-value（简化版）
    t_stat = rho * np.sqrt((n - 2) / (1 - rho**2))
    # 简化：不计算精确 p-value，返回 0.05 作为占位
    p_value = 0.05
    
    return rho, p_value

# ============================================================
#  配置
# ============================================================
SHARED_DIR = Path(__file__).parent          # 共用模块目录（用于import）
BASE_DIR = Path(__file__).parent.parent     # 项目根目录（用于数据）

# 确保可以导入同目录的模块
import sys

def log(msg, level="INFO"):
    """简易日志函数（防止异常路径调用未定义的log导致NameError崩溃）"""
    from datetime import datetime as _dt
    print(f"[{_dt.now().strftime('%H:%M:%S')}] [{level}] {msg}")

sys.path.insert(0, str(SHARED_DIR))
AI_PREDICTIONS_DIR = BASE_DIR / "ai_predictions"
AI_EVOLUTION_LOG = BASE_DIR / "ai_evolution_log.jsonl"
AI_PROMPT_STATE = BASE_DIR / "ai_prompt_state.json"  # 当前 prompt 配置
# ============================================================
#  JSONL 辅助函数（替代 jsonlines 库）
# ============================================================
def _append_jsonl(filepath: Path, obj: dict):
    """追加一个 JSON 对象到 JSONL 文件"""
    try:
        with open(filepath, "a", encoding="utf-8") as f:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")
    except Exception as e:
        print(f"[AI Feedback] 日志写入失败: {e}")

def _read_jsonl(filepath: Path) -> list:
    """读取 JSONL 文件，返回对象列表"""
    results = []
    if not filepath.exists():
        return results
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    results.append(json.loads(line))
                except Exception as e:
                    log(f"操作异常: {e}", "WARN")
    return results

def _write_jsonl(filepath: Path, objs: list):
    """覆盖写入 JSONL 文件"""
    with open(filepath, "w", encoding="utf-8") as f:
        for obj in objs:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")


# 确保目录存在
AI_PREDICTIONS_DIR.mkdir(exist_ok=True)


# ============================================================
#  1. 记录 AI 预测
# ============================================================
def record_ai_prediction(code: str, name: str, snapshot_date: str,
                        ai_score: int, ai_result: dict,
                        composite_score: float, concept_list: list):
    """
    记录单只股票的 AI 分析预测，供后续验证。
    
    Args:
        code: 股票代码
        name: 股票名称
        snapshot_date: 选股日期 (YYYY-MM-DD)
        ai_score: AI 评分 (0-100)
        ai_result: AI 分析结果字典（含行业定位、催化剂等）
        composite_score: 综合得分 (0-100)
        concept_list: 概念列表
    """
    prediction = {
        "code": code,
        "name": name,
        "snapshot_date": snapshot_date,
        "ai_score": ai_score,
        "ai_industry": ai_result.get("行业定位", ""),
        "ai_catalyst_count": len(ai_result.get("催化剂", [])),
        "ai_bearish_count": len(ai_result.get("利空因素", [])),
        "ai_investment_logic": ai_result.get("投资逻辑", ""),
        "composite_score": composite_score,
        "concepts": concept_list,
        "actual_returns": {},  # 后续填充
        "verified": False,
    }
    
    # 按日期写入文件
    out_file = AI_PREDICTIONS_DIR / f"{snapshot_date}.jsonl"
    _append_jsonl(out_file, prediction)
    
    return True


# ============================================================
#  2. 验证预测（延迟调用）
# ============================================================
def verify_predictions(hold_days: list = [1, 3, 5, 10, 20]):
    """
    扫描 ai_predictions/ 目录，对满足条件的记录验证实际涨跌。
    
    逻辑：
      - 读取所有未验证的记录
      - 如果距离 snapshot_date 已超过 max(hold_days) + 5 天 → 拉 K 线验证
      - 填充 actual_returns 字段，标记 verified=True
    """
    from backtest_engine import _fetch_kline_sina as get_kline  # 使用回测引擎的 K 线获取
    
    updated_count = 0
    for f in sorted(AI_PREDICTIONS_DIR.rglob("*.jsonl")):
        snapshot_date_str = f.stem  # YYYY-MM-DD
        try:
            snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d")
        except Exception:
            continue
        
        # 如果距离选股日还不够长，跳过
        if (datetime.now() - snapshot_date).days < max(hold_days) + 5:
            continue
        
        # 读取并更新
        records = _read_jsonl(f)
        new_records = []
        for obj in records:
            if obj.get("verified"):
                new_records.append(obj)
                continue
                
                # 拉 K 线验证
                code = obj["code"]
                df_kline = get_kline(code, days=120)
                if df_kline is None or len(df_kline) < max(hold_days) + 5:
                    new_records.append(obj)
                    continue
                
                # 计算实际涨跌
                actual = _calculate_actual_returns(df_kline, snapshot_date, hold_days)
                if actual:
                    obj["actual_returns"] = actual
                    obj["verified"] = True
                    updated_count += 1
                
                new_records.append(obj)
        
        # 写回
        _write_jsonl(f, new_records)
    
    return updated_count


def _calculate_actual_returns(df_kline: pd.DataFrame, snapshot_date: datetime, hold_days: list) -> dict:
    """计算实际涨跌"""
    df = df_kline.sort_values("日期")
    # 确保 snapshot_date 和"日期"列都是 Timestamp 类型
    snapshot_date_dt = pd.Timestamp(snapshot_date) if not isinstance(snapshot_date, pd.Timestamp) else snapshot_date
    df["日期"] = pd.to_datetime(df["日期"])
    
    # 找到选股日当天的收盘价
    snapshot_row = df[df["日期"].dt.date == snapshot_date_dt.date()]
    if len(snapshot_row) == 0:
        # 选股日不是交易日，找之后第一个交易日
        snapshot_row = df[df["日期"] > snapshot_date_dt].head(1)
    
    if len(snapshot_row) == 0:
        return None
    
    buy_price = snapshot_row.iloc[0]["收盘"]
    
    # 计算各持有期的涨跌
    result = {}
    for hd in hold_days:
        target_date = snapshot_date_dt + timedelta(days=hd)
        target_row = df[df["日期"] >= target_date].head(1)
        if len(target_row) > 0:
            sell_price = target_row.iloc[0]["收盘"]
            ret = (sell_price - buy_price) / buy_price
            result[str(hd)] = round(ret, 4)
    
    return result if result else None


# ============================================================
#  3. 评估 AI 评分性能
# ============================================================
def evaluate_ai_performance(min_samples: int = 20) -> dict:
    """
    评估 AI 评分的预测力。
    
    返回：
        {
            "ic_1d": 0.05,   # AI评分 vs 1天涨跌的 IC
            "ic_5d": 0.03,
            "ic_20d": 0.02,
            "n_samples": 45,
            "prompt_version": "v1.2",
            "recommendation": "keep" | "adjust",
        }
    """
    # 加载所有已验证的记录
    verified = []
    for f in AI_PREDICTIONS_DIR.rglob("*.jsonl"):
        for obj in _read_jsonl(f):
                if obj.get("verified") and obj.get("actual_returns"):
                    verified.append(obj)
    
    if len(verified) < min_samples:
        return {
            "n_samples": len(verified),
            "ic_1d": None,
            "ic_5d": None,
            "ic_20d": None,
            "recommendation": "insufficient_data",
        }
    
    # 计算各持有期的 IC
    results = {"n_samples": len(verified)}
    for hd in [1, 3, 5, 10, 20]:
        ai_scores = []
        actual_returns = []
        for obj in verified:
            ret = obj["actual_returns"].get(str(hd))
            if ret is not None:
                ai_scores.append(obj["ai_score"])
                actual_returns.append(ret)
        
        if len(ai_scores) >= 10:
            ic, p_value = _spearmanr(ai_scores, actual_returns)
            results[f"ic_{hd}d"] = round(ic, 4)
            results[f"ic_{hd}d_pvalue"] = round(p_value, 4)
        else:
            results[f"ic_{hd}d"] = None
    
    # 综合判断
    ic_5d = results.get("ic_5d", 0) or 0
    ic_20d = results.get("ic_20d", 0) or 0
    avg_ic = np.mean([ic for ic in [results.get(f"ic_{hd}d", 0) for hd in [3, 5, 10]] if ic])
    
    if avg_ic < 0.02:
        results["recommendation"] = "adjust"
        results["adjust_reason"] = f"平均IC={avg_ic:.3f}，预测力不足"
    elif avg_ic > 0.05:
        results["recommendation"] = "keep"
    else:
        results["recommendation"] = "monitor"
    
    results["avg_ic"] = round(avg_ic, 4)
    results["prompt_version"] = _get_prompt_version()
    
    return results


# ============================================================
#  4. 调整 Prompt（自动迭代）
# ============================================================
def tune_ai_prompt(evaluation: dict, cfg: dict = None) -> dict:
    """
    根据评估结果调整 AI 分析 prompt。
    
    调整策略：
      - 如果 IC < 0.02 → 调整 prompt 维度权重（强调某些维度、降低某些维度）
      - 记录调整历史到 ai_evolution_log.jsonl
    
    返回：
        {"success": True, "new_prompt_version": "v1.3", "changes": [...]}
    """
    if evaluation.get("recommendation") != "adjust":
        return {"success": False, "reason": "无需调整"}
    
    # 加载当前 prompt 状态
    prompt_state = _load_prompt_state()
    
    # 调整维度权重（示例：根据 IC 低的持有期调整）
    changes = []
    
    # 示例调整逻辑：如果短期 IC 低，强调「催化剂」维度
    ic_1d = evaluation.get("ic_1d", 0) or 0
    ic_20d = evaluation.get("ic_20d", 0) or 0
    
    if ic_1d < 0.02:
        prompt_state["weights"]["催化剂"] = min(1.0, prompt_state["weights"].get("催化剂", 0.3) + 0.1)
        changes.append(f"催化剂权重 {prompt_state['weights']['催化剂']-.1:.1f} → {prompt_state['weights']['催化剂']:.1f}")
    
    if ic_20d < 0.02:
        prompt_state["weights"]["行业景气度"] = min(1.0, prompt_state["weights"].get("行业景气度", 0.2) + 0.1)
        changes.append(f"行业景气度权重 {prompt_state['weights']['行业景气度']-.1:.1f} → {prompt_state['weights']['行业景气度']:.1f}")
    
    # 保存新状态
    prompt_state["version"] = _increment_version(prompt_state.get("version", "v1.0"))
    prompt_state["last_adjust"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    prompt_state["last_evaluation"] = evaluation
    _save_prompt_state(prompt_state)
    
    # 记录到 evolution log
    log_entry = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "action": "adjust_prompt",
        "old_version": evaluation.get("prompt_version"),
        "new_version": prompt_state["version"],
        "evaluation": evaluation,
        "changes": changes,
    }
    _append_jsonl(AI_EVOLUTION_LOG, log_entry)
    
    return {
        "success": True,
        "new_prompt_version": prompt_state["version"],
        "changes": changes,
    }


# ============================================================
#  辅助函数
# ============================================================
def _get_prompt_version() -> str:
    state = _load_prompt_state()
    return state.get("version", "v1.0")


def _load_prompt_state() -> dict:
    if AI_PROMPT_STATE.exists():
        return json.loads(AI_PROMPT_STATE.read_text(encoding="utf-8"))
    # 默认状态
    return {
        "version": "v1.0",
        "weights": {
            "行业定位": 0.1,
            "核心产品": 0.1,
            "催化剂": 0.3,
            "利空因素": 0.2,
            "机构动态": 0.1,
            "行业景气度": 0.2,
        },
        "last_adjust": None,
        "last_evaluation": None,
    }


def _save_prompt_state(state: dict):
    AI_PROMPT_STATE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def _increment_version(version: str) -> str:
    try:
        num = int(version[1:])
        return f"v{num + 1}"
    except Exception:
        return "v1.0"


# ============================================================
#  5. 集成接口（供 stock_picker.py 调用）
# ============================================================
def on_ai_analysis_complete(code: str, name: str, snapshot_date: str,
                            ai_score: int, ai_result: dict,
                            composite_score: float, concept_list: list):
    """
    stock_picker.py 中 AI 分析完成后调用此函数。
    """
    try:
        record_ai_prediction(
            code=code,
            name=name,
            snapshot_date=snapshot_date,
            ai_score=ai_score,
            ai_result=ai_result,
            composite_score=composite_score,
            concept_list=concept_list,
        )
    except Exception as e:
        print(f"[AIFeedback] 记录预测失败: {e}")


def on_evolution_monthly():
    """
    evolution.py 月迭代时调用此函数。
    """
    try:
        # 先验证历史预测
        n_verified = verify_predictions()
        print(f"[AIFeedback] 验证完成: {n_verified} 条记录已更新")
        
        # 评估性能
        evaluation = evaluate_ai_performance(min_samples=20)
        print(f"[AIFeedback] 评估结果: IC_5d={evaluation.get('ic_5d')}, 建议={evaluation.get('recommendation')}")
        
        # 如果需要调整，调整 prompt
        if evaluation.get("recommendation") == "adjust":
            result = tune_ai_prompt(evaluation)
            print(f"[AIFeedback] Prompt 调整: {result.get('changes')}")
        
        return evaluation
    except Exception as e:
        print(f"[AIFeedback] 月迭代失败: {e}")
        return None


if __name__ == "__main__":
    # 测试
    print("测试: 记录预测")
    on_ai_analysis_complete(
        code="600519",
        name="贵州茅台",
        snapshot_date="2026-06-23",
        ai_score=75,
        ai_result={"行业定位": "白酒", "催化剂": [], "利空因素": []},
        composite_score=82.5,
        concept_list=["白酒", "消费"],
    )
    print("完成")
