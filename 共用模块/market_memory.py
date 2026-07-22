#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
市场情景记忆模块 — 借鉴 LivingMemory 的记忆生命周期思想
==========================================================
功能：
  1. 每日记录市场情景标签 + Top5表现 → 形成历史经验库
  2. 情景召回：给定当前市场标签，返回历史相似日的Top5表现作为参考
  3. 每周LLM策略总结：把本周回测数据喂给LLM生成可读总结

设计原则：
  - 独立模块，不侵入现有选股/回测/调优流程
  - 数据存储在 data/market_memory/ 目录
  - 自动遗忘：超过90天的记录自动清理（重要性衰减）
  - 异常安全：任何失败不影响主流程

存储格式：data/market_memory/scenarios.jsonl （每行一个JSON）
"""

import json
import os
import sys
from pathlib import Path
from datetime import datetime, timedelta

BASE_DIR = Path(__file__).parent.parent
MEMORY_DIR = BASE_DIR / "data" / "market_memory"
MEMORY_DIR.mkdir(parents=True, exist_ok=True)

SCENARIO_FILE = MEMORY_DIR / "scenarios.jsonl"
WEEKLY_SUMMARY_DIR = MEMORY_DIR / "weekly_summaries"
WEEKLY_SUMMARY_DIR.mkdir(parents=True, exist_ok=True)

MAX_AGE_DAYS = 90  # 超过90天的记录自动清理


def _safe_log(msg, level="INFO"):
    """安全日志输出（不依赖bug_log，避免循环导入）"""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] [{level}] {msg}")


# ============================================================
#  1. 每日情景记录
# ============================================================

def record_daily_scenario(df_scored, market_regime="unknown",
                          northbound_dir="无数据", top_n=5):
    """
    记录当日市场情景 + TopN表现。

    在 evolution.py 的日监控步骤后调用。
    数据来源全部是已有变量，不额外拉取数据。

    Args:
        df_scored: 当日选股结果 DataFrame（需含 代码/名称/综合得分/行业）
        market_regime: 市场环境 "bull"/"bear"/"sideways"
        northbound_dir: 北向资金方向字符串
        top_n: 记录前N名
    """
    try:
        today = datetime.now().strftime("%Y-%m-%d")

        # 提取TopN
        top_picks = []
        if df_scored is not None and len(df_scored) > 0:
            _df = df_scored.head(top_n)
            for _, row in _df.iterrows():
                top_picks.append({
                    "代码": str(row.get("代码", "")),
                    "名称": str(row.get("名称", "")),
                    "综合得分": round(float(row.get("综合得分", 0)), 2),
                    "行业": str(row.get("行业", "")),
                })

        # 涨跌家数比（从df_scored推算，不额外拉数据）
        up_count = 0
        down_count = 0
        if df_scored is not None and "涨跌幅" in df_scored.columns:
            _chg = df_scored["涨跌幅"].dropna()
            up_count = int((_chg > 0).sum())
            down_count = int((_chg < 0).sum())

        scenario = {
            "日期": today,
            "记录时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "市场环境": market_regime,
            "北向资金": northbound_dir,
            "涨跌家数比": f"{up_count}:{down_count}" if (up_count + down_count) > 0 else "无数据",
            "扫描数": len(df_scored) if df_scored is not None else 0,
            "Top5": top_picks,
            # 后续回测验证后补充实际收益
            "Top5实际收益": None,
            "已验证": False,
        }

        # 追加写入（每行一个JSON）
        with open(SCENARIO_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(scenario, ensure_ascii=False) + "\n")

        _safe_log(f"  [情景记忆] 已记录 {today} 市场情景 (环境={market_regime}, 北向={northbound_dir})")
        return True

    except Exception as e:
        _safe_log(f"  [情景记忆] 记录失败: {e}", "WARN")
        return False


def update_scenario_with_returns(date_str, top5_returns):
    """
    回测验证后，补充Top5的实际收益。

    Args:
        date_str: 日期 "YYYY-MM-DD"
        top5_returns: [{"代码": "xxx", "收益": 0.032}, ...]
    """
    try:
        if not SCENARIO_FILE.exists():
            return False

        lines = SCENARIO_FILE.read_text(encoding="utf-8").strip().split("\n")
        updated = False
        new_lines = []

        for line in lines:
            try:
                scenario = json.loads(line)
                if scenario.get("日期") == date_str and not scenario.get("已验证"):
                    scenario["Top5实际收益"] = top5_returns
                    scenario["已验证"] = True
                    updated = True
                new_lines.append(json.dumps(scenario, ensure_ascii=False))
            except json.JSONDecodeError:
                new_lines.append(line)

        if updated:
            SCENARIO_FILE.write_text("\n".join(new_lines) + "\n", encoding="utf-8")
            _safe_log(f"  [情景记忆] 已补充 {date_str} Top5实际收益")

        return updated

    except Exception as e:
        _safe_log(f"  [情景记忆] 补充收益失败: {e}", "WARN")
        return False


# ============================================================
#  2. 情景召回
# ============================================================

def recall_similar_scenarios(market_regime="unknown", northbound_dir="",
                             top_k=3):
    """
    召回历史相似市场情景。

    匹配规则：
      1. 市场环境相同（bull/bear/sideways）
      2. 北向资金方向相同
      3. 优先返回已验证（有实际收益）的记录

    Args:
        market_regime: 当前市场环境
        northbound_dir: 当前北向资金方向
        top_k: 返回最近K条

    Returns:
        list[dict]: 历史相似情景列表
    """
    try:
        if not SCENARIO_FILE.exists():
            return []

        scenarios = []
        with open(SCENARIO_FILE, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    s = json.loads(line.strip())
                    scenarios.append(s)
                except json.JSONDecodeError:
                    continue

        if not scenarios:
            return []

        # 匹配：市场环境 + 北向资金
        matched = []
        for s in scenarios:
            score = 0
            if s.get("市场环境") == market_regime and market_regime != "unknown":
                score += 2
            if s.get("北向资金") == northbound_dir and northbound_dir:
                score += 1
            if s.get("已验证"):
                score += 1  # 已验证的优先
            if score > 0:
                matched.append((score, s))

        # 按匹配分数降序，同分按日期降序
        matched.sort(key=lambda x: (-x[0], x[1].get("日期", "")), reverse=False)
        matched.sort(key=lambda x: -x[0])

        return [s for _, s in matched[:top_k]]

    except Exception as e:
        _safe_log(f"  [情景记忆] 召回失败: {e}", "WARN")
        return []


def format_scenario_recall(scenarios):
    """格式化情景召回结果为可读文本（用于AI prompt注入）"""
    if not scenarios:
        return ""

    lines = ["【历史相似市场情景参考】"]

    for s in scenarios:
        date = s.get("日期", "?")
        regime = s.get("市场环境", "?")
        nb = s.get("北向资金", "?")
        up_down = s.get("涨跌家数比", "?")
        verified = s.get("已验证", False)

        lines.append(f"  {date} | 环境={regime} 北向={nb} 涨跌比={up_down}")

        # Top5推荐
        top5 = s.get("Top5", [])
        if top5:
            picks = [f"{p.get('名称','?')}({p.get('综合得分',0):.0f}分)" for p in top5[:3]]
            lines.append(f"    当日Top3: {', '.join(picks)}")

        # 实际收益（如果有）
        if verified and s.get("Top5实际收益"):
            rets = s.get("Top5实际收益", [])
            if rets:
                avg_ret = sum(r.get("收益", 0) for r in rets) / len(rets) if rets else 0
                win_rate = sum(1 for r in rets if r.get("收益", 0) > 0) / len(rets) if rets else 0
                lines.append(f"    实际收益: 平均{avg_ret*100:+.1f}% 胜率{win_rate*100:.0f}%")

    return "\n".join(lines)


# ============================================================
#  3. 每周LLM策略总结
# ============================================================

def generate_weekly_summary():
    """
    生成本周策略总结。

    流程：
      1. 收集本周7天的情景记录
      2. 收集本周回测报告
      3. 调用LLM生成可读总结
      4. 存储到 weekly_summaries/

    在 evolution.py 的周迭代步骤中调用。
    """
    try:
        now = datetime.now()
        week_ago = now - timedelta(days=7)

        # 1. 收集本周情景记录
        scenarios = _load_scenarios_in_range(week_ago, now)

        if not scenarios:
            _safe_log("  [情景记忆] 本周无情景记录，跳过总结")
            return None

        # 2. 收集本周回测报告
        backtest_reports = _load_backtest_reports_in_range(week_ago, now)

        # 3. 构造LLM输入
        summary_input = _build_summary_input(scenarios, backtest_reports)

        # 4. 调用LLM
        llm_result = _call_llm_for_summary(summary_input)

        # 5. 存储
        week_str = now.strftime("%Y-W%W")
        summary_file = WEEKLY_SUMMARY_DIR / f"summary_{week_str}.json"
        summary_data = {
            "周次": week_str,
            "生成时间": now.strftime("%Y-%m-%d %H:%M:%S"),
            "覆盖天数": len(scenarios),
            "回测报告数": len(backtest_reports),
            "LLM总结": llm_result,
            "原始数据摘要": {
                "日期范围": [s.get("日期") for s in scenarios],
                "市场环境分布": _count_regimes(scenarios),
                "北向资金分布": _count_northbound(scenarios),
            },
        }

        summary_file.write_text(json.dumps(summary_data, ensure_ascii=False, indent=2),
                                encoding="utf-8")
        _safe_log(f"  [情景记忆] 周总结已生成: {summary_file.name}")

        # 也写入进化日志
        try:
            from evolution import _evo_log
            _evo_log("周总结", f"覆盖{len(scenarios)}天, LLM总结={llm_result[:100] if llm_result else '失败'}")
        except Exception:
            pass

        return summary_data

    except Exception as e:
        _safe_log(f"  [情景记忆] 周总结失败: {e}", "WARN")
        return None


def _load_scenarios_in_range(start_date, end_date):
    """加载日期范围内的情景记录"""
    if not SCENARIO_FILE.exists():
        return []

    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    scenarios = []
    with open(SCENARIO_FILE, "r", encoding="utf-8") as f:
        for line in f:
            try:
                s = json.loads(line.strip())
                d = s.get("日期", "")
                if start_str <= d <= end_str:
                    scenarios.append(s)
            except json.JSONDecodeError:
                continue

    return scenarios


def _load_backtest_reports_in_range(start_date, end_date):
    """加载日期范围内的回测报告"""
    report_dir = BASE_DIR / "reports"
    if not report_dir.exists():
        return []

    start_str = start_date.strftime("%Y%m%d")
    end_str = end_date.strftime("%Y%m%d")

    reports = []
    for f in report_dir.glob("回测报告_*.json"):
        try:
            ts_str = f.stem.replace("回测报告_", "")
            if start_str <= ts_str[:8] <= end_str:
                data = json.loads(f.read_text(encoding="utf-8"))
                reports.append(data)
        except Exception:
            continue

    return reports


def _build_summary_input(scenarios, backtest_reports):
    """构造LLM输入文本"""
    lines = ["请根据以下本周量化选股系统的运行数据，生成一段简洁的策略总结（200字以内）。"]
    lines.append("")
    lines.append("## 本周市场情景")

    for s in scenarios:
        date = s.get("日期", "?")
        regime = s.get("市场环境", "?")
        nb = s.get("北向资金", "?")
        up_down = s.get("涨跌家数比", "?")
        verified = s.get("已验证", False)

        line = f"- {date}: 环境={regime}, 北向={nb}, 涨跌比={up_down}"
        if verified and s.get("Top5实际收益"):
            rets = s.get("Top5实际收益", [])
            if rets:
                avg_ret = sum(r.get("收益", 0) for r in rets) / len(rets)
                line += f", Top5平均收益={avg_ret*100:+.1f}%"
        lines.append(line)

    lines.append("")
    lines.append("## 回测数据")

    for bt in backtest_reports[-3:]:  # 最多3份
        bt_time = bt.get("时间", "?")
        bt_result = bt.get("回测", {})
        if isinstance(bt_result, dict):
            status = bt_result.get("状态", "?")
            lines.append(f"- {bt_time}: 回测状态={status}")

    lines.append("")
    lines.append("请总结：1.本周市场特征 2.策略表现 3.改进建议")

    return "\n".join(lines)


def _call_llm_for_summary(prompt_text):
    """调用LLM生成总结（复用已有的AI分析能力）"""
    try:
        # 尝试用 stock_ai_analysis 中的LLM调用
        sys.path.insert(0, str(Path(__file__).parent))
        from stock_ai_analysis import _get_llm_client

        client = _get_llm_client()
        if client is None:
            return "LLM客户端不可用，跳过AI总结"

        response = client.chat.completions.create(
            model=os.environ.get("AI_MODEL", "deepseek-chat"),
            messages=[
                {"role": "system", "content": "你是量化策略分析师，请简洁专业地总结本周选股系统表现。"},
                {"role": "user", "content": prompt_text},
            ],
            max_tokens=300,
            temperature=0.3,
        )

        return response.choices[0].message.content.strip()

    except ImportError:
        return "AI分析模块不可用，跳过LLM总结"
    except Exception as e:
        return f"LLM总结失败: {e}"


# ============================================================
#  4. 自动遗忘（清理超期记录）
# ============================================================

def cleanup_old_scenarios():
    """清理超过MAX_AGE_DAYS天的情景记录"""
    try:
        if not SCENARIO_FILE.exists():
            return 0

        cutoff = (datetime.now() - timedelta(days=MAX_AGE_DAYS)).strftime("%Y-%m-%d")

        kept = []
        removed = 0
        with open(SCENARIO_FILE, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    s = json.loads(line.strip())
                    if s.get("日期", "") >= cutoff:
                        kept.append(line.strip())
                    else:
                        removed += 1
                except json.JSONDecodeError:
                    continue

        if removed > 0:
            SCENARIO_FILE.write_text("\n".join(kept) + ("\n" if kept else ""),
                                     encoding="utf-8")
            _safe_log(f"  [情景记忆] 清理 {removed} 条过期记录 (>{MAX_AGE_DAYS}天)")

        return removed

    except Exception as e:
        _safe_log(f"  [情景记忆] 清理失败: {e}", "WARN")
        return 0


# ============================================================
#  5. 统计信息
# ============================================================

def get_memory_stats():
    """获取记忆库统计信息"""
    try:
        if not SCENARIO_FILE.exists():
            return {"总数": 0, "已验证": 0, "最早日期": None, "最近日期": None}

        scenarios = []
        with open(SCENARIO_FILE, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    scenarios.append(json.loads(line.strip()))
                except json.JSONDecodeError:
                    continue

        if not scenarios:
            return {"总数": 0, "已验证": 0, "最早日期": None, "最近日期": None}

        verified = sum(1 for s in scenarios if s.get("已验证"))
        dates = [s.get("日期", "") for s in scenarios if s.get("日期")]

        return {
            "总数": len(scenarios),
            "已验证": verified,
            "最早日期": min(dates) if dates else None,
            "最近日期": max(dates) if dates else None,
            "周总结数": len(list(WEEKLY_SUMMARY_DIR.glob("*.json"))),
        }

    except Exception:
        return {"总数": 0, "已验证": 0, "最早日期": None, "最近日期": None}


def _count_regimes(scenarios):
    """统计市场环境分布"""
    counts = {}
    for s in scenarios:
        r = s.get("市场环境", "unknown")
        counts[r] = counts.get(r, 0) + 1
    return counts


def _count_northbound(scenarios):
    """统计北向资金分布"""
    counts = {}
    for s in scenarios:
        nb = s.get("北向资金", "无数据")
        counts[nb] = counts.get(nb, 0) + 1
    return counts
