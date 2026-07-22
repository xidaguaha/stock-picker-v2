#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
进化引擎 v2.0 — 三层迭代调度中枢
===============================================================
这是量化选股系统"越用越聪明"的中枢神经。

每次选股后自动触发，按频率分级执行：

  ┌─────────────────────────────────────────────────────────────┐
  │ 每次运行 → 日监控                                           │
  │   - 数据积累（全量行情+因子得分存档）                        │
  │   - 模拟交易（更新持仓、平仓、开新仓）                       │
  │   - 快速因子IC监控（预警失效因子）                           │
  ├─────────────────────────────────────────────────────────────┤
  │ 每5-7天 → 周迭代 （距上次>=5天 + >=5交易日）                 │
  │   - TopN回测验证 + 全市场IC分析                              │
  │   - 权重微调（EMA α=0.12，小步快跑）                        │
  ├─────────────────────────────────────────────────────────────┤
  │ 每20-30天 → 月迭代（距上次>=20天 + >=15交易日）              │
  │   - 深度IC+ICIR分析 + 市场分环境                             │
  │   - 因子健康度评估 + 淘汰失效因子                            │
  │   - 大权重调整（EMA α=0.30）                                │
  └─────────────────────────────────────────────────────────────┘

关键设计:
  - 日监控只管预警，不动权重
  - 周迭代小步快跑，日积月累
  - 月迭代深度优化，重大纠偏
  - config.json 记录 _last_daily, _last_weekly, _last_monthly
"""

import json
import os
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

# ============================================================
#  日志（既print给控制台看，又写入日志文件）
# ============================================================
try:
    from logger import get_logger
    _evo_logger = get_logger()
    _HAS_EVO_LOGGER = True
except Exception:
    _HAS_EVO_LOGGER = False


def _log(msg: str, level: str = "INFO"):
    """进化引擎日志：print + 写入日志文件"""
    if _HAS_EVO_LOGGER:
        try:
            getattr(_evo_logger, level.lower(), _evo_logger.info)(msg)
        except Exception:
            pass

# ============================================================
#  路径配置
# ============================================================
BASE_DIR     = Path(__file__).parent.parent  # 指向项目根目录
CONFIG_JSON  = BASE_DIR / "config.json"
EVO_LOG      = BASE_DIR / "evolution_log.jsonl"


def _evo_log(action, detail, result="success"):
    """写入进化日志（JSONL + 项目日志系统双写）"""
    entry = {
        "时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "操作": action,
        "详情": str(detail),
        "结果": result,
    }
    with open(EVO_LOG, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    # 同时写入项目日志系统
    _log(f"[进化] {action}: {detail}", "ERROR" if result != "success" else "INFO")


def load_config():
    """加载 config.json"""
    if CONFIG_JSON.exists():
        return json.loads(CONFIG_JSON.read_text(encoding="utf-8"))
    return {}


def save_config_field(key, value):
    """更新 config.json 中的单个字段"""
    cfg = load_config()
    cfg[key] = value
    CONFIG_JSON.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")


def _get_last_dates(cfg):
    """从配置中提取上次各层级执行时间"""
    return {
        "daily":   cfg.get("_last_daily"),
        "weekly":  cfg.get("_last_weekly"),
        "monthly": cfg.get("_last_monthly"),
    }


# ============================================================
#  进化周期主入口
# ============================================================

def run_evolution_cycle(df_scored, df_quotes, concept_map, meta,
                        skip_backtest=False, skip_tuning=False):
    """
    执行一次完整进化周期（日监控必跑，周/月按条件触发）。

    Args:
        df_scored:   本次因子得分 DataFrame
        df_quotes:   本次原始行情 DataFrame
        concept_map: 概念映射 dict
        meta:        元数据 dict (in_session, data_source 等)
        skip_backtest: 跳过回测
        skip_tuning:   跳过所有调优

    Returns:
        dict: 进化周期摘要
    """
    summary = {
        "时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "步骤": {},
    }

    cfg = load_config()
    last_dates = _get_last_dates(cfg)

    print("\n" + "=" * 60)
    print("  🧬 进化引擎 v2.0 — 三层迭代")
    print("=" * 60)

    # ────────────────────────────────────────────────────────
    #  Step A: 数据积累（每次必做）
    # ────────────────────────────────────────────────────────
    try:
        from data_accumulator import (
            accumulate_raw_quotes, accumulate_factor_scores,
            accumulate_concept_map, accumulate_daily_picks,
            format_accumulation_report,
        )
        accumulate_raw_quotes(df_quotes, meta)
        accumulate_factor_scores(df_scored, meta)
        accumulate_concept_map(concept_map, meta)
        accumulate_daily_picks(df_scored.head(10), meta)

        acc_report = format_accumulation_report()
        print(acc_report)

        summary["步骤"]["数据积累"] = "成功"
        _evo_log("数据积累", "全量存档完成")
    except Exception as e:
        summary["步骤"]["数据积累"] = f"失败: {e}"
        print(f"  [进化] 数据积累异常: {e}")
        _evo_log("数据积累", f"异常: {e}", "failure")

    # ────────────────────────────────────────────────────────
    #  Step B: 模拟交易（每次必做）
    # ────────────────────────────────────────────────────────
    try:
        from paper_trader import update_positions, open_positions, format_trade_report

        update_positions()
        open_positions(df_scored.head(10))

        trade_report = format_trade_report()
        print(trade_report)

        summary["步骤"]["模拟交易"] = "成功"
        _evo_log("模拟交易", "持仓更新+开新仓")
    except Exception as e:
        summary["步骤"]["模拟交易"] = f"失败: {e}"
        print(f"  [进化] 模拟交易异常: {e}")
        _evo_log("模拟交易", f"异常: {e}", "failure")

    # ────────────────────────────────────────────────────────
    #  Tier 1: 日监控 — 每次必跑
    # ────────────────────────────────────────────────────────
    daily_report = None
    if not skip_tuning:
        try:
            from factor_tuner import FactorTuner, format_daily_report

            tuner = FactorTuner()
            daily_report = tuner.tune_daily()
            if daily_report and daily_report.get("状态") != "跳过":
                print(format_daily_report(daily_report))

            status = daily_report.get("状态", "跳过") if daily_report else "失败"
            summary["步骤"]["日监控"] = f"{status}"
            save_config_field("_last_daily", datetime.now().strftime("%Y-%m-%d"))

            # ── 市场情景记忆：每日记录 ──
            try:
                from market_memory import record_daily_scenario
                from market_regime import detect_market_regime
                _regime = detect_market_regime()
                _nb = meta.get("northbound_dir", "无数据") if meta else "无数据"
                record_daily_scenario(df_scored, market_regime=_regime,
                                      northbound_dir=_nb, top_n=5)
            except Exception as e:
                print(f"  [情景记忆] 记录失败: {e}")
        except Exception as e:
            summary["步骤"]["日监控"] = f"失败: {e}"
            print(f"  [进化] 日监控异常: {e}")
            _evo_log("日监控", f"异常: {e}", "failure")

    # ────────────────────────────────────────────────────────
    #  Tier 2: 周迭代 — 条件触发（必须在回测之前，避免被慢回测阻塞）
    # ────────────────────────────────────────────────────────
    weekly_report = None
    if not skip_tuning:
        try:
            from factor_tuner import FactorTuner, format_weekly_report

            tuner = FactorTuner()
            should, reason = tuner.should_tune_weekly(last_dates.get("weekly"))
            if should:
                print("\n  ┌" + "─" * 58 + "┐")
                print("  │  📅 触发周迭代 — 因子权重微调")
                print("  └" + "─" * 58 + "┘")
                weekly_report = tuner.tune_weekly(dry_run=False)
                print(format_weekly_report(weekly_report))

                w_status = weekly_report.get("状态", "?")
                summary["步骤"]["周迭代"] = f"{w_status}"
                _evo_log("周迭代", f"{w_status}: {reason}")

                # ── 市场环境检测：周级别刷新（不每次选股都检测）──
                try:
                    from market_regime import get_regime_weights
                    _regime, _ = get_regime_weights(force_refresh=True)
                    _evo_log("市场环境", f"周度刷新: {_regime}")
                    print(f"  │  市场环境检测: {_regime}")
                except Exception as e:
                    print(f"  [市场环境] 刷新失败: {e}")

                # ── 市场情景记忆：每周LLM策略总结 ──
                try:
                    from market_memory import generate_weekly_summary
                    generate_weekly_summary()
                except Exception as e:
                    print(f"  [情景记忆] 周总结失败: {e}")
            else:
                summary["步骤"]["周迭代"] = f"跳过: {reason}"
                print(f"\n  [周迭代] {reason}")
        except Exception as e:
            summary["步骤"]["周迭代"] = f"失败: {e}"
            print(f"  [进化] 周迭代异常: {e}")
            _evo_log("周迭代", f"异常: {e}", "failure")

    # ────────────────────────────────────────────────────────
    #  Tier 3: 月迭代 — 条件触发（在回测之前，避免被阻塞）
    # ────────────────────────────────────────────────────────
    monthly_report = None
    if not skip_tuning:
        try:
            from factor_tuner import FactorTuner, format_monthly_report

            tuner = FactorTuner()
            should, reason = tuner.should_tune_monthly(last_dates.get("monthly"))
            if should:
                print("\n  ┌" + "─" * 58 + "┐")
                print("  │  🎯 触发月迭代 — 深度优化")
                print("  └" + "─" * 58 + "┘")
                monthly_report = tuner.tune_monthly(dry_run=False)
                print(format_monthly_report(monthly_report))

                # AI 评分反馈评估（月迭代时执行）
                try:
                    from ai_feedback import on_evolution_monthly as _run_ai_feedback
                    ai_eval = _run_ai_feedback()
                    if ai_eval:
                        print(f"\n  [AI反馈] 评估完成: IC_5d={ai_eval.get('ic_5d')}, 建议={ai_eval.get('recommendation')}")
                except Exception as e:
                    print(f"\n  [AI反馈] 评估失败: {e}")

                m_status = monthly_report.get("状态", "?")
                summary["步骤"]["月迭代"] = f"{m_status}"
                _evo_log("月迭代", f"{m_status}: {reason}")
            else:
                summary["步骤"]["月迭代"] = f"跳过: {reason}"
                print(f"\n  [月迭代] {reason}")
        except Exception as e:
            summary["步骤"]["月迭代"] = f"失败: {e}"
            print(f"  [进化] 月迭代异常: {e}")
            _evo_log("月迭代", f"异常: {e}", "failure")

    # ────────────────────────────────────────────────────────
    #  Step C: 回测验证（放在周/月迭代之后）
    #   TopN回测拉K线验证选出的股票；IC/分层/全样本用已回填数据不拉K线
    # ────────────────────────────────────────────────────────
    backtest_report = None
    quantile_report = None
    full_val_report = None
    if not skip_backtest:
        try:
            from backtest_engine import (run_backtest, format_backtest_report,
                                         run_full_market_ic, format_full_market_ic_report,
                                         run_quantile_backtest, run_full_sample_validation)

            # 1. TopN 回测（验证选出的股票，只拉10只/快照的K线）
            bt = run_backtest(hold_days=[1, 3, 5, 10], top_n=10, verbose=False)
            if bt:
                backtest_report = bt  # BUG修复: 之前bt的结果从未赋给backtest_report，导致回测反思永远不执行
                print(format_backtest_report(bt))
                summary["步骤"]["TopN回测"] = f"完成: {bt['验证记录数']} 条"
            else:
                summary["步骤"]["TopN回测"] = "数据不足"

            # 2. 全市场IC分析（用已回填数据，不拉K线）
            ic_r = run_full_market_ic(hold_days=[1, 3, 5, 10], verbose=False)
            if ic_r:
                print(format_full_market_ic_report(ic_r))
                summary["步骤"]["全市场IC"] = f"完成: {ic_r['有效样本数']} 条"
            else:
                summary["步骤"]["全市场IC"] = "数据不足"

            # 3. 分层回测（用已回填数据，不拉K线）
            try:
                qr = run_quantile_backtest(hold_days=[1, 3, 5, 10], verbose=False)
                if qr:
                    quantile_report = qr
                    valid = qr.get("打分有效性", {})
                    mono_ok = all(v.get("单调", False) for v in valid.values())
                    summary["步骤"]["分层回测"] = f"单调={'OK' if mono_ok else 'FAIL'} 样本={qr['有效样本数']}"
                else:
                    summary["步骤"]["分层回测"] = "数据不足"
            except Exception as e:
                summary["步骤"]["分层回测"] = f"失败: {e}"
                print(f"  [分层回测] 异常: {e}")

            # 4. 全样本验证（用已回填数据，不拉K线）
            try:
                fv = run_full_sample_validation(hold_days=[1, 3, 5, 10], verbose=False)
                if fv:
                    full_val_report = fv
                    results = fv.get("结果", {})
                    ic_avg = np.mean([r.get("IC", 0) for r in results.values()]) if results else 0
                    summary["步骤"]["全样本验证"] = f"IC均值={ic_avg:+.4f} 样本={fv['验证样本数']}"
                else:
                    summary["步骤"]["全样本验证"] = "数据不足"
            except Exception as e:
                summary["步骤"]["全样本验证"] = f"失败: {e}"
                print(f"  [全样本验证] 异常: {e}")

            _evo_log("回测", f"TopN={summary['步骤'].get('TopN回测','?')} 分层={summary['步骤'].get('分层回测','?')} 全样本={summary['步骤'].get('全样本验证','?')}")
        except Exception as e:
            summary["步骤"]["回测"] = f"失败: {e}"
            print(f"  [进化] 回测异常: {e}")
            _evo_log("回测", f"异常: {e}", "failure")

    # ────────────────────────────────────────────────────────
    #  Step E: 自我反思
    # ────────────────────────────────────────────────────────
    reflections = _generate_reflections(backtest_report, weekly_report, monthly_report, daily_report)
    if reflections:
        print(reflections)

    print("\n  ✅ 进化周期完成")
    _evo_log("进化周期", f"完成 — {json.dumps(summary['步骤'], ensure_ascii=False)}")

    # ── 市场情景记忆：自动遗忘 ──
    try:
        from market_memory import cleanup_old_scenarios
        cleanup_old_scenarios()
    except Exception:
        pass

    return summary


def _generate_reflections(backtest_report, weekly_report, monthly_report, daily_report):
    """基于各层级结果生成反思建议"""
    lines = []
    lines.append("")
    lines.append("╔" + "═" * 70 + "╗")
    lines.append("║  💡 自我反思 & 改进建议")
    lines.append("╠" + "═" * 70 + "╣")

    has_content = False

    # 日监控警告
    if daily_report and daily_report.get("状态") in ("warning", "danger"):
        danger = daily_report.get("危险因子", [])
        warnings = daily_report.get("预警因子", [])
        if danger:
            for d in danger:
                lines.append(f"║  🚨 因子 {d.get('因子','?')} 严重失效 (IC={d.get('IC',0):+.4f})，建议关注")
                has_content = True
        if warnings:
            for w in warnings[:3]:
                lines.append(f"║  ⚠ 因子 {w.get('因子','?')} 预警 ({w.get('级别','')})")
                has_content = True

    # 回测反思
    if backtest_report:
        stats = backtest_report.get("逐档统计", {})
        best_d = None
        best_sharpe = -999
        for d, s in stats.items():
            if s and s.get("夏普比率", -999) > best_sharpe:
                best_sharpe = s["夏普比率"]
                best_d = d
        if best_d:
            lines.append(f"║  最佳持仓周期: {best_d} 天 (夏普 {best_sharpe:+.2f})")
            has_content = True

        for d, s in stats.items():
            if s and s.get("胜率", 0) < 0.30 and s.get("样本数", 0) >= 10:
                lines.append(f"║  ⚠ {d}天持仓胜率过低 ({s['胜率']:.1%})，建议回避该周期")
                has_content = True

        bench = backtest_report.get("基准对比", {})
        for d, cmp in bench.items():
            alpha = cmp.get("超额收益", 0)
            if alpha < -0.02:
                lines.append(f"║  ⚠ {d}天: 跑输沪深300 ({alpha:+.1%})，需调整策略")
                has_content = True

    # 月迭代总结
    if monthly_report and monthly_report.get("状态") not in ("跳过", "失败"):
        eliminated = monthly_report.get("淘汰因子", [])
        if eliminated:
            lines.append(f"║  💀 本月淘汰: {', '.join(eliminated)}")
            has_content = True
        max_c = monthly_report.get("最大变化", 0)
        if max_c > 0.05:
            lines.append(f"║  📊 月度权重大幅调整 ({max_c:+.1%})，请观察下次回测")
            has_content = True

    if not has_content:
        lines.append("║  ✓ 系统运行正常，持续积累数据中")
        lines.append("║  ✓ 日监控无异常，因子表现稳定")

    lines.append("╚" + "═" * 70 + "╝")
    return "\n".join(lines)


# ============================================================
#  状态查看
# ============================================================

def get_evolution_stats():
    """获取进化系统全量统计"""
    cfg = load_config()
    last = _get_last_dates(cfg)

    stats = {
        "进化日志": 0,
        "上次日监控": last.get("daily", "从未"),
        "上次周迭代": last.get("weekly", "从未"),
        "上次月迭代": last.get("monthly", "从未"),
        "当前权重": cfg.get("weights", {}),
        "数据积累": {},
        "回测报告": 0,
        "调优报告": {},
        "模拟交易": {},
    }

    if EVO_LOG.exists():
        stats["进化日志"] = sum(1 for _ in open(EVO_LOG, encoding="utf-8"))

    # 数据积累统计
    from data_accumulator import get_accumulated_stats
    stats["数据积累"] = get_accumulated_stats()

    # 报告数
    reports = BASE_DIR / "reports"
    if reports.exists():
        stats["回测报告"] = len(list(reports.glob("回测报告_*.json")))
        stats["调优报告"] = {
            "周迭代": len(list(reports.glob("周迭代_*.json"))),
            "月迭代": len(list(reports.glob("月迭代_*.json"))),
        }

    # 模拟交易统计
    from paper_trader import get_trade_summary
    stats["模拟交易"] = get_trade_summary()

    # 下次触发预估
    stats["下次触发预估"] = _estimate_next_triggers(cfg, last, stats)

    return stats


def _estimate_next_triggers(cfg, last, stats):
    """预估下次周/月迭代触发时间"""
    est = {}
    today = datetime.now()

    # 周迭代预估
    if last.get("weekly"):
        last_w = datetime.strptime(last["weekly"], "%Y-%m-%d")
        days_to = 5 - (today - last_w).days
        if days_to > 0:
            est["周迭代"] = f"约 {days_to} 天后"
        else:
            est["周迭代"] = "可触发（下次运行）"
    else:
        est["周迭代"] = "首次运行后约5天"

    # 月迭代预估
    if last.get("monthly"):
        last_m = datetime.strptime(last["monthly"], "%Y-%m-%d")
        days_to = 20 - (today - last_m).days
        if days_to > 0:
            est["月迭代"] = f"约 {days_to} 天后"
        else:
            est["月迭代"] = "可触发（下次运行）"
    else:
        est["月迭代"] = "首次运行后约20天"

    return est


def format_evolution_status():
    """格式化进化系统状态"""
    s = get_evolution_stats()
    dat = s["数据积累"]
    trade = s["模拟交易"]
    est = s["下次触发预估"]

    lines = []
    lines.append("")
    lines.append("╔" + "═" * 62 + "╗")
    lines.append("║  🧬 进化系统 v2.0 — 三层迭代状态")
    lines.append("╠" + "═" * 62 + "╣")
    lines.append(f"║  📅 上次日监控: {s['上次日监控']}")
    lines.append(f"║  📅 上次周迭代: {s['上次周迭代']}")
    lines.append(f"║  📅 上次月迭代: {s['上次月迭代']}")
    lines.append("╟" + "─" * 62 + "╢")
    lines.append(f"║  🔮 下次周迭代: {est.get('周迭代', '—')}")
    lines.append(f"║  🔮 下次月迭代: {est.get('月迭代', '—')}")
    lines.append("╟" + "─" * 62 + "╢")
    lines.append(f"║  进化日志: {s['进化日志']:>4d} 条  │  回测报告: {s['回测报告']:>4d} 份")
    lines.append(f"║  周调优: {s['调优报告'].get('周迭代',0):>4d} 次    │  月调优: {s['调优报告'].get('月迭代',0):>4d} 次")
    lines.append(f"║  数据积累: {dat.get('总运行次数', 0):>4d} 次    │  交易日: {dat.get('交易日数', 0):>4d} 天")
    lines.append(f"║  模拟交易: {trade.get('已平仓数', 0):>4d} 已平仓  │  {trade.get('持仓中数', 0)} 持仓中")

    if trade.get("已平仓数", 0) > 0:
        lines.append(f"║  胜率: {trade.get('总胜率', 0):.1%}  │  累计盈亏: ¥{trade.get('总盈亏金额', 0):>+.2f}")

    # 当前权重速览
    weights = s.get("当前权重", {})
    if weights:
        lines.append("╟" + "─" * 62 + "╢")
        lines.append("║  当前因子权重 (Top 5):")
        for factor, w in list(sorted(weights.items(), key=lambda x: x[1], reverse=True))[:5]:
            lines.append(f"║    {factor:18s} → {w:.2%}")

    lines.append("╚" + "═" * 62 + "╝")
    return "\n".join(lines)


if __name__ == "__main__":
    print(format_evolution_status())
