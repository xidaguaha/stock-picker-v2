#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
因子调优器 v2.0 — 三层迭代：日监控 / 周微调 / 月深度优化
============================================================
核心理念：
  不是拍脑袋定权重，而是让数据说话。
  积累足够的回测数据后，自动计算每个因子的真实有效性，
  动态调整权重，让高分因子获得更高权重。

三层迭代架构：
  ┌─────────────────────────────────────────────────────────────┐
  │ 日迭代 (每次运行)  → 快速IC监控 → 只预警不出手              │
  │   触发: 每次运行  条件: >=5个交易日数据                      │
  │   动作: 检查因子IC是否翻负/大幅衰减 → 日志警告               │
  ├─────────────────────────────────────────────────────────────┤
  │ 周迭代 (每5-7天)  → IC微调 → 小步快跑                       │
  │   触发: 距上次>=5天 + >=5交易日  平滑: EMA α=0.12            │
  │   动作: 全市场IC分析 → 权重微调 → 写回config.json            │
  ├─────────────────────────────────────────────────────────────┤
  │ 月迭代 (每20-30天)→ 深度优化 → 市场分环境 + 因子淘汰        │
  │   触发: 距上次>=20天 + >=15交易日  平滑: EMA α=0.30          │
  │   动作: IC+ICIR+分环境 → 大调整 → 剔除失效因子 → 生成月报   │
  └─────────────────────────────────────────────────────────────┘

调优方法：
  1. IC 分析（信息系数）：因子得分与未来收益的 Spearman 相关系数
     → IC 越高 = 因子越有效 → 权重越高
  2. ICIR（信息比率）：IC 均值 / IC 标准差
     → IR 越高 = 因子稳定性和可靠性越高
  3. 滚动窗口：用最近N次数据计算，避免过拟合历史
  4. EMA平滑：新旧权重加权平均，避免剧烈抖动
  5. 市场分环境：区分牛/熊/震荡市，分别维护三套权重 （月迭代专属）
"""

import pandas as pd
import numpy as np
import json
from datetime import datetime, timedelta
from pathlib import Path

# ============================================================
#  路径配置
# ============================================================
BASE_DIR      = Path(__file__).parent.parent  # 指向项目根目录
CONFIG_JSON   = BASE_DIR / "config.json"
FACTOR_WEIGHTS_FILE = BASE_DIR / "factor_weights.json"
PERF_DIR      = BASE_DIR / "performance"
REPORTS_DIR   = BASE_DIR / "reports"
FACTOR_SCORES = BASE_DIR / "data" / "factor_scores"
EVO_LOG       = BASE_DIR / "evolution_log.jsonl"

# 默认配置
DEFAULT_MIN_SAMPLES_DAILY   = 5     # 日监控最少交易日
DEFAULT_MIN_SAMPLES_WEEKLY  = 5     # 周迭代最少交易日
DEFAULT_MIN_SAMPLES_MONTHLY = 15    # 月迭代最少交易日
DEFAULT_WEEKLY_INTERVAL     = 5     # 周迭代间隔（天）
DEFAULT_MONTHLY_INTERVAL    = 20    # 月迭代间隔（天）
DEFAULT_SMOOTHING_WEEKLY    = 0.12  # 周平滑系数
DEFAULT_SMOOTHING_MONTHLY   = 0.30  # 月平滑系数
DEFAULT_WEIGHT_DELTA_WEEKLY = 0.015 # 周调优最小变化阈值
DEFAULT_WEIGHT_DELTA_MONTHLY= 0.03  # 月调优最小变化阈值
DEFAULT_IC_DANGER           = -0.02 # IC低于此值视为"失效警告"
DEFAULT_IC_FATAL            = -0.05 # IC低于此值视为"严重失效"


def _evo_log(action, detail, result="success"):
    """写入进化日志"""
    entry = {
        "时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "操作": action,
        "详情": detail,
        "结果": result,
    }
    with open(EVO_LOG, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")


class FactorTuner:
    """因子自动调优器 v2.0 — 三层迭代"""

    def __init__(self,
                 min_samples_daily=DEFAULT_MIN_SAMPLES_DAILY,
                 min_samples_weekly=DEFAULT_MIN_SAMPLES_WEEKLY,
                 min_samples_monthly=DEFAULT_MIN_SAMPLES_MONTHLY,
                 weekly_interval=DEFAULT_WEEKLY_INTERVAL,
                 monthly_interval=DEFAULT_MONTHLY_INTERVAL,
                 smoothing_weekly=DEFAULT_SMOOTHING_WEEKLY,
                 smoothing_monthly=DEFAULT_SMOOTHING_MONTHLY,
                 weight_delta_weekly=DEFAULT_WEIGHT_DELTA_WEEKLY,
                 weight_delta_monthly=DEFAULT_WEIGHT_DELTA_MONTHLY,
                 ic_danger=DEFAULT_IC_DANGER,
                 ic_fatal=DEFAULT_IC_FATAL):
        self.min_samples_daily   = min_samples_daily
        self.min_samples_weekly  = min_samples_weekly
        self.min_samples_monthly = min_samples_monthly
        self.weekly_interval     = weekly_interval
        self.monthly_interval    = monthly_interval
        self.smoothing_weekly    = smoothing_weekly
        self.smoothing_monthly   = smoothing_monthly
        self.weight_delta_weekly = weight_delta_weekly
        self.weight_delta_monthly= weight_delta_monthly
        self.ic_danger           = ic_danger
        self.ic_fatal            = ic_fatal

    @staticmethod
    def load_current_weights():
        """从 factor_weights.json 加载当前因子权重"""
        if not FACTOR_WEIGHTS_FILE.exists():
            return None
        data = json.loads(FACTOR_WEIGHTS_FILE.read_text(encoding="utf-8-sig"))
        return data.get("weights", None)

    @staticmethod
    def load_historical_factors():
        """加载所有历史因子得分数据"""
        all_data = []
        if not FACTOR_SCORES.exists():
            return None

        for f in sorted(FACTOR_SCORES.glob("factors_*.parquet")):
            try:
                df = pd.read_parquet(f)
                all_data.append(df)
            except Exception:
                continue

        if not all_data:
            return None

        combined = pd.concat(all_data, ignore_index=True)
        combined = combined.drop_duplicates(
            subset=["快照日期", "代码"], keep="last"
        )
        return combined

    @staticmethod
    def _identify_factor_cols(df):
        """识别因子列"""
        return [c for c in df.columns
                if any(k in c for k in ["动量_", "趋势_", "量能_", "估值_", "规模_", "技术_", "概念_"])]

    def compute_ic(self, df_factors, factor_names, future_col="未来5日收益"):
        """计算每个因子对未来收益的 IC 值（Spearman Rank Correlation）。"""
        if future_col not in df_factors.columns:
            return {}

        valid = df_factors[factor_names + [future_col]].dropna(subset=[future_col])
        if len(valid) < self.min_samples_daily:
            return {}

        ic_results = {}
        for col in factor_names:
            if col not in valid.columns or valid[col].nunique() < 3:
                continue
            ic = valid[col].corr(valid[future_col], method="spearman")
            if not np.isnan(ic):
                ic_results[col] = round(float(ic), 6)

        return dict(sorted(ic_results.items(), key=lambda x: abs(x[1]), reverse=True))

    def compute_icir(self, df_factors, factor_names, future_cols):
        """
        计算 ICIR（信息比率）= IC均值 / IC标准差。
        在多个未来收益列上计算，取平均。
        """
        icir_results = {}
        date_groups = df_factors.groupby("快照日期")
        if date_groups.ngroups < 3:
            return icir_results

        for col in factor_names:
            if col not in df_factors.columns:
                continue
            ic_list = []
            for date, group in date_groups:
                valid = group[[col] + [c for c in future_cols if c in group.columns]]
                valid = valid.dropna(subset=[c for c in future_cols if c in valid.columns])
                if len(valid) < 5:
                    continue
                for fc in future_cols:
                    if fc not in valid.columns:
                        continue
                    ic = valid[col].corr(valid[fc], method="spearman")
                    if not np.isnan(ic):
                        ic_list.append(ic)

            if len(ic_list) >= 3:
                ic_mean = np.mean(ic_list)
                ic_std  = np.std(ic_list)
                ir = ic_mean / (ic_std + 1e-9)
                icir_results[col] = round(float(ir), 4)

        return dict(sorted(icir_results.items(), key=lambda x: abs(x[1]), reverse=True))

    def _compute_combined_ic(self, df, factor_cols, future_cols):
        """计算综合IC（所有未来收益列IC平均值）"""
        ic_results = {}
        for fc in future_cols:
            ic = self.compute_ic(df, factor_cols, fc)
            if ic:
                ic_results[fc] = ic

        combined = {}
        for col in factor_cols:
            vals = [ic.get(col, 0) for ic in ic_results.values()]
            combined[col] = round(float(np.mean(vals)), 6)

        return combined, ic_results

    def _optimize_weights(self, current_weights, combined_ic, smoothing,
                          weight_delta, icir_results=None):
        """
        核心权重优化算法（日/周/月共用）。
        返回: (new_weights, changes) 或 (None, {})
        """
        if not combined_ic:
            return None, {}

        # nan 保护：IC值为nan时视为0，避免nan传播到权重
        import math
        abs_ic = {}
        for k, v in combined_ic.items():
            if isinstance(v, float) and math.isnan(v):
                abs_ic[k] = 0.0
            else:
                abs_ic[k] = abs(v)
        total_ic = sum(abs_ic.get(k, 0) for k in current_weights)
        if total_ic < 1e-9:
            return None, {}

        # 按IC比例分配
        new_weights = {}
        for factor, old_w in current_weights.items():
            ic_val = abs_ic.get(factor, 0)
            # 低IC惩罚
            if ic_val < 0.02:
                ic_val *= 0.5
            new_w = ic_val / (total_ic + 1e-9)
            new_weights[factor] = new_w

        # ICIR微调
        if icir_results:
            for factor in new_weights:
                ir = icir_results.get(factor, None)
                if ir is not None:
                    if ir > 0.5:
                        new_weights[factor] *= 1.1
                    elif ir < -0.5:
                        new_weights[factor] *= 0.8

        # 重新归一化
        total_new = sum(new_weights.values())
        if total_new > 0:
            new_weights = {k: round(v / total_new, 4) for k, v in new_weights.items()}

        # 最低权重保护
        new_weights = {k: max(v, 0.02) for k, v in new_weights.items()}
        total_final = sum(new_weights.values())
        if total_final > 0:
            new_weights = {k: round(v / total_final, 4) for k, v in new_weights.items()}

        # EMA平滑
        smoothed = {}
        for factor in new_weights:
            old_w = current_weights.get(factor, new_weights[factor])
            smoothed[factor] = round(
                old_w * (1 - smoothing) + new_weights[factor] * smoothing, 4
            )

        # 归一化
        total_sm = sum(smoothed.values())
        if total_sm > 0:
            smoothed = {k: round(v / total_sm, 4) for k, v in smoothed.items()}

        # 计算变化量
        changes = {}
        for factor in smoothed:
            old_w = current_weights.get(factor, 0)
            changes[factor] = round(abs(smoothed[factor] - old_w), 4)

        max_change = max(changes.values()) if changes else 0
        if max_change < weight_delta:
            return None, {"最大变化": max_change, "阈值": weight_delta}

        return smoothed, changes

    # ================================================================
    #  Tier 1 — 日监控
    # ================================================================

    def tune_daily(self):
        """
        日监控：每次运行后快速检查因子健康度。

        只预警，不修改权重。
        - 检查因子IC是否翻负 (低于 ic_danger)
        - 检查因子IC是否严重失效 (低于 ic_fatal)
        - 记录趋势变化（相比上次的IC变化）

        Returns:
            dict: 监控报告 {"状态": "ok"|"warning"|"danger", "预警因子": [...]}
        """
        df = self.load_historical_factors()
        if df is None:
            return {"状态": "跳过", "原因": "无因子得分数据"}

        # 只取最近 N 天的数据
        date_groups = sorted(df["快照日期"].unique())
        if len(date_groups) < self.min_samples_daily:
            return {
                "状态": "跳过",
                "原因": f"交易日不足: {len(date_groups)} < {self.min_samples_daily}",
            }

        recent_dates = date_groups[-self.min_samples_daily:]
        df_recent = df[df["快照日期"].isin(recent_dates)]

        factor_cols = self._identify_factor_cols(df_recent)
        if not factor_cols:
            return {"状态": "跳过", "原因": "未找到因子列"}

        # 只用未来5日收益做快速IC
        future_cols = [c for c in df_recent.columns if "未来5日" in c]
        if not future_cols:
            return {"状态": "跳过", "原因": "无未来收益列"}

        ic = self.compute_ic(df_recent, factor_cols, future_cols[0])
        if not ic:
            return {"状态": "跳过", "原因": "IC分析无有效结果"}

        warnings = []
        danger = []
        ic_report = {}

        for factor, ic_val in ic.items():
            ic_report[factor] = ic_val
            if ic_val < self.ic_fatal:
                danger.append({"因子": factor, "IC": ic_val, "级别": "严重失效"})
            elif ic_val < self.ic_danger:
                warnings.append({"因子": factor, "IC": ic_val, "级别": "预警"})

        # 趋势对比：检查最近3天IC趋势
        if len(date_groups) >= 3:
            df_3d = df[df["快照日期"].isin(date_groups[-3:])]
            ic_per_day = {}
            for d in date_groups[-3:]:
                day_df = df_3d[df_3d["快照日期"] == d]
                ic_per_day[d] = self.compute_ic(day_df, factor_cols, future_cols[0])

            # 检查哪些因子IC连续2天下降
            for factor in factor_cols:
                ics = [ic_per_day[d].get(factor, 0) for d in date_groups[-3:]]
                if len(ics) >= 3 and ics[-1] < ics[-2] < ics[-3]:
                    # 连续下降
                    if ics[-1] < self.ic_danger:
                        trend_msg = f"{factor}: IC连续下降 {[f'{x:+.4f}' for x in ics]}"
                        if factor not in [w["因子"] for w in warnings]:
                            warnings.append({"因子": factor, "趋势": trend_msg, "级别": "趋势恶化"})

        status = "danger" if danger else "warning" if warnings else "ok"

        report = {
            "层级": "日监控",
            "状态": status,
            "时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "分析交易日": len(date_groups),
            "IC分析": ic_report,
            "预警因子": warnings,
            "危险因子": danger,
        }

        _evo_log("日监控", f"状态={status} 预警={len(warnings)} 危险={len(danger)}")
        return report

    # ================================================================
    #  Tier 2 — 周微调
    # ================================================================

    def should_tune_weekly(self, last_weekly_date=None):
        """判断是否触发周迭代"""
        df = self.load_historical_factors()
        if df is None:
            return False, "无因子数据"

        date_groups = df["快照日期"].nunique()
        if date_groups < self.min_samples_weekly:
            return False, f"交易日不足: {date_groups} < {self.min_samples_weekly}"

        if last_weekly_date:
            last_dt = datetime.strptime(last_weekly_date, "%Y-%m-%d")
            days_since = (datetime.now() - last_dt).days
            if days_since < self.weekly_interval:
                return False, f"距上次周迭代仅 {days_since} 天 (< {self.weekly_interval})"

        return True, f"条件满足 ({date_groups}个交易日)"

    def tune_weekly(self, dry_run=False):
        """
        周微调：基于全市场IC分析，小步调整权重。

        EMA平滑系数 0.12，变化阈值 0.015，调幅温和。
        """
        current = self.load_current_weights()
        if not current:
            return {"状态": "失败", "原因": "未找到 weights"}

        df = self.load_historical_factors()
        if df is None or len(df) < 5:
            return {"状态": "跳过", "原因": f"数据不足: {len(df) if df is not None else 0} 条"}

        factor_cols = self._identify_factor_cols(df)
        if not factor_cols:
            return {"状态": "跳过", "原因": "未找到因子列"}

        future_cols = [c for c in df.columns if c.startswith("未来") and "收益" in c]
        combined_ic, ic_results = self._compute_combined_ic(df, factor_cols, future_cols)

        if not combined_ic:
            return {"状态": "跳过", "原因": "IC分析无有效结果"}

        icir = self.compute_icir(df, factor_cols, future_cols)

        new_weights, changes = self._optimize_weights(
            current, combined_ic,
            smoothing=self.smoothing_weekly,
            weight_delta=self.weight_delta_weekly,
            icir_results=icir,
        )

        if new_weights is None:
            return {
                "状态": "跳过",
                "原因": f"最大变化 {changes.get('最大变化', 0):.4f} < {self.weight_delta_weekly}",
                "最大变化": changes.get("最大变化", 0),
            }

        max_change = max(changes.values()) if changes else 0

        report = {
            "层级": "周迭代",
            "状态": "建议更新" if not dry_run else "仅分析(dry_run)",
            "时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "样本数": len(df),
            "交易日": df["快照日期"].nunique(),
            "因子数": len(factor_cols),
            "平滑系数": self.smoothing_weekly,
            "IC分析": {
                "分析未来列": future_cols,
                "综合IC": combined_ic,
            },
            "ICIR分析": icir,
            "当前权重": current,
            "建议权重": new_weights,
            "权重变化": changes,
            "最大变化": round(max_change, 4),
        }

        if not dry_run:
            # 写入迭代变更记录
            try:
                from evolution_record import record_weight_change
                record_weight_change(
                    tier="weekly",
                    before_weights=current,
                    after_weights=new_weights,
                    reason=f"周迭代IC分析，最大变化={max_change:.4f}",
                    backtest_summary=None
                )
            except Exception as e:
                print(f"  [迭代记录] 写入失败: {e}")

            self._save_weights(new_weights)
            self._save_config_field("_last_weekly", datetime.now().strftime("%Y-%m-%d"))

        self._save_report(report, "周迭代")
        _evo_log("周迭代", f"权重{'已更新' if not dry_run else '分析完成'}")
        return report

    # ================================================================
    #  Tier 3 — 月深度优化
    # ================================================================

    def should_tune_monthly(self, last_monthly_date=None):
        """判断是否触发月迭代"""
        df = self.load_historical_factors()
        if df is None:
            return False, "无因子数据"

        date_groups = df["快照日期"].nunique()
        if date_groups < self.min_samples_monthly:
            return False, f"交易日不足: {date_groups} < {self.min_samples_monthly}"

        if last_monthly_date:
            last_dt = datetime.strptime(last_monthly_date, "%Y-%m-%d")
            days_since = (datetime.now() - last_dt).days
            if days_since < self.monthly_interval:
                return False, f"距上次月迭代仅 {days_since} 天 (< {self.monthly_interval})"

        return True, f"条件满足 ({date_groups}个交易日)"

    def tune_monthly(self, dry_run=False):
        """
        月深度优化：全市场IC+ICIR + 市场分环境 + 因子淘汰。

        动作：
        1. 完整IC/ICIR分析（所有持有周期）
        2. 因子有效性排名（连续失效的考虑淘汰）
        3. 市场环境分拆（牛/熊/震荡）暂为占位
        4. 深度权重调整（EMA α=0.30）
        5. 生成月度报告
        """
        current = self.load_current_weights()
        if not current:
            return {"状态": "失败", "原因": "未找到 weights"}

        df = self.load_historical_factors()
        if df is None or len(df) < 10:
            return {"状态": "跳过", "原因": f"数据不足: {len(df) if df is not None else 0} 条"}

        factor_cols = self._identify_factor_cols(df)
        if not factor_cols:
            return {"状态": "跳过", "原因": "未找到因子列"}

        future_cols = [c for c in df.columns if c.startswith("未来") and "收益" in c]
        combined_ic, all_ic = self._compute_combined_ic(df, factor_cols, future_cols)

        if not combined_ic:
            return {"状态": "跳过", "原因": "IC分析无有效结果"}

        icir = self.compute_icir(df, factor_cols, future_cols)

        # ── 因子有效性评估 ──
        factor_health = {}
        eliminated = []
        for factor in factor_cols:
            ic_val = combined_ic.get(factor, 0)
            ir_val = icir.get(factor, 0)
            # 连续多月IC为负且IR也低 → 建议淘汰
            if ic_val < self.ic_fatal and ir_val < -0.1:
                factor_health[factor] = "建议淘汰"
                eliminated.append(factor)
            elif ic_val < self.ic_danger:
                factor_health[factor] = "弱有效"
            elif ic_val > 0.05:
                factor_health[factor] = "强有效"
            else:
                factor_health[factor] = "一般"

        # 淘汰：把淘汰因子的权重设为0，重新归一化
        current_adj = dict(current)
        if eliminated:
            for ef in eliminated:
                current_adj[ef] = 0

        # ── 深度优化 ──
        new_weights, changes = self._optimize_weights(
            current_adj, combined_ic,
            smoothing=self.smoothing_monthly,
            weight_delta=self.weight_delta_monthly,
            icir_results=icir,
        )

        # 强制将淘汰因子权重归零（_optimize_weights 中的最低权重保护和EMA平滑会导致其恢复权重）
        if new_weights and eliminated:
            for ef in eliminated:
                new_weights[ef] = 0.0
            total = sum(new_weights.values())
            if total > 0:
                new_weights = {k: round(v / total, 4) for k, v in new_weights.items()}
            changes = {k: round(abs(new_weights.get(k, 0) - current.get(k, 0)), 4) for k in new_weights}

        if new_weights is None:
            # 即使没到阈值，月迭代至少报告完整数据
            return {
                "状态": "仅分析",
                "原因": f"变化不足阈值",
                "层级": "月迭代",
                "时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "样本数": len(df),
                "交易日": df["快照日期"].nunique(),
                "因子数": len(factor_cols),
                "因子健康度": factor_health,
                "淘汰因子": eliminated,
                "IC分析": {"综合IC": combined_ic},
                "ICIR分析": icir,
                "当前权重": current,
                "建议权重": current_adj,
            }

        max_change = max(changes.values()) if changes else 0

        report = {
            "层级": "月迭代",
            "状态": "建议更新" if not dry_run else "仅分析(dry_run)",
            "时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "样本数": len(df),
            "交易日": df["快照日期"].nunique(),
            "因子数": len(factor_cols),
            "平滑系数": self.smoothing_monthly,
            "IC分析": {
                "分析未来列": future_cols,
                "综合IC": combined_ic,
                "分项IC": all_ic,
            },
            "ICIR分析": icir,
            "因子健康度": factor_health,
            "淘汰因子": eliminated,
            "当前权重": current,
            "建议权重": new_weights,
            "权重变化": changes,
            "最大变化": round(max_change, 4),
        }

        if not dry_run:
            # 写入迭代变更记录
            try:
                from evolution_record import record_weight_change
                record_weight_change(
                    tier="monthly",
                    before_weights=current,
                    after_weights=new_weights,
                    reason=f"月迭代深度优化，淘汰{len(eliminated)}个因子，最大变化={max_change:.4f}",
                    backtest_summary=None
                )
            except Exception as e:
                print(f"  [迭代记录] 写入失败: {e}")

            self._save_weights(new_weights)
            self._save_config_field("_last_monthly", datetime.now().strftime("%Y-%m-%d"))

        self._save_report(report, "月迭代")
        _evo_log("月迭代", f"权重{'已更新' if not dry_run else '分析完成'} 淘汰{len(eliminated)}个")
        return report

    # ================================================================
    #  工具方法
    # ================================================================

    def _save_weights(self, new_weights):
        """将新权重写回 factor_weights.json"""
        if not FACTOR_WEIGHTS_FILE.exists():
            return

        # nan 保护：任何权重为nan时拒绝写入，保留旧权重
        import math
        for k, v in new_weights.items():
            if isinstance(v, float) and math.isnan(v):
                print(f"  [权重保护] 检测到nan权重 {k}, 拒绝写入, 保留旧权重")
                return

        data = json.loads(FACTOR_WEIGHTS_FILE.read_text(encoding="utf-8-sig"))
        old_weights = data.get("weights", {})
        data["weights"] = new_weights
        data["last_updated"] = datetime.now().strftime("%Y-%m-%d")
        data["update_source"] = data.get("update_source", "default")
        # 保留优化历史
        history = data.get("_optimize_history", [])
        history.append({
            "时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "旧权重": old_weights,
            "新权重": new_weights,
        })
        data["_optimize_history"] = history[-20:]
        FACTOR_WEIGHTS_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    def _save_config_field(self, key, value):
        """更新 config.json 中的单个字段"""
        if not CONFIG_JSON.exists():
            return
        cfg = json.loads(CONFIG_JSON.read_text(encoding="utf-8"))
        cfg[key] = value
        CONFIG_JSON.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")

    def _save_report(self, report, tier_name):
        """保存调优报告"""
        path = REPORTS_DIR / f"{tier_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    # ================================================================
    #  兼容旧版 run() 接口
    # ================================================================
    def run(self, dry_run=False):
        """兼容旧版单次调优接口，走周迭代逻辑"""
        return self.tune_weekly(dry_run=dry_run)

    def should_optimize(self, last_optimize_date=None):
        """兼容旧版判断，等同周迭代判断"""
        return self.should_tune_weekly(last_optimize_date)


# ============================================================
#  格式化输出
# ============================================================

def format_daily_report(report):
    """格式化日监控报告"""
    if report.get("状态") == "跳过":
        return ""

    lines = []
    lines.append("")
    lines.append("╔" + "═" * 66 + "╗")
    lines.append("║  🟢 日监控 — 因子健康度巡检")
    lines.append("╠" + "═" * 66 + "╣")
    lines.append(f"║  状态: {report['状态']:10s}  │  交易日: {report.get('分析交易日', 0)}天")

    ic = report.get("IC分析", {})
    if ic:
        lines.append("╟" + "─" * 66 + "╢")
        lines.append("║  因子                │ IC值     │ 健康度")
        for factor, ic_val in sorted(ic.items(), key=lambda x: x[1], reverse=True):
            if ic_val > 0.05:
                health = "🟢 好"
            elif ic_val > 0:
                health = "🟡 一般"
            elif ic_val > -0.02:
                health = "🟠 弱"
            else:
                health = "🔴 差"
            lines.append(f"║  {factor:18s} │ {ic_val:+.4f}  │ {health}")

    warnings = report.get("预警因子", [])
    danger = report.get("危险因子", [])
    if warnings or danger:
        lines.append("╟" + "─" * 66 + "╢")
        for w in warnings:
            lines.append(f"║  ⚠ {w.get('因子', '')}: {w.get('级别', '')}")
        for d in danger:
            lines.append(f"║  🚨 {d.get('因子', '')}: {d.get('级别', '')} IC={d.get('IC', 0):+.4f}")

    lines.append("╚" + "═" * 66 + "╝")
    return "\n".join(lines)


def format_weekly_report(report):
    """格式化周迭代报告"""
    if report.get("状态") in ("跳过", "失败"):
        return f"\n  [周迭代] {report['状态']}: {report.get('原因', '未知')}"

    lines = []
    lines.append("")
    lines.append("╔" + "═" * 72 + "╗")
    lines.append("║  📊 周迭代 — 因子权重微调")
    lines.append("╠" + "═" * 72 + "╣")
    lines.append(f"║  状态: {report['状态']:8s}  │ 交易日: {report.get('交易日', 0):>3d}天  │ "
                 f"平滑: {report.get('平滑系数', 0.12):.2f}")
    lines.append("╟" + "─" * 72 + "╢")
    lines.append("║  因子              │ IC值     │ 原权重  →  新权重  │ 变化")
    lines.append("╟" + "─" * 72 + "╢")

    ic = report.get("IC分析", {}).get("综合IC", {})
    old_w = report.get("当前权重", {})
    new_w = report.get("建议权重", {})
    changes = report.get("权重变化", {})

    for factor in sorted(new_w.keys(), key=lambda f: new_w[f], reverse=True):
        ic_val = ic.get(factor, 0)
        ow = old_w.get(factor, 0)
        nw = new_w.get(factor, 0)
        chg = changes.get(factor, 0)
        direction = "↑" if nw > ow else "↓" if nw < ow else "→"
        lines.append(
            f"║  {factor:18s} │ {ic_val:+.4f}  │ "
            f"{ow:.4f} {direction} {nw:.4f}  │ {chg:+.4f}"
        )

    lines.append("╚" + "═" * 72 + "╝")
    return "\n".join(lines)


def format_monthly_report(report):
    """格式化月迭代报告"""
    if report.get("状态") in ("跳过", "失败"):
        return f"\n  [月迭代] {report['状态']}: {report.get('原因', '未知')}"

    lines = []
    lines.append("")
    lines.append("╔" + "═" * 80 + "╗")
    lines.append("║  🎯 月迭代 — 深度优化报告")
    lines.append("╠" + "═" * 80 + "╣")
    lines.append(f"║  状态: {report['状态']:8s}  │ 交易日: {report.get('交易日', 0):>3d}天  │ "
                 f"样本: {report.get('样本数', 0):>5d}条  │ 平滑: {report.get('平滑系数', 0.30):.2f}")
    lines.append("╠" + "═" * 80 + "╣")

    # 因子健康度
    health = report.get("因子健康度", {})
    if health:
        lines.append("║  【因子健康度评估】")
        for factor, h in health.items():
            emoji = "🟢" if "强" in h else "🟡" if "一般" in h else "🔴" if "淘汰" in h else "🟠"
            lines.append(f"║    {emoji} {factor:18s} → {h}")
        lines.append("╟" + "─" * 80 + "╢")

    # 淘汰因子
    eliminated = report.get("淘汰因子", [])
    if eliminated:
        lines.append(f"║  ⚠ 本月淘汰因子: {', '.join(eliminated)}")
        lines.append("╟" + "─" * 80 + "╢")

    # 权重变化
    lines.append("║  因子              │ IC值     │ 原权重  →  新权重  │ 变化")
    lines.append("╟" + "─" * 80 + "╢")

    ic = report.get("IC分析", {}).get("综合IC", {})
    old_w = report.get("当前权重", {})
    new_w = report.get("建议权重", {})
    changes = report.get("权重变化", {})

    for factor in sorted(new_w.keys(), key=lambda f: new_w[f], reverse=True):
        ic_val = ic.get(factor, 0)
        ow = old_w.get(factor, 0)
        nw = new_w.get(factor, 0)
        chg = changes.get(factor, 0)
        direction = "↑" if nw > ow else "↓" if nw < ow else "→"
        lines.append(
            f"║  {factor:18s} │ {ic_val:+.4f}  │ "
            f"{ow:.4f} {direction} {nw:.4f}  │ {chg:+.4f}"
        )

    lines.append("╚" + "═" * 80 + "╝")
    return "\n".join(lines)


# ============================================================
#  兼容旧版 format_tuning_report
# ============================================================
def format_tuning_report(report):
    """通用格式化，兼容旧调用"""
    tier = report.get("层级", "周迭代")
    if tier == "日监控":
        return format_daily_report(report)
    elif tier == "月迭代":
        return format_monthly_report(report)
    else:
        return format_weekly_report(report)


def auto_tune(dry_run=False):
    """便捷函数：自动调优（旧接口，走周迭代）"""
    tuner = FactorTuner()
    return tuner.run(dry_run=dry_run)


if __name__ == "__main__":
    print("=" * 50)
    print("  因子调优器 v2.0 本地测试")
    print("=" * 50)

    tuner = FactorTuner()

    print("\n--- 日监控 ---")
    r = tuner.tune_daily()
    print(format_daily_report(r))

    print("\n--- 周迭代 (dry_run) ---")
    r = tuner.tune_weekly(dry_run=True)
    print(format_weekly_report(r))
