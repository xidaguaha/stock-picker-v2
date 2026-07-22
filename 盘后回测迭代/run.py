#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
盘后回测迭代 — 5天回溯验证 + 三项迭代
职责：
  1. 读取过去5天的选股结果
  2. 分别验证1/2/3/4/5天持仓收益（Rank IC / 胜率 / 收益率）
  3. 验证模拟交易
  4. 进化迭代
  5. 缓存月度清理
"""
import sys
from pathlib import Path

SHARED = Path(__file__).parent.parent / "共用模块"
sys.path.insert(0, str(SHARED))

from bug_log import setup_logger, log_info, log_error, log_perf
setup_logger("盘后回测迭代")

import json
import time as _time
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from stock_picker import get_now

BASE_DIR = Path(__file__).parent.parent
REPORT_DIR = BASE_DIR / "reports"
REPORT_DIR.mkdir(parents=True, exist_ok=True)


def _load_recent_snapshots(history_dir: Path, days: int = 5) -> list:
    try:
        from trading_calendar import is_trading_day
    except ImportError:
        is_trading_day = lambda dt: dt.weekday() < 5

    snapshots = []
    checked = 0
    d = datetime.now()
    while len(snapshots) < days and checked < 15:
        d = d - timedelta(days=1)
        checked += 1
        if not is_trading_day(d):
            continue
        date_str = d.strftime("%Y%m%d")
        files = sorted(history_dir.rglob(f"snapshot_{date_str}_*.csv"), reverse=True)
        if files:
            try:
                df = pd.read_csv(files[0], encoding="utf-8-sig")
                if "代码" in df.columns and len(df) > 0:
                    snapshots.append((date_str, df))
                    log_info(f"  加载 {date_str}: {files[0].name} ({len(df)} 只)")
            except Exception as e:
                log_error(e, f"加载 {date_str} 失败")
    if not snapshots:
        log_info("  无历史快照")
    else:
        log_info(f"  共加载 {len(snapshots)} 个交易日快照")
    return snapshots


def _load_today_snapshot(history_dir: Path) -> pd.DataFrame:
    today_str = datetime.now().strftime("%Y%m%d")
    files = sorted(history_dir.rglob(f"snapshot_{today_str}_*.csv"), reverse=True)
    if files:
        try:
            df = pd.read_csv(files[0], encoding="utf-8-sig")
            if "代码" in df.columns and len(df) > 0:
                log_info(f"  今日快照: {files[0].name} ({len(df)} 只)")
                return df
        except Exception as e:
            log_error(e, "加载今日快照失败")
    log_info("  无今日快照")
    return None


def _backtest_snapshot(snap_date: str, snap_df: pd.DataFrame,
                       hold_days_list: list) -> dict:
    from backtest_engine import verify_pick_performance
    from concurrent.futures import ThreadPoolExecutor, as_completed

    results = {d: [] for d in hold_days_list}
    verified = 0

    _df = snap_df.copy()
    if "综合得分" in _df.columns:
        _df = _df.sort_values("综合得分", ascending=False)
    _df_top = _df.head(50)

    snap_dt = datetime.strptime(snap_date, "%Y%m%d")

    def _verify_one(row):
        code = str(row["代码"]).zfill(6)
        score = row.get("综合得分", 0)
        perf = verify_pick_performance(code, snap_dt, hold_days=hold_days_list)
        return code, score, perf

    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(_verify_one, row) for _, row in _df_top.iterrows()]
        for future in as_completed(futures):
            try:
                code, score, perf = future.result()
                if perf is None:
                    continue
                verified += 1
                for d in hold_days_list:
                    ret = perf.get(d)
                    if ret is not None:
                        results[d].append({"代码": code, "综合得分": score, "收益": ret})
            except Exception:
                continue

    return {"verified": verified, "total": len(snap_df), "results": results}


def _cleanup_cache():
    try:
        from trading_calendar import is_trading_day
    except ImportError:
        is_trading_day = lambda dt: dt.weekday() < 5

    cutoff = datetime.now()
    trading_days_found = 0
    while trading_days_found < 30:
        cutoff -= timedelta(days=1)
        if is_trading_day(cutoff):
            trading_days_found += 1
    cutoff_str = cutoff.strftime("%Y-%m-%d")
    cleaned = 0

    ai_cache = BASE_DIR / "agent_data" / "ai_analysis_cache.json"
    if ai_cache.exists():
        try:
            cache = json.loads(ai_cache.read_text(encoding="utf-8"))
            new_cache = {}
            for k, v in cache.items():
                ts = v.get("_cached_at", v.get("分析时间", ""))
                if ts:
                    try:
                        cache_date = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                        if cache_date.strftime("%Y-%m-%d") >= cutoff_str:
                            new_cache[k] = v
                    except Exception:
                        new_cache[k] = v
                else:
                    new_cache[k] = v
            removed = len(cache) - len(new_cache)
            if removed > 0:
                ai_cache.write_text(json.dumps(new_cache, ensure_ascii=False, indent=2), encoding="utf-8")
                cleaned += removed
                log_info(f"  AI缓存: 清理 {removed} 条, 保留 {len(new_cache)} 条")
        except Exception as e:
            log_error(e, "AI缓存清理失败")

    news_dir = BASE_DIR / "agent_data" / "news"
    if news_dir.exists():
        news_cleaned = 0
        for f in news_dir.glob("*.jsonl"):
            try:
                file_date = datetime.strptime(f.stem, "%Y-%m-%d")
                if (datetime.now() - file_date).days > 7:
                    f.unlink()
                    news_cleaned += 1
            except Exception as e:
                log_error(e, "缓存清理操作异常")
        if news_cleaned > 0:
            log_info(f"  新闻缓存: 清理 {news_cleaned} 个过期文件 (保留7天)")
            cleaned += news_cleaned

    agent_dir = BASE_DIR / "agent_data"
    for f in agent_dir.glob("ai_analysis_*.json"):
        try:
            ts_str = f.stem.replace("ai_analysis_", "")
            if not ts_str[:8].isdigit():
                continue
            file_date = datetime.strptime(ts_str[:8], "%Y%m%d")
            if file_date.strftime("%Y-%m-%d") < cutoff_str:
                f.unlink()
                cleaned += 1
        except Exception as e:
            log_error(e, "缓存清理操作异常")

    for f in REPORT_DIR.glob("回测报告_*.json"):
        try:
            ts_str = f.stem.replace("回测报告_", "")
            if not ts_str[:8].isdigit():
                continue
            file_date = datetime.strptime(ts_str[:8], "%Y%m%d")
            if file_date.strftime("%Y-%m-%d") < cutoff_str:
                f.unlink()
                cleaned += 1
        except Exception as e:
            log_error(e, "缓存清理操作异常")

    kline_dir = BASE_DIR / "cache"
    if kline_dir.exists():
        kline_cleaned = 0
        for f in kline_dir.glob("kline_*.parquet"):
            try:
                mtime = datetime.fromtimestamp(f.stat().st_mtime)
                if mtime.strftime("%Y-%m-%d") < cutoff_str:
                    f.unlink()
                    kline_cleaned += 1
            except Exception as e:
                log_error(e, "缓存清理操作异常")
        if kline_cleaned > 0:
            log_info(f"  K线缓存: 清理 {kline_cleaned} 个过期文件")
            cleaned += kline_cleaned

    factor_dir = BASE_DIR / "data" / "factor_scores"
    if factor_dir.exists():
        factor_cleaned = 0
        for f in factor_dir.glob("factors_*.parquet"):
            try:
                ts_str = f.stem.replace("factors_", "")
                file_date = datetime.strptime(ts_str[:8], "%Y%m%d")
                if file_date.strftime("%Y-%m-%d") < cutoff_str:
                    f.unlink()
                    factor_cleaned += 1
            except Exception as e:
                log_error(e, "缓存清理操作异常")
        if factor_cleaned > 0:
            log_info(f"  因子得分: 清理 {factor_cleaned} 个过期文件")
            cleaned += factor_cleaned

    history_dir = BASE_DIR / "history"
    if history_dir.exists():
        snap_cleaned = 0
        for f in history_dir.rglob("snapshot_*.csv"):
            try:
                ts_str = f.stem.replace("snapshot_", "")
                file_date = datetime.strptime(ts_str[:8], "%Y%m%d")
                if file_date.strftime("%Y-%m-%d") < cutoff_str:
                    f.unlink()
                    snap_cleaned += 1
            except Exception as e:
                log_error(e, "缓存清理操作异常")
        for f in history_dir.rglob("snapshot_*.json"):
            try:
                ts_str = f.stem.replace("snapshot_", "")
                file_date = datetime.strptime(ts_str[:8], "%Y%m%d")
                if file_date.strftime("%Y-%m-%d") < cutoff_str:
                    f.unlink()
                    snap_cleaned += 1
            except Exception as e:
                log_error(e, "缓存清理操作异常")
        if snap_cleaned > 0:
            log_info(f"  历史快照: 清理 {snap_cleaned} 个过期文件")
            cleaned += snap_cleaned

    pred_dir = BASE_DIR / "ai_predictions"
    if pred_dir.exists():
        pred_cleaned = 0
        for f in pred_dir.glob("*.jsonl"):
            try:
                file_date = datetime.strptime(f.stem, "%Y-%m-%d")
                if file_date.strftime("%Y-%m-%d") < cutoff_str:
                    f.unlink()
                    pred_cleaned += 1
            except Exception as e:
                log_error(e, "缓存清理操作异常")
        if pred_cleaned > 0:
            log_info(f"  AI预测记录: 清理 {pred_cleaned} 个过期文件")
            cleaned += pred_cleaned

    trades_dir = BASE_DIR / "trades"
    if trades_dir.exists():
        trade_cleaned = 0
        for f in trades_dir.glob("trade_log_*.jsonl"):
            try:
                file_date = datetime.strptime(f.stem.replace("trade_log_", ""), "%Y-%m-%d")
                if file_date.strftime("%Y-%m-%d") < cutoff_str:
                    f.unlink()
                    trade_cleaned += 1
            except Exception as e:
                log_error(e, "缓存清理操作异常")
        if trade_cleaned > 0:
            log_info(f"  模拟交易日志: 清理 {trade_cleaned} 个过期文件")
            cleaned += trade_cleaned

    concept_dir = BASE_DIR / "data" / "concept_maps"
    if concept_dir.exists():
        concept_cleaned = 0
        for f in concept_dir.glob("concepts_*.json"):
            try:
                ts_str = f.stem.replace("concepts_", "")
                file_date = datetime.strptime(ts_str[:8], "%Y%m%d")
                if file_date.strftime("%Y-%m-%d") < cutoff_str:
                    f.unlink()
                    concept_cleaned += 1
            except Exception as e:
                log_error(e, "缓存清理操作异常")
        if concept_cleaned > 0:
            log_info(f"  概念映射: 清理 {concept_cleaned} 个过期文件")
            cleaned += concept_cleaned

    if cleaned > 0:
        log_info(f"  缓存清理完成: 共清理 {cleaned} 条/文件 (保留最近30个交易日)")
    else:
        log_info("  缓存无需清理")


def main():
    log_info("=" * 50)
    log_info("盘后回测迭代 — K线回溯验证 + 迭代")
    log_info("=" * 50)

    start = _time.time()
    report = {"时间": get_now().isoformat(), "回测": {}}

    try:
        history_dir = BASE_DIR / "history"

        log_info("Step 1: 加载过去5个交易日选股快照")
        snapshots = _load_recent_snapshots(history_dir, days=5)

        log_info("Step 2: 加载今日选股快照")
        df_today = _load_today_snapshot(history_dir)

        if snapshots:
            log_info("Step 3: 5个交易日K线回溯验证 (Top50)")
            import threading

            hold_periods = [1, 2, 3, 4, 5]
            all_metrics = {}
            _bt_result = {"done": False, "metrics": {}, "error": None}

            def _run_backtest():
                try:
                    for hold_days in hold_periods:
                        period_results = []
                        for snap_date, snap_df in snapshots:
                            bt = _backtest_snapshot(snap_date, snap_df, [hold_days])
                            if bt["verified"] > 0:
                                rets = [r["收益"] for r in bt["results"][hold_days]]
                                scores = [r["综合得分"] for r in bt["results"][hold_days]]
                                ic = 0.0
                                if len(rets) >= 2:
                                    try:
                                        ic = float(np.corrcoef(scores, rets)[0, 1])
                                        if np.isnan(ic):
                                            ic = 0.0
                                    except Exception:
                                        ic = 0.0
                                top5_ret = float(np.mean(rets[:5])) if len(rets) >= 5 else 0
                                top5_win = float(np.mean([r > 0 for r in rets[:5]])) if len(rets) >= 5 else 0
                                top10_win = float(np.mean([r > 0 for r in rets[:10]])) if len(rets) >= 10 else 0
                                period_results.append({
                                    "选股日期": snap_date,
                                    "验证数": bt["verified"],
                                    "Rank_IC": round(ic, 4),
                                    "Top5胜率": round(top5_win, 4),
                                    "Top5收益": round(top5_ret, 4),
                                    "Top10胜率": round(top10_win, 4),
                                })

                        if period_results:
                            avg_ic = np.mean([r["Rank_IC"] for r in period_results])
                            avg_top5_win = np.mean([r["Top5胜率"] for r in period_results])
                            avg_top5_ret = np.mean([r["Top5收益"] for r in period_results])
                            avg_top10_win = np.mean([r["Top10胜率"] for r in period_results])

                            summary = {
                                "验证交易日数": len(period_results),
                                "平均IC": round(float(avg_ic), 4),
                                "平均Top5胜率": round(float(avg_top5_win), 4),
                                "平均Top5收益": round(float(avg_top5_ret), 4),
                                "平均Top10胜率": round(float(avg_top10_win), 4),
                                "逐日明细": period_results,
                            }
                            all_metrics[f"{hold_days}个交易日持仓"] = summary

                            log_info(f"  {hold_days}天持仓: IC={avg_ic:+.4f} Top5胜率={avg_top5_win:.1%} "
                                     f"Top5收益={avg_top5_ret:+.2%} 验证={len(period_results)}天")
                        else:
                            all_metrics[f"{hold_days}个交易日持仓"] = {"状态": "无数据"}

                    _bt_result["done"] = True
                    _bt_result["metrics"] = all_metrics
                except Exception as e:
                    _bt_result["error"] = e

            t = threading.Thread(target=_run_backtest, daemon=True)
            t.start()
            t.join(timeout=600)

            if t.is_alive():
                log_info("Step 3: K线回测验证超时(10分钟)，跳过剩余回测")
                report["回测"] = all_metrics if all_metrics else {"状态": "超时跳过"}
            elif _bt_result["error"]:
                log_error(_bt_result["error"], "Step 3回测异常")
                report["回测"] = {"状态": f"异常: {_bt_result['error']}"}
            else:
                report["回测"] = _bt_result["metrics"]
        else:
            log_info("Step 3: 无历史快照，跳过回测")

        log_info("Step 4: 验证模拟交易")
        try:
            from paper_trader import update_positions, open_positions, format_trade_report
            update_positions()
            if df_today is not None and len(df_today) > 0:
                open_positions(df_today.head(10))
            print(format_trade_report())
            report["模拟交易"] = "已更新"
        except Exception as e:
            log_error(e, "模拟交易跳过")

        log_info("Step 5: 进化迭代")
        if df_today is not None and len(df_today) > 0:
            try:
                from backtest_engine import backfill_future_returns
                log_info("  5a: 回填未来收益到因子数据...")
                bf_result = backfill_future_returns(hold_days=[1, 3, 5, 10], verbose=True)
                if bf_result and isinstance(bf_result, dict):
                    if bf_result.get("处理文件数", 0) > 0:
                        log_info(f"  回填完成: {bf_result['处理文件数']}文件, {bf_result['更新记录数']}记录")
                        report["未来收益回填"] = bf_result
                    else:
                        log_info(f"  回填: 无需处理 (跳过{bf_result.get('跳过文件数', '?')}文件)")
                else:
                    log_info("  回填: 返回None或异常格式，跳过（不影响进化迭代）")
            except Exception as e:
                log_error(e, "回填异常（不影响进化迭代）")

            try:
                from evolution import run_evolution_cycle
                from stock_picker import FACTOR_WEIGHTS, HOT_CONCEPTS, VERSION
                meta = {
                    "in_session": False, "session_name": "盘后回测迭代",
                    "data_source": "K线缓存", "weights": FACTOR_WEIGHTS,
                    "hot_concepts": HOT_CONCEPTS, "version": VERSION,
                }
                run_evolution_cycle(
                    df_scored=df_today, df_quotes=None,
                    concept_map={}, meta=meta,
                )
                report["进化迭代"] = "完成"
            except Exception as e:
                log_error(e, "进化迭代跳过")
        else:
            log_info("  无今日快照，跳过进化迭代")

        log_info("Step 6: 保存报告")
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = REPORT_DIR / f"回测报告_{ts}.json"
        report_file.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        log_info(f"报告: {report_file}")

        log_info("Step 7: 缓存清理检查")
        _cleanup_cache()

        log_perf("盘后回测迭代完成", _time.time() - start)

    except Exception as e:
        log_error(e, "盘后回测迭代异常")
        raise


if __name__ == "__main__":
    main()
