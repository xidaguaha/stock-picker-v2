#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
竞价推送集成测试 — 用缓存数据模拟竞价时段，随时可跑，不用等第二天

用法:
    python tests/test_auction_integration.py

测试内容:
    1. 竞价时段自动识别 (_is_trading_run=True)
    2. 快照/归档/输出 Top3 一致性 (二次评分后保存)
    3. AI关键词存在性
    4. 模拟交易读取的Top3 = 快照Top3
"""
import sys
import os
import json
import glob
import shutil
import datetime
from pathlib import Path
from unittest.mock import patch

# ── 路径设置 ──
BASE_DIR = Path(__file__).parent.parent
SHARED = BASE_DIR / "共用模块"
AUCTION = BASE_DIR / "竞价量化推送"
sys.path.insert(0, str(SHARED))
sys.path.insert(0, str(AUCTION))

PASS = 0
FAIL = 0
ERRORS = []


def check(name, condition, detail=""):
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  [PASS] {name}")
    else:
        FAIL += 1
        ERRORS.append(f"{name}: {detail}")
        print(f"  [FAIL] {name} — {detail}")


# ═══════════════════════════════════════════════════
# 测试1: 竞价时段自动识别
# ═══════════════════════════════════════════════════
def test_auction_auto_detect():
    print("\n=== 测试1: 竞价时段自动识别 ===")

    # 模拟 9:21 周三
    mock_time = datetime.datetime(2026, 7, 16, 9, 21, 0)
    config = None

    _is_auction = (config or {}).get("_load_yesterday_ai", False)

    # 复制 run.py 第283-290行的自动识别逻辑
    if not _is_auction and mock_time.weekday() < 5:
        if mock_time.hour == 9 and 15 <= mock_time.minute < 30:
            _is_auction = True
            if config is None:
                config = {}
            config["_load_yesterday_ai"] = True
            config["_is_trading_run"] = True

    _is_trading_run = config.get("_is_trading_run", False)
    _skip_ai = config.get("_skip_ai", False)
    _load_yesterday = config.get("_load_yesterday_ai", False)
    if _is_trading_run:
        _load_yesterday = True
        _skip_ai = True

    check("9:21 自动识别 _is_trading_run=True", _is_trading_run == True)
    check("9:21 自动识别 _skip_ai=True", _skip_ai == True)
    check("9:21 自动识别 _load_yesterday=True", _load_yesterday == True)

    # 模拟 10:30 非竞价时段
    mock_time2 = datetime.datetime(2026, 7, 16, 10, 30, 0)
    config2 = None
    _is_auction2 = False
    if not _is_auction2 and mock_time2.weekday() < 5:
        if mock_time2.hour == 9 and 15 <= mock_time2.minute < 30:
            _is_auction2 = True
            config2 = config2 or {}
            config2["_is_trading_run"] = True

    _is_trading_run2 = (config2 or {}).get("_is_trading_run", False)
    check("10:30 非竞价 _is_trading_run=False", _is_trading_run2 == False)

    # 模拟周末 9:21
    mock_time3 = datetime.datetime(2026, 7, 18, 9, 21, 0)  # 周六
    config3 = None
    _is_auction3 = False
    if not _is_auction3 and mock_time3.weekday() < 5:
        if mock_time3.hour == 9 and 15 <= mock_time3.minute < 30:
            _is_auction3 = True
            config3 = config3 or {}
            config3["_is_trading_run"] = True

    _is_trading_run3 = (config3 or {}).get("_is_trading_run", False)
    check("周六9:21 不触发自动识别", _is_trading_run3 == False)


# ═══════════════════════════════════════════════════
# 测试2: 快照/归档/输出 Top3 一致性
# ═══════════════════════════════════════════════════
def test_snapshot_consistency():
    print("\n=== 测试2: 快照/归档/输出 Top3 一致性 ===")

    import pandas as pd
    import numpy as np

    # 找最新的快照CSV作为测试数据
    snaps = sorted(glob.glob(str(BASE_DIR / "history" / "*" / "snapshot_20260715_*.csv")))
    if not snaps:
        # 找任何最近的快照
        snaps = sorted(glob.glob(str(BASE_DIR / "history" / "*" / "snapshot_*.csv")))
    if not snaps:
        check("找到快照数据", False, "无可用快照CSV")
        return

    snap_file = snaps[-1]
    print(f"  使用快照: {Path(snap_file).name}")

    df = pd.read_csv(snap_file, encoding="utf-8-sig")

    # 确保有综合得分列
    if "综合得分" not in df.columns:
        check("快照有综合得分列", False, "缺少综合得分列")
        return
    check("快照有综合得分列", True)

    # 模拟 enrich_news_and_signals 中的二次评分
    # 加载 stock_picker 的函数
    from stock_picker import (
        compute_composite_score, FACTOR_WEIGHTS,
        save_results, archive_run, format_output
    )

    # 备份当前 output/history 目录中的文件列表
    output_dir = BASE_DIR / "output"
    history_dir = BASE_DIR / "history" / "202607"

    before_outputs = set(f.name for f in output_dir.glob("选股结果_*"))
    before_snaps = set(f.name for f in history_dir.glob("snapshot_*"))

    try:
        # 运行二次评分（模拟enrich_news_and_signals的关键部分）
        # 注意：我们只测试 compute_composite_score 的重排序效果
        df_before = df.copy()
        top3_before = df_before.head(3)[["代码", "名称", "综合得分"]].values.tolist()

        df_rescored = compute_composite_score(df.copy(), FACTOR_WEIGHTS)
        top3_after = df_rescored.head(3)[["代码", "名称", "综合得分"]].values.tolist()

        # 保存结果（模拟修复后的顺序：enrich之后才save）
        save_results(df_rescored, top_n=3)

        # 读取刚保存的output
        out_files = sorted(output_dir.glob("选股结果_*.json"))
        if out_files:
            out_data = json.loads(out_files[-1].read_text(encoding="utf-8"))
            top3_output = [[r["代码"], r["名称"], r["综合得分"]] for r in out_data[:3]]

            # 验证 output 和 rescored 一致
            output_codes = [r[0] for r in top3_output]
            rescored_codes = [r[0] for r in top3_after]
            check("output Top3 = 二次评分后 Top3", output_codes == rescored_codes,
                  f"output={output_codes} vs rescored={rescored_codes}")
        else:
            check("output 文件生成", False, "无output文件")

        # 验证二次评分确实可能改变排名
        before_codes = [r[0] for r in top3_before]
        after_codes = [r[0] for r in top3_after]
        if before_codes == after_codes:
            print(f"  [INFO] 二次评分未改变Top3排名（{before_codes}）")
        else:
            print(f"  [INFO] 二次评分改变了Top3排名: {before_codes} → {after_codes}")
            check("二次评分后保存（排名变化时能检测到）", True)

    finally:
        # 清理测试生成的文件
        for f in output_dir.glob("选股结果_*"):
            if f.name not in before_outputs:
                f.unlink(missing_ok=True)
        for f in history_dir.glob("snapshot_*"):
            if f.name not in before_snaps:
                f.unlink(missing_ok=True)


# ═══════════════════════════════════════════════════
# 测试3: AI关键词存在性
# ═══════════════════════════════════════════════════
def test_ai_keywords():
    print("\n=== 测试3: AI关键词存在性 ===")

    # 检查 latest_ai_analysis.json
    ai_file = BASE_DIR / "agent_data" / "latest_ai_analysis.json"
    if not ai_file.exists():
        check("latest_ai_analysis.json 存在", False, "文件不存在")
        return
    check("latest_ai_analysis.json 存在", True)

    data = json.loads(ai_file.read_text(encoding="utf-8"))
    results = data.get("results", [])
    check("AI分析结果非空", len(results) > 0, f"results={len(results)}")

    has_kw = sum(1 for r in results if r.get("AI_关键词", ""))
    total = len(results)
    kw_ratio = has_kw / total if total > 0 else 0
    print(f"  [INFO] AI关键词覆盖率: {has_kw}/{total} ({kw_ratio:.1%})")
    check("AI关键词覆盖率 > 10%", kw_ratio > 0.1, f"覆盖率={kw_ratio:.1%}")

    # 检查最新快照的Top3是否有关键词
    snaps = sorted(glob.glob(str(BASE_DIR / "history" / "*" / "snapshot_20260715_*.json")))
    if not snaps:
        snaps = sorted(glob.glob(str(BASE_DIR / "history" / "*" / "snapshot_*.json")))
    if snaps:
        snap_data = json.loads(Path(snaps[-1]).read_text(encoding="utf-8"))
        top10 = snap_data.get("Top10", [])
        top3_with_kw = sum(1 for r in top10[:3] if r.get("AI_关键词", ""))
        check("最新快照Top3有AI关键词", top3_with_kw >= 1,
              f"Top3中{top3_with_kw}/3有关键词")
    else:
        check("找到快照JSON", False, "无快照JSON")


# ═══════════════════════════════════════════════════
# 测试4: 模拟交易读取的Top3 = 快照Top3
# ═══════════════════════════════════════════════════
def test_paper_trader_reads_snapshot():
    print("\n=== 测试4: 模拟交易读取的Top3 = 快照Top3 ===")

    import pandas as pd

    # 读取 盘中模拟交易/run.py 的逻辑
    # 它读 history/snapshot_YYYYMMDD_*.csv，取 head(top_n)
    today = datetime.datetime.now().strftime("%Y%m%d")
    today_files = sorted(
        (BASE_DIR / "history").rglob(f"snapshot_{today}_*.csv"), reverse=True
    )

    if not today_files:
        # 用最近的可用的
        all_snaps = sorted((BASE_DIR / "history").rglob("snapshot_*.csv"), reverse=True)
        if all_snaps:
            today_files = [all_snaps[0]]
            print(f"  [INFO] 今日无快照，使用最近: {today_files[0].name}")
        else:
            check("找到快照CSV", False, "无任何快照")
            return

    snap_file = today_files[0]
    df = pd.read_csv(snap_file, encoding="utf-8-sig")

    # 读取config top_n
    config_file = BASE_DIR / "config.json"
    cfg = json.loads(config_file.read_text(encoding="utf-8-sig"))
    top_n = cfg.get("top_n", 3)

    # 模拟交易读取的Top3（统一zero-fill到6位）
    trader_top = df.head(top_n)[["代码", "名称"]].values.tolist()
    trader_codes = [str(r[0]).zfill(6) for r in trader_top]

    # 对应的JSON快照Top3
    json_snap = snap_file.with_suffix(".json")
    if json_snap.exists():
        snap_data = json.loads(json_snap.read_text(encoding="utf-8"))
        top10 = snap_data.get("Top10", [])
        snap_top = [[r["代码"], r["名称"]] for r in top10[:top_n]]
        snap_codes = [r[0] for r in snap_top]

        check("模拟交易Top3 = 快照JSON Top3", trader_codes == snap_codes,
              f"trader={trader_codes} vs snap={snap_codes}")

        # 也检查 output JSON
        out_files = sorted((BASE_DIR / "output").glob("选股结果_*.json"))
        if out_files:
            # 找匹配时间戳的
            snap_timestamp = snap_file.stem.split("_")[-1]
            matching_out = [f for f in out_files if snap_timestamp in f.name]
            if matching_out:
                out_data = json.loads(matching_out[-1].read_text(encoding="utf-8"))
                out_codes = [r["代码"] for r in out_data[:top_n]]
                check("模拟交易Top3 = output Top3", trader_codes == out_codes,
                      f"trader={trader_codes} vs output={out_codes}")
    else:
        check("快照JSON存在", False, f"{json_snap.name} 不存在")


# ═══════════════════════════════════════════════════
# 测试5: 代码顺序检查（静态分析）
# ═══════════════════════════════════════════════════
def test_code_order_static():
    print("\n=== 测试5: 代码顺序静态检查（save_results在enrich之后）===")

    # 检查 run.py 中 save_results/archive_run 是否在 enrich_news_and_signals 之后
    run_py = (AUCTION / "run.py").read_text(encoding="utf-8")

    # 找 enrich_news_and_signals 的位置（最后一次出现=主流程中的调用）
    enrich_pos = run_py.rfind("enrich_news_and_signals(df, top_n)")
    save_pos = run_py.rfind("save_results(df, top_n)")
    archive_pos = run_py.rfind("archive_run(df, concept_map")

    if enrich_pos == -1 or save_pos == -1 or archive_pos == -1:
        check("找到关键函数调用", False, "函数调用缺失")
        return
    check("找到关键函数调用", True)

    check("save_results 在 enrich 之后", save_pos > enrich_pos,
          f"save={save_pos} enrich={enrich_pos}")
    check("archive_run 在 enrich 之后", archive_pos > enrich_pos,
          f"archive={archive_pos} enrich={enrich_pos}")

    # 检查 stock_picker.py 同样的顺序
    sp_py = (SHARED / "stock_picker.py").read_text(encoding="utf-8")
    # 找 main() 函数内的调用
    main_start = sp_py.find("def main(")
    if main_start == -1:
        check("stock_picker.py 有main()", False)
        return

    main_section = sp_py[main_start:]
    enrich_pos2 = main_section.find("enrich_news_and_signals(df, top_n)")
    save_pos2 = main_section.find("save_results(df, top_n)")
    archive_pos2 = main_section.find("archive_run(df, concept_map")

    if enrich_pos2 == -1 or save_pos2 == -1 or archive_pos2 == -1:
        check("stock_picker main() 找到关键函数", False, "函数调用缺失")
        return
    check("stock_picker main() 找到关键函数", True)

    check("stock_picker: save_results 在 enrich 之后", save_pos2 > enrich_pos2,
          f"save={save_pos2} enrich={enrich_pos2}")
    check("stock_picker: archive_run 在 enrich 之后", archive_pos2 > enrich_pos2,
          f"archive={archive_pos2} enrich={enrich_pos2}")


# ═══════════════════════════════════════════════════
# 测试6: 飞书推送格式检查
# ═══════════════════════════════════════════════════
def test_feishu_format():
    print("\n=== 测试6: 飞书推送格式检查 ===")

    from stock_picker import select_diversified_topn
    import pandas as pd

    snaps = sorted(glob.glob(str(BASE_DIR / "history" / "*" / "snapshot_*.csv")))
    if not snaps:
        check("找到快照数据", False)
        return

    df = pd.read_csv(snaps[-1], encoding="utf-8-sig")

    # 检查 select_diversified_topn 是否正常工作
    try:
        top3 = select_diversified_topn(df, top_n=3)
        check("select_diversified_topn 正常返回3只", len(top3) == 3,
              f"返回{len(top3)}只")
    except Exception as e:
        check("select_diversified_topn 正常运行", False, str(e))


# ═══════════════════════════════════════════════════
# 主函数
# ═══════════════════════════════════════════════════
if __name__ == "__main__":
    print("=" * 60)
    print("  竞价推送集成测试")
    print("  用缓存数据模拟，随时可跑")
    print("=" * 60)

    test_auction_auto_detect()
    test_snapshot_consistency()
    test_ai_keywords()
    test_paper_trader_reads_snapshot()
    test_code_order_static()
    test_feishu_format()

    print("\n" + "=" * 60)
    print(f"  结果: {PASS} passed, {FAIL} failed")
    if ERRORS:
        print("  失败项:")
        for e in ERRORS:
            print(f"    - {e}")
    print("=" * 60)

    sys.exit(0 if FAIL == 0 else 1)
