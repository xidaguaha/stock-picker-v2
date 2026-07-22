#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
盘前数据预热 — 8:00-8:10 提前缓存非实时数据
====================================================
竞价阶段(9:21)只需拉实时行情即可，大幅缩短耗时。

预热内容：
  - 全量股票列表（若缓存过期）
  - K线数据（若缓存过期）
  - 财报/估值数据（若缓存过期）
  - 概念映射（若缓存过期）
  - 北向资金（非实时，提前获取）

运行方式：
  由 scheduler.py 在 8:00-8:10 自动调度
  或手动: python 盘前预热.py
"""

import sys
import time
from pathlib import Path
from datetime import datetime, timedelta

BASE_DIR = Path(__file__).parent
COMMON = BASE_DIR / "共用模块"
sys.path.insert(0, str(COMMON))

import pandas as pd
from stock_picker import get_stock_list, DEFAULT_CONFIG, load_config
from kline_fetcher import KlineFetcher

log_file = BASE_DIR / "logs" / "preheat.log"
log_file.parent.mkdir(parents=True, exist_ok=True)


def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def is_cache_fresh(path: Path, ttl_hours: int = 24):
    """检查缓存文件是否在有效期内"""
    if not path.exists():
        return False
    age_hours = (datetime.now() - datetime.fromtimestamp(path.stat().st_mtime)).total_seconds() / 3600
    return age_hours < ttl_hours


def preheat_stock_list():
    """预热股票列表"""
    cache_path = BASE_DIR / "cache" / "stock_list.csv"
    if is_cache_fresh(cache_path, ttl_hours=24):
        log("  股票列表缓存有效，跳过")
        return True
    try:
        stocks = get_stock_list()
        stocks.to_csv(cache_path, index=False, encoding="utf-8-sig")
        log(f"  股票列表: {len(stocks)} 只")
        return True
    except Exception as e:
        log(f"  股票列表失败: {e}")
        return False


def preheat_kline_data(top_n: int = 200):
    """预热TopN股票的K线数据（用于打分，非实时）"""
    cache_dir = BASE_DIR / "cache"
    cache_dir.mkdir(exist_ok=True)

    # 读取股票列表（优先用缓存）
    cache_path = cache_dir / "stock_list.csv"
    if cache_path.exists():
        stocks = pd.read_csv(cache_path)
    else:
        log("  无股票列表缓存，跳过K线预热")
        return False

    # 优先预热昨日Top200（基于市值/流动性）
    # 实际运行时，可以用前一天的选股结果来预热
    # 这里简单预热市值最大的200只
    if "总市值" in stocks.columns:
        top_stocks = stocks.nlargest(top_n, "总市值")
    else:
        top_stocks = stocks.head(top_n)

    kf = KlineFetcher()
    preheated = 0
    failed = 0

    for _, row in top_stocks.iterrows():
        code = str(row["代码"]).zfill(6)
        kline_file = cache_dir / f"kline_{code}.parquet"

        if is_cache_fresh(kline_file, ttl_hours=24):
            preheated += 1
            continue

        try:
            df = kf.get_kline(code, days=60)
            if df is not None and len(df) > 0:
                df.to_parquet(kline_file)
                preheated += 1
            else:
                failed += 1
        except Exception:
            failed += 1

        # 控制频率，避免触发风控
        if _ % 50 == 0:
            time.sleep(2)

    log(f"  K线预热: {preheated} 只成功, {failed} 只失败")
    return failed < preheated * 0.5  # 失败率<50%算成功


def preheat_valuation_data():
    """预热估值数据"""
    cache_path = BASE_DIR / "cache" / "valuation_data.csv"
    if is_cache_fresh(cache_path, ttl_hours=24):
        log("  估值缓存有效，跳过")
        return True

    try:
        from stock_picker import _get_valuation_data
        df = _get_valuation_data()
        if df is not None and len(df) > 0:
            df.to_csv(cache_path, index=False, encoding="utf-8-sig")
            log(f"  估值数据: {len(df)} 只")
            return True
    except Exception as e:
        log(f"  估值数据失败: {e}")
    return False


def preheat_concept_map():
    """预热概念映射"""
    cache_path = BASE_DIR / "data" / "concept_maps" / f"concepts_{datetime.now().strftime('%Y%m%d')}.json"
    if is_cache_fresh(cache_path, ttl_hours=24):
        log("  概念映射缓存有效，跳过")
        return True

    try:
        from stock_picker import _get_concept_map
        concept_map = _get_concept_map()
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        import json
        cache_path.write_text(json.dumps(concept_map, ensure_ascii=False), encoding="utf-8")
        log(f"  概念映射: {len(concept_map)} 只")
        return True
    except Exception as e:
        log(f"  概念映射失败: {e}")
    return False


def preheat_northbound():
    """预热北向资金（非实时，提前获取）"""
    try:
        from stock_picker import hsgt_realtime
        data = hsgt_realtime()
        if data:
            log(f"  北向资金: 净流入 {data.get('净流入', 0):.2f} 亿")
            return True
    except Exception as e:
        log(f"  北向资金失败: {e}")
    return False


def preheat_market_sentiment():
    """预热市场情绪数据"""
    try:
        from sentiment_data import fetch_market_sentiment, fetch_hot_concepts
        sentiment = fetch_market_sentiment()
        hot = fetch_hot_concepts(top_n=5)
        if sentiment:
            log(f"  市场情绪: {sentiment['score']} ({sentiment['trend']})")
        if hot:
            log(f"  热门概念: {', '.join([c['name'] for c in hot])}")
        return True
    except Exception as e:
        log(f"  情绪数据失败: {e}")
    return False


def save_preheat_status(success: bool):
    """保存预热状态，供scheduler检查"""
    status_file = BASE_DIR / "cache" / "preheat_status.json"
    status_file.parent.mkdir(parents=True, exist_ok=True)
    import json
    status = {
        "date": datetime.now().strftime("%Y-%m-%d"),
        "completed": success,
        "timestamp": datetime.now().isoformat(),
    }
    status_file.write_text(json.dumps(status, ensure_ascii=False), encoding="utf-8")


def main():
    log("=" * 50)
    log("盘前数据预热开始")
    log("=" * 50)

    start = time.time()
    results = {}

    # 1. 股票列表
    log("[1/6] 预热股票列表...")
    results["stock_list"] = preheat_stock_list()

    # 2. K线数据
    log("[2/6] 预热K线数据...")
    results["kline"] = preheat_kline_data(top_n=200)

    # 3. 估值数据
    log("[3/6] 预热估值数据...")
    results["valuation"] = preheat_valuation_data()

    # 4. 概念映射
    log("[4/6] 预热概念映射...")
    results["concept"] = preheat_concept_map()

    # 5. 北向资金
    log("[5/6] 预热北向资金...")
    results["northbound"] = preheat_northbound()

    # 6. 市场情绪
    log("[6/6] 预热市场情绪...")
    results["sentiment"] = preheat_market_sentiment()

    elapsed = time.time() - start
    success_count = sum(1 for v in results.values() if v)
    log(f"\n预热完成: {success_count}/6 成功, 耗时 {elapsed:.1f} 秒")

    # 保存状态
    save_preheat_status(success_count >= 4)  # 至少4项成功算预热完成

    return success_count >= 4


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("预热中断")
    except Exception as e:
        log(f"预热异常: {e}")
        import traceback
        traceback.print_exc()
