#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""盘中模拟交易 — 按今日Top3模拟买入，不重新选股，不推送"""
import sys
from pathlib import Path

SHARED = Path(__file__).parent.parent / "共用模块"
sys.path.insert(0, str(SHARED))

from bug_log import setup_logger, log_info, log_error, log_perf
setup_logger("盘中模拟交易")

import time as _time
import pandas as pd
from datetime import datetime
from stock_picker import get_now

BASE_DIR = Path(__file__).parent.parent


def main():
    log_info("=" * 50)
    log_info("盘中模拟交易 — 按今日Top3模拟买入")
    log_info("=" * 50)

    start = _time.time()
    try:
        from paper_trader import open_positions, update_positions, format_trade_report, TRADES_DIR

        today_str = datetime.now().strftime("%Y-%m-%d")
        bought_marker = TRADES_DIR / f".bought_{today_str}"
        if bought_marker.exists():
            log_info(f"今日({today_str})已买入，跳过")
            return

        log_info("更新持仓")
        update_positions()

        history_dir = BASE_DIR / "history"
        today = datetime.now().strftime("%Y%m%d")
        today_files = sorted(history_dir.rglob(f"snapshot_{today}_*.csv"), reverse=True)

        if not today_files:
            log_info("今日无选股结果，跳过")
            return

        df = pd.read_csv(today_files[0])
        log_info(f"读取选股结果: {today_files[0].name} ({len(df)} 只)")

        from config_loader import load_config
        cfg = load_config()
        top_n = cfg.get("top_n", 3)
        buy_count = open_positions(df.head(top_n))
        if buy_count > 0:
            bought_marker.touch(exist_ok=True)
            log_info(f"模拟买入Top{top_n}: 买入 {buy_count} 只")
        else:
            log_info(f"模拟买入Top{top_n}: 无新买入")

        report = format_trade_report()
        print(report)

        log_perf("盘中模拟交易完成", _time.time() - start)

    except ImportError as e:
        log_error(e, "模拟交易模块未就绪")
    except Exception as e:
        log_error(e, "模拟交易异常")
        raise


if __name__ == "__main__":
    main()
