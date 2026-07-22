#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""盘中数据补充 — 资金流/龙虎榜等盘中数据"""
import sys
from pathlib import Path

SHARED = Path(__file__).parent.parent / "共用模块"
sys.path.insert(0, str(SHARED))

from bug_log import setup_logger, log_info, log_error, log_perf, log_data
setup_logger("盘中数据补充")

import json
import time
from datetime import datetime
from stock_picker import em_get, eastmoney_datacenter, get_now

BASE_DIR = Path(__file__).parent.parent
INTRADAY_FILE = BASE_DIR / "agent_data" / "intraday_data.json"


def fetch_fund_flow(top_codes):
    results = {}
    for code in top_codes[:20]:
        try:
            market = "1" if code.startswith("6") else "0"
            url = "https://push2his.eastmoney.com/api/qt/stock/fflow/daykline/get"
            params = {"secid": f"{market}.{code}", "fields1": "f1,f2,f3",
                      "fields2": "f51,f52,f53,f54,f55,f56", "klt": "101", "lmt": "1"}
            r = em_get(url, params=params, timeout=10)
            data = r.json()
            klines = data.get("data", {}).get("klines", [])
            if klines:
                parts = klines[-1].split(",")
                results[code] = {"主力净流入": float(parts[1]) if len(parts) > 1 else 0}
            time.sleep(0.3)
        except Exception as e:
            log_error(e, f"资金流{code}失败")
    return results


def main():
    log_info("=" * 50)
    log_info("盘中数据补充")
    log_info("=" * 50)

    try:
        history_dir = BASE_DIR / "history"
        today = datetime.now().strftime("%Y%m%d")
        today_files = sorted(history_dir.rglob(f"snapshot_{today}_*.csv"), reverse=True)

        if not today_files:
            log_info("无今日选股结果，跳过")
            return

        import pandas as pd
        df = pd.read_csv(today_files[0])
        top_codes = df["代码"].astype(str).str.zfill(6).head(10).tolist()
        log_info(f"Top10: {', '.join(top_codes)}")

        log_info("抓取资金流向...")
        fund_flow = fetch_fund_flow(top_codes)
        log_data("资金流向", len(fund_flow))

        log_info("抓取龙虎榜...")
        try:
            today_str = datetime.now().strftime("%Y-%m-%d")
            dragon = eastmoney_datacenter("RPT_DAILYBILLBOARD_DETAILSNEW",
                                          columns="ALL", filter_str=f"'{today_str}'", page_size=50)
            dragon = dragon[:20] if dragon else []
            log_data("龙虎榜", len(dragon))
        except Exception as e:
            log_error(e, "龙虎榜失败")
            dragon = []

        intraday_data = {
            "timestamp": datetime.now().isoformat(),
            "fund_flow": fund_flow,
            "dragon_tiger": dragon,
        }
        INTRADAY_FILE.write_text(json.dumps(intraday_data, ensure_ascii=False, indent=2), encoding="utf-8")
        log_info(f"已保存: {INTRADAY_FILE}")
        log_perf("盘中数据补充完成")

    except Exception as e:
        log_error(e, "盘中数据补充异常")
        raise


if __name__ == "__main__":
    main()
