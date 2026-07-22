#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""盘前数据检查 — 数据源连通 + AI结果验证"""
import sys
from pathlib import Path

SHARED = Path(__file__).parent.parent / "共用模块"
sys.path.insert(0, str(SHARED))

from bug_log import setup_logger, log_info, log_error, log_data
setup_logger("盘前数据检查")

import json
from stock_picker import get_now

BASE_DIR = Path(__file__).parent.parent
AI_RESULT_FILE = BASE_DIR / "agent_data" / "latest_ai_analysis.json"


def check_source(name, url, timeout=10):
    try:
        import requests
        r = requests.get(url, timeout=timeout)
        ok = r.status_code == 200
        status = "OK" if ok else f"HTTP {r.status_code}"
        log_data(name, 1 if ok else 0, ok)
        return ok
    except Exception as e:
        log_error(e, f"{name} 不可达")
        return False


def main():
    log_info("=" * 50)
    log_info("盘前数据检查")
    log_info("=" * 50)

    log_info("数据源连通性:")
    check_source("东方财富", "https://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=1&fs=m:0+t:6")
    check_source("腾讯财经", "http://qt.gtimg.cn/q=sh600519")
    check_source("新浪财经", "https://finance.sina.com.cn/realstock/company/sh600519/nc.shtml")

    log_info("昨日AI分析:")
    if AI_RESULT_FILE.exists():
        try:
            data = json.loads(AI_RESULT_FILE.read_text(encoding="utf-8"))
            count = data.get("analyzed") or len(data.get("results", []))
            ai_date = data.get("date", "?")
            log_data("AI分析文件", count, True)
            log_info(f"  日期: {ai_date}, 分析: {count} 只")
        except Exception as e:
            log_error(e, "AI文件解析失败")
    else:
        log_info("AI分析文件不存在")

    for d in ["agent_data", "agent_data/news", "cache", "logs", "history", "reports"]:
        (BASE_DIR / d).mkdir(parents=True, exist_ok=True)

    log_info("检查完成")


if __name__ == "__main__":
    main()
