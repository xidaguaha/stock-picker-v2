#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
盘后AI分析 — 新闻爬虫 + LLM评分（盘前预热）
职责：
  1. 爬取信源新闻
  2. LLM分析新闻→提取催化剂/利空
  3. 生成AI因子，写入 agent_data/latest_ai_analysis.json
运行：
  由 scheduler.py 在盘后自动调度（或手动: python 盘后AI分析/run.py）
"""
import sys
from pathlib import Path

SHARED = Path(__file__).parent.parent / "共用模块"
sys.path.insert(0, str(SHARED))

from bug_log import setup_logger, log_info, log_error, log_perf
setup_logger("盘后AI分析")

import json
import time as _time
from datetime import datetime, timedelta
from stock_picker import get_now

BASE_DIR = Path(__file__).parent.parent
OUTPUT = BASE_DIR / "agent_data" / "latest_ai_analysis.json"
NEWS_DIR = BASE_DIR / "agent_data" / "news"
NEWS_DIR.mkdir(parents=True, exist_ok=True)


def _fetch_eastmoney_announcements(begin: str, end: str, top_n: int = 20):
    """抓取东方财富公告，过滤掉资管计划"""
    from stock_picker import eastmoney_datacenter
    try:
        df = eastmoney_datacenter("RPT_CUSTOM_STOCK_ANNOUNCEMENT",
                                  columns="SECURITY_CODE,SECURITY_NAME,ANNOUNCEMENT_DATE,ANNOUNCEMENT_TITLE",
                                  filter_str=f"'{begin}~{end}'", page_size=top_n)
        if df is None or len(df) == 0:
            return []
        out = []
        for _, r in df.iterrows():
            title = str(r.get("ANNOUNCEMENT_TITLE", ""))
            if "资管计划" in title or "资产管理" in title:
                continue
            out.append({"code": str(r.get("SECURITY_CODE", "")),
                        "title": title,
                        "date": str(r.get("ANNOUNCEMENT_DATE", "")),
                        "source": "东财公告"})
        return out[:top_n]
    except Exception as e:
        log_error(e, "东财公告失败")
        return []


def _fetch_eastmoney_reports(begin: str, end: str, top_n: int = 20):
    """抓取东财研报（近7天）"""
    from stock_picker import eastmoney_datacenter
    try:
        df = eastmoney_datacenter("RPT_STOCK_RATINGCHANGE", columns="SECURITY_CODE,SECURITY_NAME,RESEARCHER,ORG_NAME,RATING_NAME,RATING_CHANGE,INDUSTRY_CODE",
                                  filter_str=f"'{begin}~{end}'", page_size=top_n)
        if df is None or len(df) == 0:
            return []
        out = []
        for _, r in df.iterrows():
            out.append({"code": str(r.get("SECURITY_CODE", "")),
                        "title": f"{r.get('ORG_NAME','')}评级{r.get('RATING_NAME','')}",
                        "date": str(r.get("RATING_CHANGE", "")),
                        "source": "东财研报"})
        return out[:top_n]
    except Exception as e:
        log_error(e, "东财研报失败")
        return []


def _fetch_sina_sector_news(top_n: int = 20):
    """抓取新浪行业新闻，过滤旧研报（年份 < 当前年-1）"""
    import requests
    from bs4 import BeautifulSoup
    current_year = datetime.now().year
    min_year = current_year - 1
    out = []
    try:
        r = requests.get("https://finance.sina.com.cn/stock/hkstock/ggscyd/",
                         headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
        r.encoding = "utf-8"
        soup = BeautifulSoup(r.text, "html.parser")
        for item in soup.select(".news-item, .list_item")[:top_n]:
            title = item.get_text(strip=True)
            # 过滤旧研报
            import re
            years = re.findall(r'20\d{2}', title)
            if years and int(years[0]) < min_year:
                continue
            if len(title) > 10:
                out.append({"code": "", "title": title, "date": "", "source": "新浪行业"})
    except Exception as e:
        log_error(e, "新浪行业新闻失败")
    return out[:top_n]


def _fetch_ths_bkjj(top_n: int = 10):
    """抓取同花顺板块聚焦新闻"""
    import requests
    from bs4 import BeautifulSoup
    out = []
    try:
        r = requests.get("https://basic.10jqka.com.cn/api/stockph/bkjj/",
                         headers={"User-Agent": "Mozilla/5.0", "Referer": "https://basic.10jqka.com.cn/"},
                         timeout=15)
        data = r.json()
        for item in data.get("data", [])[:top_n]:
            out.append({"code": str(item.get("code", "")),
                        "title": str(item.get("title", "")),
                        "date": str(item.get("time", "")),
                        "source": "同花顺板块"})
    except Exception as e:
        log_error(e, "同花顺板块聚焦失败")
    return out[:top_n]


def _fetch_longhubang(top_n: int = 15):
    """抓取同花顺龙虎榜"""
    import requests
    from bs4 import BeautifulSoup
    out = []
    try:
        r = requests.get("https://data.10jqka.com.cn/rank/longhu/",
                         headers={"User-Agent": "Mozilla/5.0", "Referer": "https://data.10jqka.com.cn/"},
                         timeout=15)
        r.encoding = "utf-8"
        soup = BeautifulSoup(r.text, "html.parser")
        rows = soup.select(".J-ajax-table tbody tr")
        for row in rows[:top_n]:
            tds = row.find_all("td")
            if len(tds) >= 3:
                code = tds[0].get_text(strip=True)
                name = tds[1].get_text(strip=True)
                reason = tds[2].get_text(strip=True) if len(tds) > 2 else ""
                out.append({"code": code, "title": f"龙虎榜: {name} {reason}",
                            "date": datetime.now().strftime("%Y-%m-%d"), "source": "龙虎榜"})
    except Exception as e:
        log_error(e, "龙虎榜失败")
    return out[:top_n]


def fetch_all_news():
    today = datetime.now().strftime("%Y-%m-%d")
    begin = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")

    announcements = _fetch_eastmoney_announcements(begin, today, top_n=20)
    reports = _fetch_eastmoney_reports(begin, today, top_n=20)
    sina_news = _fetch_sina_sector_news(top_n=20)
    ths_bkjj = _fetch_ths_bkjj(top_n=10)
    longhu = _fetch_longhubang(top_n=15)

    news = announcements + reports + sina_news + ths_bkjj + longhu
    return news


def main():
    log_info("=" * 50)
    log_info("盘后AI分析")
    log_info("=" * 50)

    start = _time.time()

    log_info("抓取新闻...")
    news = fetch_all_news()
    log_info(f"新闻: {len(news)} 条")

    today_str = datetime.now().strftime("%Y-%m-%d")
    news_file = NEWS_DIR / f"{today_str}.jsonl"
    with open(news_file, "w", encoding="utf-8") as f:
        for item in news:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")

    log_info("调用AI分析...")
    try:
        from stock_ai_analysis import analyze_stock
        results = []
        for item in news[:20]:
            code = item.get("code", "")
            if not code:
                continue
            try:
                ai_result = analyze_stock(code, item.get("title", ""))
                results.append({"code": code, "title": item.get("title", ""),
                                "ai": ai_result, "date": today_str})
            except Exception as e:
                log_error(e, f"AI分析 {code} 失败")

        output = {"date": today_str, "analyzed": len(results), "results": results}
        OUTPUT.parent.mkdir(parents=True, exist_ok=True)
        OUTPUT.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
        log_info(f"AI分析完成: {len(results)} 只 → {OUTPUT}")

    except ImportError as e:
        log_error(e, "AI分析模块未就绪")
    except Exception as e:
        log_error(e, "AI分析异常")

    log_perf("盘后AI分析完成", _time.time() - start)


if __name__ == "__main__":
    main()
