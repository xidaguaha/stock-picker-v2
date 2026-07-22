#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AI 分析模块 v1.0 — 自动化外围信息搜集与评分
=======================================================
职责：
  1. 读取初筛股票池（agent_input.json 或由程序直接传入）
  2. 对每只股票，通过 AI API 搜索外围信息：
     - 行业定位 & 细分赛道
     - 核心产品 & 下游应用
     - 近期催化剂（事件/政策/订单）
     - 利空因素（竞争/政策风险/业绩风险）
     - 机构动态（北向/基金/龙虎榜）
  3. AI 给出综合评分（0-100）和投资逻辑摘要
  4. 结果写入 stock_ai_analysis.json，供主程序合并

支持两种 AI 调用方式：
  A. OpenClaw 本地 API（推荐）：直接调用本机 OpenClaw Gateway
  B. 大模型 API（OpenAI/DeepSeek/通义等）：在 config.json 配置 api_key

配置项（config.json）：
  "ai_analysis": {
    "enabled": true,
    "provider": "openclaw",          // "openclaw" | "openai" | "deepseek" | "custom"
    "api_key": "",                    // 非空时使用对应API
    "api_base": "",                   // 自定义endpoint
    "model": "qclaw/pool-hy3-preview",
    "score_weight": 0.25,            // AI评分在综合得分中权重
    "max_concurrent": 3,              // 并发分析数量
    "cache_hours": 24                // 相同股票24小时内不重复分析
  }
"""

import json
import os
import time
import requests
import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

# ============================================================
#  路径配置
# ============================================================
BASE_DIR = Path(__file__).parent.parent  # 指向项目根目录
# AGENT_DIR 必须指向项目根目录的 agent_data（与盘后AI分析/run.py 和盘前数据检查一致）
AGENT_DIR = BASE_DIR / "agent_data"
AGENT_DIR.mkdir(parents=True, exist_ok=True)

ANALYSIS_FILE = AGENT_DIR / "stock_ai_analysis.json"
CACHE_FILE = AGENT_DIR / "ai_analysis_cache.json"

# OpenClaw Gateway 默认地址
OPENCLAW_GATEWAY_URL = "http://127.0.0.1:3000"


def log(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"  [{ts}] [AIAnalysis] {msg}")


# ============================================================
#  配置读取
# ============================================================

def load_ai_config():
    """从 config.json 加载 AI 分析配置，缺失项填默认值"""
    # 查找config.json: 项目根目录
    root_cfg = BASE_DIR / "config.json"

    default = {
        "enabled": True,
        "provider": "openclaw",
        "api_key": "",
        "api_base": "",
        "model": "qclaw/pool-hy3-preview",
        "score_weight": 0.25,
        "max_concurrent": 3,
        "cache_hours": 24,
        "timeout_seconds": 60,
        "openclaw_gateway": "http://127.0.0.1:3000",
    }

    # 读项目根目录的 config.json
    cfg_path = root_cfg
    if not cfg_path.exists():
        return default
    try:
        cfg = json.loads(cfg_path.read_text(encoding="utf-8-sig"))
        ai_cfg = cfg.get("ai_analysis", {})
        for k, v in default.items():
            if k not in ai_cfg:
                ai_cfg[k] = v
        return ai_cfg
    except Exception as e:
        log(f"配置读取失败: {e}，使用默认配置")
        return default


# ============================================================
#  缓存管理
# ============================================================

def _cache_key(code: str, name: str) -> str:
    return hashlib.md5(f"{code}_{name}".encode()).hexdigest()[:16]


def load_cache(cfg: dict) -> dict:
    if not CACHE_FILE.exists():
        return {}
    try:
        cache = json.loads(CACHE_FILE.read_text(encoding="utf-8"))
        now = datetime.now()
        today_str = now.strftime("%Y-%m-%d")

        # 检查缓存中是否有今天的数据
        has_today = False
        for v in cache.values():
            ts_str = v.get("_cached_at", "")
            if ts_str.startswith(today_str):
                has_today = True
                break

        if has_today:
            # 今天已分析过：保留今天的，清除昨天及之前的
            valid = {}
            for k, v in cache.items():
                ts_str = v.get("_cached_at", "")
                if ts_str.startswith(today_str):
                    valid[k] = v
            return valid
        else:
            # 今天第一次运行：清空所有旧缓存
            return {}
    except Exception:
        return {}


def save_cache(cache: dict):
    try:
        CACHE_FILE.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as e:
        log(f"缓存保存失败: {e}")

def load_ai_cache() -> dict:
    """
    加载 AI 分析缓存，返回简化格式供 compute_factors() 使用。
    返回: {"code_name": {"ai_score": 50}, ...}
    """
    cfg = load_ai_config()
    raw = load_cache(cfg)
    simplified = {}
    for k, v in raw.items():
        code = v.get("code", "")
        name = v.get("name", "")
        if code and name:
            key = f"{code}_{name}"
            ai_score = v.get("AI评分", {})
            score = ai_score.get("总分", 50) if isinstance(ai_score, dict) else 50
            simplified[key] = {"ai_score": score}
    return simplified


def fetch_stock_news(code: str, name: str, days: int = 3) -> str:
    """
    多源抓取个股新闻，交叉验证。
    来源1: 新浪财经个股新闻页（主力，零鉴权）
    来源2: 东财公告（公司公告/重大事项）
    来源3: 东财研报（机构研报/评级）
    返回: 分号分隔的新闻标题（最多 800 字）
    """
    import requests as _req
    import re as _re
    import json as _json
    import time as _time
    from pathlib import Path as _Path
    from datetime import datetime as _dt, timedelta as _td
    
    code_str = str(code).zfill(6)
    cache_key = f"{code_str}_{name}"
    today = _dt.now().strftime("%Y-%m-%d")
    cache_file = AGENT_DIR / "news" / f"{today}.jsonl"
    cache_file.parent.mkdir(parents=True, exist_ok=True)
    err_log = AGENT_DIR / "news_errors.log"

    def _log(msg):
        """写错误日志到文件（UTF-8 编码）"""
        try:
            with open(err_log, "a", encoding="utf-8") as lf:
                lf.write(f"[{_dt.now().strftime('%Y-%m-%d %H:%M:%S')}] {code_str}({name}): {msg}\n")
        except Exception as e:
            log(f"操作异常: {e}", "WARN")

    # 1. 内存缓存（1小时内同一只股票不重复抓）
    if not hasattr(fetch_stock_news, "_mem"):
        fetch_stock_news._mem = {}
    _mem_key = f"{code_str}_{today}"
    if _mem_key in fetch_stock_news._mem:
        log(f"  新闻内存缓存命中: {name}")
        return fetch_stock_news._mem[_mem_key][:800]

    # 2. 磁盘缓存（JSONL，当天有效）
    if cache_file.exists():
        try:
            for line in cache_file.read_text(encoding="utf-8").strip().split("\n"):
                if line.strip():
                    row = _json.loads(line)
                    if row.get("code") == code_str:
                        cached_news = "；".join([n.get("title","") for n in row.get("news", [])])[:800]
                        if cached_news:
                            log(f"  新闻磁盘缓存命中: {name}, {len(row.get('news',[]))}条")
                            fetch_stock_news._mem[_mem_key] = cached_news
                            return cached_news
        except Exception as e:
            _log(f"磁盘缓存读取失败: {e}")

    ALL = []
    errors = []

    # ── 来源1: 新浪财经个股新闻页 ──────────────────────────────────
    try:
        market = "sh" if code_str.startswith("6") else "sz"
        url = f"https://finance.sina.com.cn/realstock/company/{market}{code_str}/nc.shtml"
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        r = _req.get(url, headers=headers, timeout=10)
        r.encoding = "gbk"
        html = r.text
        titles = _re.findall(r'<a[^>]+title="([^"]{10,200})"[^>]*>', html)
        skip_words = ['随身自选股', '融资融券标的', 'MSCI成分', '模拟交易', '沪股通标的', '深股通标的', 'Level-2', 'level2', '股市雷达']
        real_news = [t for t in titles if len(t) > 15 and not any(s in t for s in skip_words)]

        # 过滤旧年份研报：新浪个股页混合展示历史研报，需排除2年前的标题
        _cur_year = _dt.now().year
        def _is_old_research(title):
            """标题中包含2年前的年份（如2024年及以前），判定为旧研报"""
            years_found = _re.findall(r'(20\d{2})年', title)
            for y in years_found:
                try:
                    if int(y) < _cur_year - 1:
                        return True
                except ValueError:
                    pass
            return False

        real_news = [t for t in real_news if not _is_old_research(t)]
        for t in real_news[:days * 5]:
            ALL.append({"title": t.strip(), "source": "新浪财经"})
        _log(f"来源1(新浪) 抓到 {len(real_news[:days*5])} 条 (过滤旧研报后)")
    except Exception as e:
        errors.append(f"新浪失败: {e}")
        _log(f"来源1(新浪) 失败: {e}")

    # ── 来源2: 东财公告（公司公告/研报/重大事项）────────────────
    try:
        url2 = "https://np-anotice-stock.eastmoney.com/api/security/ann"
        r2 = _req.get(
            url2,
            params={"sr": "-1", "page_size": "10", "page_index": "1",
                    "client_source": "web", "stock_list": code_str},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=10,
        )
        d2 = r2.json()
        # 过滤无关的券商资管计划公告（API会返回与个股无关的基金/资管公告）
        _skip_announcement = [
            '集合资产管理计划', '业绩报酬计提', '合同条款变更',
            '变更合同条款备案', '分红预告公告', '资产管理计划',
        ]
        _ann_count = 0
        for item in (d2.get("data", {}).get("list") or []):
            tt = item.get("title", "")
            if tt and not any(s in tt for s in _skip_announcement):
                ALL.append({"title": tt.strip(), "source": "东财公告"})
                _ann_count += 1
        _log(f"来源2(东财公告) 抓到 {_ann_count} 条 (过滤资管公告后)")
    except Exception as e:
        errors.append(f"东财公告失败: {e}")
        _log(f"来源2(东财公告) 失败: {e}")

    # ── 来源3: 东财研报（机构研报/评级）──────────────────────
    try:
        url3 = "https://reportapi.eastmoney.com/report/list"
        _begin = (_dt.now() - _td(days=7)).strftime("%Y-%m-%d")
        _end = _dt.now().strftime("%Y-%m-%d")
        r3 = _req.get(
            url3,
            params={"industryCode": "*", "pageSize": "5", "industry": "*",
                    "rating": "*", "ratingChange": "*",
                    "beginTime": _begin, "endTime": _end,
                    "pageNo": "1", "fields": "", "qType": "0",
                    "orgCode": "", "code": code_str},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=10,
        )
        d3 = r3.json()
        for art in (d3.get("data") or []):
            tt = art.get("title", "")
            if tt:
                ALL.append({"title": tt.strip(), "source": "东财研报"})
        _log(f"来源3(东财研报) 抓到 {len([n for n in ALL if n['source']=='东财研报'])} 条 (近7天)")
    except Exception as e:
        errors.append(f"东财研报失败: {e}")
        _log(f"来源3(东财研报) 失败: {e}")

    # ── 去重（按标题）──
    seen = set()
    unique = []
    for n in ALL:
        if n["title"] not in seen:
            seen.add(n["title"])
            unique.append(n)

    _log(f"合计去重后 {len(unique)} 条 | 失败来源: {len(errors)} | {errors if errors else '全部成功'}")

    # ── 写缓存文件（JSONL，每天一个文件，7天自动清理）────────
    try:
        with open(cache_file, "a", encoding="utf-8") as cf:
            cf.write(_json.dumps({"code": code_str, "name": name, "news": unique[:days * 5], "ts": _dt.now().isoformat()}, ensure_ascii=False) + "\n")
        # 清理7天前的缓存文件
        for fn in cache_file.parent.iterdir():
            if fn.is_file() and (_dt.now().timestamp() - fn.stat().st_mtime) > 7 * 86400:
                try:
                    fn.unlink()
                except Exception as e:
                    log(f"操作异常: {e}", "WARN")
    except Exception as e:
        _log(f"写缓存失败: {e}")

    result = "；".join([n["title"] for n in unique[:days * 5]])
    fetch_stock_news._mem[_mem_key] = result
    return result[:800]



# ============================================================
#  AI 调用：OpenClaw Gateway
# ============================================================

def _call_openclaw(messages: list, cfg: dict) -> Optional[str]:
    """
    调用 OpenClaw Gateway 的聊天接口。
    OpenClaw Gateway 兼容 OpenAI Chat Completions 格式。
    """
    gateway_url = cfg.get("openclaw_gateway", OPENCLAW_GATEWAY_URL)
    api_key = cfg.get("api_key", "")
    # 支持 credentials 字段 / ${ENV} 占位符 / 环境变量回退
    if not api_key:
        api_key = cfg.get("credentials", {}).get("siliconflow_api_key", "")
    if isinstance(api_key, str) and api_key.startswith("${") and api_key.endswith("}"):
        api_key = os.environ.get(api_key[2:-1], "")
    if not api_key:
        api_key = os.environ.get("SILICONFLOW_API_KEY", "")

    # 尝试几个可能的 endpoint
    endpoints = [
        f"{gateway_url}/api/v1/chat/completions",
        f"{gateway_url}/v1/chat/completions",
        f"{gateway_url}/chat/completions",
    ]

    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    payload = {
        "model": cfg.get("model", "qclaw/pool-hy3-preview"),
        "messages": messages,
        "temperature": 0.3,
        "max_tokens": 1500,
    }

    for ep in endpoints:
        for attempt in range(2):
            try:
                resp = requests.post(ep, headers=headers, json=payload, timeout=cfg.get("timeout_seconds", 60))
                if resp.status_code == 200:
                    data = resp.json()
                    return data["choices"][0]["message"]["content"]
            except Exception as e:
                log(f"  OpenClaw endpoint {ep} 第{attempt+1}次失败: {e}")
                if attempt < 1:
                    time.sleep(1 + attempt)
            continue

    # 如果所有 endpoint 都失败，尝试直接 HTTP 请求 OpenClaw 的内部接口
    log("  所有 OpenClaw endpoint 均失败，尝试 sessions_send...")
    return None


def _call_openai_compatible(messages: list, cfg: dict) -> Optional[str]:
    """调用 OpenAI 兼容接口（DeepSeek/通义/自定义等）"""
    api_base = cfg.get("api_base", "")
    api_key = cfg.get("api_key", "")
    # 支持 credentials 字段 / ${ENV} 占位符 / 环境变量回退
    if not api_key:
        api_key = cfg.get("credentials", {}).get("siliconflow_api_key", "")
    if isinstance(api_key, str) and api_key.startswith("${") and api_key.endswith("}"):
        api_key = os.environ.get(api_key[2:-1], "")
    if not api_key:
        api_key = os.environ.get("SILICONFLOW_API_KEY", "")
    model = cfg.get("model", "gpt-4o")

    if not api_base:
        # 默认 OpenAI
        api_base = "https://api.openai.com/v1"

    url = f"{api_base.rstrip('/')}/chat/completions"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    payload = {
        "model": model,
        "messages": messages,
        "temperature": 0.3,
        "max_tokens": 1500,
    }
    for attempt in range(3):
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=cfg.get("timeout_seconds", 60))
            if resp.status_code == 200:
                try:
                    data = resp.json()
                    return data["choices"][0]["message"]["content"]
                except (KeyError, IndexError, TypeError) as e:
                    log(f"  API 返回异常结构: {e}, 内容: {resp.text[:200]}")
                    return None
            else:
                log(f"  API 返回 {resp.status_code}: {resp.text[:200]}")
                if 400 <= resp.status_code < 500:
                    return None  # 4xx 客户端错误，不重试
        except Exception as e:
            log(f"  API 调用第{attempt+1}次异常: {e}")
            if attempt < 2:
                time.sleep(2 ** attempt)
    return None


def call_ai(messages: list, cfg: dict) -> Optional[str]:
    """
    统一 AI 调用入口，支持多模型 fallback。
    依次尝试: 主模型 → fallback_models 列表
    """
    # 尝试主模型
    result = _try_single_model(messages, cfg, cfg.get("model", ""))
    if result:
        return result

    # 主模型失败，尝试 fallback
    fallbacks = cfg.get("fallback_models", [])
    for fb_model in fallbacks:
        if fb_model == cfg.get("model", ""):
            continue  # 跳过已尝试的主模型
        log(f"  Fallback → {fb_model}")
        fb_cfg = dict(cfg)
        fb_cfg["model"] = fb_model
        result = _try_single_model(messages, fb_cfg, fb_model)
        if result:
            return result

    log("  所有模型均失败")
    return None


def _try_single_model(messages: list, cfg: dict, model_name: str) -> Optional[str]:
    """尝试单个模型"""
    provider = cfg.get("provider", "openclaw")

    if provider == "openclaw":
        result = _call_openclaw(messages, cfg)
        if result:
            return result
        if cfg.get("api_key"):
            return _call_openai_compatible(messages, cfg)
        return None

    elif provider in ("openai", "deepseek", "custom"):
        return _call_openai_compatible(messages, cfg)

    return None


# ============================================================
#  分析提示词构建
# ============================================================

def build_analysis_prompt(code: str, name: str, market_data: dict,
                            concept_list: list, news: str = "") -> list:
    """
    构建给 AI 的分析提示词。
    要求 AI 返回结构化 JSON。
    """
    concept_str = "、".join(concept_list) if concept_list else "无"

    market_str = ""
    if market_data:
        zdf = market_data.get("涨跌幅")
        if zdf is not None:
            market_str += f"涨跌幅: {zdf:+.2f}%  "
        hs = market_data.get("换手率")
        if hs is not None:
            market_str += f"换手率: {hs:.2f}%  "
        mv = market_data.get("总市值")
        if mv is not None:
            market_str += f"总市值: {mv:.0f}亿  "
        pe = market_data.get("市盈率")
        if pe is not None:
            market_str += f"PE: {pe:.1f}"

    system_prompt = """你是一名股票新闻信息提取助手。你的任务是从新闻和行情数据中提取结构化信息。

你必须按以下 JSON 格式返回结果（只返回 JSON，不要有其他文字）：
```json
{
  "利好事件": ["从新闻中提取的具体利好事件", "第二个利好"],
  "利空事件": ["从新闻中提取的具体利空事件"],
  "情感倾向": "正面或负面或中性",
  "行业景气度": "高或中或低",
  "关键词": "标签1+标签2+标签3"
}
```

规则：
1. 只从提供的新闻中提取，不要编造
2. 如无明确利好，利好事件返回空数组 []
3. 如无明确利空，利空事件返回空数组 []
4. 情感倾向只能是"正面""负面""中性"三选一
5. 行业景气度只能是"高""中""低"三选一
6. 关键词3-5个，用+号连接，每个≤6字
7. 只返回 JSON，不要前缀后缀文字

示例1：
输入：东方财富 人工智能+券商概念  涨幅5.2%
新闻：公司发布AI投顾产品"妙想"，已上线内测...
输出：
{"利好事件":["发布AI投顾产品妙想"],"利空事件":[],"情感倾向":"正面","行业景气度":"高","关键词":"AI投顾+券商+产品发布"}

示例2：
输入：某ST股  涨幅-3.1%
新闻：公司收到证监会立案告知书...
输出：
{"利好事件":[],"利空事件":["收到证监会立案告知书"],"情感倾向":"负面","行业景气度":"低","关键词":"监管风险+ST"}

示例3：
输入：宁德时代  电池概念  涨幅0.5%
新闻：无明显利好利空消息
输出：
{"利好事件":[],"利空事件":[],"情感倾向":"中性","行业景气度":"中","关键词":"动力电池+新能源"}
"""

    user_prompt = f"""请提取以下股票的信息：

股票代码: {code}
股票名称: {name}
所属概念: {concept_str}
行情数据: {market_str}
最近新闻: {news}

请从新闻中提取利好事件、利空事件，判断情感倾向和行业景气度。
只返回 JSON。"""

    return [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]


# ============================================================
#  单只股票分析
# ============================================================

def analyze_stock(code: str, name: str, market_data: dict,
                  concept_list: list, cfg: dict, cache: dict) -> dict:
    """
    分析单只股票，带缓存。
    返回标准化分析结果 dict。
    """
    ck = _cache_key(code, name)
    if ck in cache:
        log(f"  {name}({code}) 使用缓存结果")
        return cache[ck]

    log(f"  正在分析 {name}({code})...")

    # 先抓取最新新闻
    news = fetch_stock_news(code, name, days=3)
    messages = build_analysis_prompt(code, name, market_data, concept_list, news)
    raw = call_ai(messages, cfg)

    result = {
        "code": code,
        "name": name,
        "分析时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "_cached_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    # 存储新闻（截断到150字）
    result["最近新闻"] = news[:150] if news else ""

    if not raw:
        result["AI评分"] = {"总分": 50, "理由": "AI调用失败，使用中性评分"}
        result["利好事件"] = []
        result["利空事件"] = []
        result["情感倾向"] = "中性"
        result["行业景气度"] = "中"
        result["关键词"] = ""
        result["_ai_success"] = False
        return result

    # 尝试解析 JSON
    try:
        # 提取 ```json ... ``` 包裹的内容
        if "```json" in raw:
            start = raw.index("```json") + 7
            end = raw.index("```", start)
            raw = raw[start:end].strip()
        elif "```" in raw:
            start = raw.index("```") + 3
            end = raw.index("```", start)
            raw = raw[start:end].strip()

        parsed = json.loads(raw)

        # 提取结构化信息
        bullish = parsed.get("利好事件", [])
        bearish = parsed.get("利空事件", [])
        sentiment = parsed.get("情感倾向", "中性")
        prosperity = parsed.get("行业景气度", "中")
        keywords = parsed.get("关键词", "")

        # 兼容旧格式（催化剂/利空因素）
        if not bullish and parsed.get("催化剂"):
            bullish = [c.get("事件", str(c)) if isinstance(c, dict) else str(c) for c in parsed["催化剂"]]
        if not bearish and parsed.get("利空因素"):
            bearish = [c.get("因素", str(c)) if isinstance(c, dict) else str(c) for c in parsed["利空因素"]]

        result["利好事件"] = bullish if isinstance(bullish, list) else []
        result["利空事件"] = bearish if isinstance(bearish, list) else []
        result["情感倾向"] = sentiment if sentiment in ("正面", "负面", "中性") else "中性"
        result["行业景气度"] = prosperity if prosperity in ("高", "中", "低") else "中"
        result["关键词"] = keywords

        # ── 评分由系统自动计算（不让模型打分）──
        # 基础分50，利好+8/条，利空-8/条，情感±10，景气度±5
        score = 50
        score += len(result["利好事件"]) * 8
        score -= len(result["利空事件"]) * 8
        if result["情感倾向"] == "正面":
            score += 10
        elif result["情感倾向"] == "负面":
            score -= 10
        if result["行业景气度"] == "高":
            score += 5
        elif result["行业景气度"] == "低":
            score -= 5
        score = max(0, min(100, score))

        reason_parts = []
        if result["利好事件"]:
            reason_parts.append(f"利好{len(result['利好事件'])}条")
        if result["利空事件"]:
            reason_parts.append(f"利空{len(result['利空事件'])}条")
        reason_parts.append(f"情感{result['情感倾向']}")
        reason_parts.append(f"景气度{result['行业景气度']}")

        result["AI评分"] = {"总分": score, "理由": "+".join(reason_parts)}
        result["催化剂"] = [{"事件": e, "时间": "", "强度": ""} for e in result["利好事件"]]
        result["利空因素"] = [{"因素": e, "程度": ""} for e in result["利空事件"]]
        result["投资逻辑"] = f"利好{len(result['利好事件'])}条,利空{len(result['利空事件'])}条,情感{result['情感倾向']}"
        result["风险提示"] = ";".join(result["利空事件"][:2]) if result["利空事件"] else "无明确利空"
        result["行业定位"] = ""
        result["_ai_success"] = True

    except (json.JSONDecodeError, Exception) as e:
        log(f"  JSON解析失败: {e}，原始返回: {raw[:200]}")
        result["AI评分"] = {"总分": 50, "理由": f"解析失败: {str(e)[:50]}"}
        result["投资逻辑"] = "AI分析解析失败"
        result["_ai_success"] = False
        result["_raw_response"] = raw[:500] if raw else ""

    # 写入缓存
    cache[ck] = result
    return result


# ============================================================
#  批量分析入口（并发版）
# ============================================================

def batch_analyze(stocks: List[Dict], cfg: dict = None,
                  use_cache: bool = True) -> List[Dict]:
    """
    并发批量分析股票列表。
    使用 ThreadPoolExecutor 并行调用 AI API，大幅提速。

    Args:
        stocks: [{"code": "002491", "name": "通鼎互联", "market_data": {...}, "concepts": [...]}, ...]
        cfg: AI配置（为None时自动加载）
        use_cache: 是否使用缓存

    Returns:
        List[Dict]: 分析结果列表
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    if cfg is None:
        cfg = load_ai_config()

    if not cfg.get("enabled", True):
        log("AI分析未启用，跳过")
        return stocks

    cache = load_cache(cfg) if use_cache else {}

    total = len(stocks)
    max_workers = cfg.get("max_concurrent", 10)
    # 上限改为全量，不再限制50只
    max_stocks = total

    log(f"开始并发分析 {max_stocks}/{total} 只股票 (并发={max_workers})...")

    def _analyze_one(stock):
        code = str(stock.get("code", "")).zfill(6)
        name = stock.get("name", "")
        market_data = stock.get("market_data", {})
        concepts = stock.get("concepts", [])
        analysis = analyze_stock(code, name, market_data, concepts, cfg, cache)
        merged = dict(stock)
        merged.update({
            "AI_行业定位": analysis.get("行业定位", ""),
            "AI_催化剂": json.dumps(analysis.get("催化剂", []), ensure_ascii=False),
            "AI_利空因素": json.dumps(analysis.get("利空因素", []), ensure_ascii=False),
            "AI_利好事件": json.dumps(analysis.get("利好事件", []), ensure_ascii=False),
            "AI_利空事件": json.dumps(analysis.get("利空事件", []), ensure_ascii=False),
            "AI_情感倾向": analysis.get("情感倾向", "中性"),
            "AI_行业景气度": analysis.get("行业景气度", "中"),
            "AI_评分": analysis.get("AI评分", {}).get("总分", 50),
            "AI_评分理由": analysis.get("AI评分", {}).get("理由", ""),
            "AI_投资逻辑": analysis.get("投资逻辑", ""),
            "AI_关键词": analysis.get("关键词", ""),
            "AI_风险提示": analysis.get("风险提示", ""),
            "AI_分析成功": analysis.get("_ai_success", False),
            "AI_分析时间": analysis.get("分析时间", ""),
        })
        return merged

    results = [None] * max_stocks
    done_count = 0
    fail_count = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {}
        for i, stock in enumerate(stocks[:max_stocks]):
            future = executor.submit(_analyze_one, stock)
            future_map[future] = i

        for future in as_completed(future_map):
            idx = future_map[future]
            try:
                result = future.result()
                results[idx] = result
                done_count += 1
                if result and not result.get("AI_分析成功"):
                    fail_count += 1
            except Exception as e:
                fail_count += 1
                # 用默认值填充
                results[idx] = dict(stocks[idx])
                results[idx].update({"AI_评分": 50, "AI_分析成功": False})

            if done_count % 100 == 0 or done_count == max_stocks:
                log(f"  进度: {done_count}/{max_stocks} (失败={fail_count})")

    # 过滤None
    results = [r for r in results if r is not None]

    # 保存缓存
    if use_cache:
        save_cache(cache)

    save_analysis_results(results, cfg)

    success = sum(1 for r in results if r.get("AI_分析成功"))
    log(f"批量分析完成: 成功={success}/{len(results)}, 失败={fail_count}")
    return results


# ============================================================
#  结果保存
# ============================================================

def save_analysis_results(results: List[Dict], cfg: dict = None):
    """保存分析结果到 JSON 和 CSV"""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    # JSON
    json_path = AGENT_DIR / f"ai_analysis_{ts}.json"
    try:
        # 去除不能序列化的字段
        clean = []
        for r in results:
            d = {k: v for k, v in r.items() if not k.startswith("_")}
            clean.append(d)
        json_path.write_text(json.dumps(clean, ensure_ascii=False, indent=2), encoding="utf-8")
        log(f"分析结果已保存: {json_path}")
    except Exception as e:
        log(f"JSON保存失败: {e}")

    # CSV（方便查看）
    try:
        import pandas as pd
        df = pd.DataFrame(results)
        csv_path = AGENT_DIR / f"ai_analysis_{ts}.csv"
        df.to_csv(csv_path, index=False, encoding="utf-8-sig")
        log(f"分析结果CSV已保存: {csv_path}")
    except Exception as e:
        log(f"CSV保存失败: {e}")


# ============================================================
#  与主程序对接：直接传入 DataFrame
# ============================================================

def analyze_dataframe(df, top_n: int = 30, cfg: dict = None, use_cache: bool = True):
    """
    直接对选股 DataFrame 进行 AI 分析（供 stock_picker.py 调用）。

    Args:
        df: 选股结果 DataFrame（已含行情数据）
        top_n: 分析前N只（避免全量分析耗时过长）
        cfg: AI配置

    Returns:
        pd.DataFrame: 添加了 AI 分析列的 DataFrame
    """
    import pandas as pd

    if cfg is None:
        cfg = load_ai_config()

    if not cfg.get("enabled", True):
        log("AI分析未启用，跳过")
        return df

    # 构造 stocks 列表
    stocks = []
    for _, row in df.head(top_n).iterrows():
        code = str(row.get("代码", "")).zfill(6)
        name = row.get("名称", "")

        market_data = {}
        for col in ["涨跌幅", "换手率", "量比", "总市值", "市盈率动态", "成交额"]:
            val = row.get(col)
            if val is not None and not (isinstance(val, float) and pd.isna(val)):
                market_data[col] = float(val)

        concepts = []
        concept_str = row.get("概念", "")
        if concept_str and isinstance(concept_str, str):
            concepts = [c.strip() for c in concept_str.split("/") if c.strip()]

        stocks.append({
            "code": code,
            "name": name,
            "market_data": market_data,
            "concepts": concepts,
        })

    results = batch_analyze(stocks, cfg, use_cache=use_cache)

    # 将 AI 分析结果合并回 DataFrame
    ai_map = {r["code"]: r for r in results}

    df["AI_评分"] = 50  # 默认中性
    df["AI_行业定位"] = ""
    df["AI_投资逻辑"] = ""
    df["AI_关键词"] = ""
    df["AI_催化剂"] = ""
    df["AI_利空因素"] = ""
    df["AI_机构动态"] = ""
    df["AI_行业景气度"] = ""
    df["AI_风险提示"] = ""
    df["AI_分析成功"] = False

    for idx, row in df.iterrows():
        code = str(row.get("代码", "")).zfill(6)
        if code in ai_map:
            ai = ai_map[code]
            df.at[idx, "AI_评分"] = ai.get("AI_评分", 50)
            df.at[idx, "AI_行业定位"] = ai.get("AI_行业定位", "")
            df.at[idx, "AI_投资逻辑"] = ai.get("AI_投资逻辑", "")
            df.at[idx, "AI_关键词"] = ai.get("AI_关键词", "")
            df.at[idx, "AI_催化剂"] = ai.get("AI_催化剂", "")
            df.at[idx, "AI_利空因素"] = ai.get("AI_利空因素", "")
            df.at[idx, "AI_机构动态"] = ai.get("AI_机构动态", "")
            df.at[idx, "AI_行业景气度"] = ai.get("AI_行业景气度", "")
            df.at[idx, "AI_风险提示"] = ai.get("AI_风险提示", "")
            df.at[idx, "AI_分析成功"] = ai.get("AI_分析成功", False)

    return df


# ============================================================
#  命令行入口
# ============================================================
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="AI股票分析模块")
    parser.add_argument("--input", type=str, help="输入JSON文件（agent_input.json格式）")
    parser.add_argument("--top_n", type=int, default=20, help="分析前N只（默认20）")
    parser.add_argument("--provider", type=str, help="AI提供商（openclaw/openai/deepseek）")
    parser.add_argument("--api_key", type=str, help="API Key")
    parser.add_argument("--model", type=str, help="模型名称")
    parser.add_argument("--no_cache", action="store_true", help="不使用缓存")
    args = parser.parse_args()

    cfg = load_ai_config()
    if args.provider:
        cfg["provider"] = args.provider
    if args.api_key:
        cfg["api_key"] = args.api_key
    if args.model:
        cfg["model"] = args.model

    if args.input and Path(args.input).exists():
        data = json.loads(Path(args.input).read_text(encoding="utf-8"))
        stocks = data.get("stocks", [])
        results = batch_analyze(stocks[:args.top_n], cfg, use_cache=not args.no_cache)
        print(f"\n分析完成，成功: {sum(1 for r in results if r.get('AI_分析成功'))}/{len(results)}")
    else:
        print("用法: python stock_ai_analysis.py --input agent_data/agent_input.json --top_n 20")
        print("或: python stock_ai_analysis.py --provider openai --api_key sk-xxx --top_n 10")



