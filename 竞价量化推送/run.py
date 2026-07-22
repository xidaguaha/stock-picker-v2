#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
量化选股系统 v6.0.1 - 自更新版
=========================================
核心原则:
  1. 东方财富永远是首选数据源(盘中/盘后均优先尝试)
  2. 所有时段均允许降级:东方财富→腾讯→新浪,不硬锁单一来源
  3. 降级后交叉验证:抽样对比另一数据源,确认准确性
  4. 竞价数据增强:个股API获取竞价涨幅+竞价额(飞书推送展示)
  5. 消息面五层整合(a-stock-data + mootdx):
     新闻+研报+信号+筹码+基础(F10/季报) → 选他原因关键词
  6. 基础数据层(mootdx 通达信免费TCP):净利润同比/营收同比/ROE/EPS等
  7. 选他原因关键词:因子得分 + 消息面信号 + 财报数据 → 自动生成标签
  8. 自更新(v5.9新增):启动时检测远程版本 + 本地源码版本,自动下载替换并重启
  9. 全链路数据时间戳, 每一条数据可追溯「何时抓取/来源/是否盘中」
  10. 每次运行完整归档到 history/, 支持回测验证

参考:
  xiaopengm3-ai/stock-picker (41因子七维评分)
  腾讯云多因子选股教程 (Rank百分位标准化)
"""

import requests
import pandas as pd
import numpy as np
import json
import os
import sys
import time
import argparse
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from io import StringIO
from concurrent.futures import ThreadPoolExecutor, as_completed
import io as _io

# ── 必须先设置共用模块路径，再导入 logger 和 stock_picker ──
_COMMON = Path(__file__).parent.parent / "共用模块"
sys.path.insert(0, str(_COMMON))

# ===================== 导入新日志系统 =====================
from logger import get_logger, init_logger, log_exceptions

# ── 导入共用模块的所有公共函数和变量（消除代码重复，自动同步更新） ──
from stock_picker import *
# 下划线开头的函数不会被 import * 导入，需显式导入
from stock_picker import _emerging_concept_sync, _data_provenance

# ── pythonw.exe 环境适配:无控制台时重定向 stdout/stderr 到日志 ──
_NO_CONSOLE = False
try:
    if sys.stdout is None or not hasattr(sys.stdout, 'write'):
        _NO_CONSOLE = True
    elif sys.executable.lower().endswith('pythonw.exe'):
        _NO_CONSOLE = True
except Exception as e:
    log(f"操作异常: {e}", "WARN")

if _NO_CONSOLE:
    # pythonw.exe 环境:重定向 stdout/stderr 到 nohup.log
    _nohup_log = BASE_DIR / 'nohup.log'
    try:
        _nohup_f = open(_nohup_log, 'a', encoding='utf-8', buffering=1)
        sys.stdout = _nohup_f
        sys.stderr = _nohup_f
    except Exception as e:
        log(f"操作异常: {e}", "WARN")

# ── Windows CMD GBK 编码安全:Emoji → ? 替换,不崩溃 ──
try:
    if hasattr(sys.stdout, 'buffer') and sys.stdout.encoding:
        _enc = sys.stdout.encoding.lower()
        if _enc in ('gbk', 'gb2312', 'cp936', 'gb18030'):
            sys.stdout = _io.TextIOWrapper(sys.stdout.buffer, encoding=_enc, errors='replace')
            sys.stderr = _io.TextIOWrapper(sys.stderr.buffer, encoding=_enc, errors='replace')
except Exception as e:
    log(f"操作异常: {e}", "WARN")

# mootdx - 通达信免费 TCP 协议,基础数据层(F10/季报)
_mootdx_available = False
try:
    from mootdx.quotes import Quotes
    from mootdx.finance import Finance
    _mootdx_available = True
except ImportError:
    pass

# stock_ai_analysis - AI外围信息分析模块(可选)
try:
    from stock_ai_analysis import analyze_dataframe as _ai_analyze_df
    from stock_ai_analysis import load_ai_config as _load_ai_cfg
    from stock_ai_analysis import batch_analyze as _ai_batch_analyze
    _ai_analysis_available = True
except ImportError:
    _ai_analysis_available = False
    def _ai_analyze_df(df, top_n=30, cfg=None):
        log("AI分析模块未安装，跳过", "WARN")
        return df
    def _load_ai_cfg():
        return {"enabled": False}

# ai_feedback - AI评分反馈模块(可选)
try:
    from ai_feedback import on_ai_analysis_complete as _record_ai_prediction
    _ai_feedback_available = True
except Exception:
    _ai_feedback_available = False

# ============================================================
#  全局配置
# ============================================================

VERSION = "6.1.0"

# 文件路径 - PyInstaller onefile 兼容
#   sys._MEIPASS: 临时解压目录(只读,含打包的 data 文件)
#   sys.executable 所在目录: EXE 真实位置(可写,日志/缓存/配置放这里)
if getattr(sys, 'frozen', False):
    _EXE_DIR = Path(sys.executable).parent       # EXE 所在目录(可写)
    _MEIPASS = Path(sys._MEIPASS)                 # 临时解压目录(只读)
    BASE_DIR = _EXE_DIR
    # config.json 查找顺序:EXE目录 → 父目录 → _MEIPASS默认配置
    CONFIG_FILE = BASE_DIR / "config.json"
    _PARENT_CONFIG = BASE_DIR.parent / "config.json"
    _MEIPASS_CONFIG = _MEIPASS / "config.json"
    if not CONFIG_FILE.exists():
        import shutil
        if _PARENT_CONFIG.exists():
            shutil.copy(_PARENT_CONFIG, CONFIG_FILE)
        elif _MEIPASS_CONFIG.exists():
            shutil.copy(_MEIPASS_CONFIG, CONFIG_FILE)
else:
    BASE_DIR = Path(__file__).parent.parent  # 指向项目根目录
    CONFIG_FILE = BASE_DIR / "config.json"

CACHE_DIR   = BASE_DIR / "cache"
LOG_DIR     = BASE_DIR / "logs"
OUT_DIR     = BASE_DIR / "output"
HISTORY_DIR = BASE_DIR / "history"

# 确保目录存在
for d in [CACHE_DIR, LOG_DIR, OUT_DIR, HISTORY_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# ── 本地日志函数（确保线程安全，覆盖 stock_picker 导入的版本） ──
_run_logger = init_logger()
_log_file_path_run = LOG_DIR / f"run_{_run_logger.run_id}.log"


def log(msg, level="INFO"):
    """带毫秒时间戳的日志"""
    if level == "DEBUG":
        _run_logger.debug(msg)
    elif level == "WARN":
        _run_logger.warn(msg)
    elif level == "ERROR":
        _run_logger.error(msg)
    elif level == "FATAL":
        _run_logger.fatal(msg)
    else:
        _run_logger.info(msg)
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    line = f"[{ts}] [{level:5s}] {msg}"
    print(line)
    try:
        with open(_log_file_path_run, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


def warn(msg): log(msg, "WARN")


def error(msg, exception=None):
    if exception:
        _run_logger.error(msg, exception=exception)
    log(msg, "ERROR")


# HTTP 请求头
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

# 数据源标识
SRC_EASTMONEY = "东方财富"
SRC_TENCENT   = "腾讯财经"
SRC_SINA      = "新浪财经"
SRC_CACHE     = "本地缓存"
SRC_LOCAL_KW  = "本地关键词"
SRC_FAILED    = "获取失败"
SRC_CROSS_OK  = "交叉验证通过"   # 降级后交叉验证确认
SRC_CROSS_WARN= "交叉验证告警"   # 降级后交叉验证存疑

# API 地址
EASTMONEY_API = "https://push2.eastmoney.com/api/qt/clist/get"
TENCENT_API   = "http://qt.gtimg.cn/q={codes}"
SINA_KLINE    = "https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData"

# 交叉验证配置
CROSS_VALIDATE_SAMPLE = 15      # 抽样校验数量
CROSS_VALIDATE_PRICE_TOL = 0.02 # 价格容忍度 (2%)
CROSS_VALIDATE_CHANGE_TOL = 0.5 # 涨跌幅容忍度 (0.5个百分点)

# ============================================================
#  东财统一请求入口 (a-stock-data 风格 - 节流+复用Session)
# ============================================================

import random
import uuid

EM_SESSION = requests.Session()
EM_SESSION.headers.update({"User-Agent": UA})
EM_MIN_INTERVAL = 1.0          # 两次东财请求最小间隔(秒)
_em_last_call = [0.0]          # 模块级上次请求时间戳

def em_get(url: str, params: dict | None = None, headers: dict | None = None,
           timeout: int = 15, **kwargs):
    """东财统一请求入口:自动节流 + 复用 session + 默认 UA。"""
    wait = EM_MIN_INTERVAL - (time.time() - _em_last_call[0])
    if wait > 0:
        time.sleep(wait + random.uniform(0.1, 0.5))
    try:
        return EM_SESSION.get(url, params=params, headers=headers, timeout=timeout, **kwargs)
    finally:
        _em_last_call[0] = time.time()

def eastmoney_datacenter(report_name: str, columns: str = "ALL",
                          filter_str: str = "", page_size: int = 50,
                          sort_columns: str = "", sort_types: str = "-1") -> list[dict]:
    """东财数据中心统一查询 - 龙虎榜/融资融券/大宗交易/股东户数/分红 共用(已内置限流)

    常用 report_name:
      RPT_DAILYBILLBOARD_DETAILSNEW  龙虎榜上榜记录
      RPT_BILLBOARD_DAILYDETAILSBUY  龙虎榜买入席位
      RPT_BILLBOARD_DAILYDETAILSSELL 龙虎榜卖出席位
      RPTA_WEB_RZRQ_GGMX             融资融券明细
      RPT_DATA_BLOCKTRADE            大宗交易记录
      RPT_HOLDERNUMLATEST            股东户数变化
    """
    params = {
        "reportName": report_name, "columns": columns,
        "filter": filter_str, "pageNumber": "1", "pageSize": str(page_size),
        "sortColumns": sort_columns, "sortTypes": sort_types,
        "source": "WEB", "client": "WEB",
    }
    r = em_get("https://datacenter-web.eastmoney.com/api/data/v1/get", params=params, timeout=15)
    d = r.json()
    if d.get("result") and d["result"].get("data"):
        return d["result"]["data"]
    return []

# 默认配置（因子权重/热门概念从 factor_weights.json 加载，已由 stock_picker 导入）
DEFAULT_CONFIG = {
    "feishu_webhook": "",
    "top_n": 10,
    "tech_limit": 0,       # 0 = 全量计算技术因子
    "enable_feishu": False,
    "min_price": 1.0,
    "min_turnover": 0.1,
    "exclude_st": True,
}

VERBOSE = True

# ============================================================
#  时间控制模块 - 核心:盘中时间精确判定
# ============================================================


def main(top_n=10, tech_limit=0, enable_feishu=False, feishu_webhook="", config=None):
    global concept_map, _data_provenance, df_scored_global, df_quotes_global

    _data_provenance = []
    run_start = get_now()

    # ── 时间保护:9:00-9:14 运行时警告并自动等待 ──
    # 东方财富/腾讯/新浪等数据源在 9:30 开盘后才更新当日数据
    # 但竞价阶段(9:15-9:30)不需要等待——使用昨日AI缓存+竞价API数据
    _is_auction = (config or {}).get("_load_yesterday_ai", False)
    # 自动识别: 工作日 9:15-9:30 运行时，自动设为竞价模式（跳过等待）
    if not _is_auction and run_start.weekday() < 5:
        if run_start.hour == 9 and 15 <= run_start.minute < 30:
            _is_auction = True
            if config is None:
                config = {}
            config["_load_yesterday_ai"] = True
            config["_is_trading_run"] = True
            log("  [自动识别] 竞价时段运行, 跳过时间保护")
    if not _is_auction and run_start.weekday() < 5:
        if run_start.hour == 9 and run_start.minute < 15:
            wait_until = run_start.replace(minute=15, second=0, microsecond=0)
            wait_sec = int((wait_until - run_start).total_seconds())
            if wait_sec > 0:
                log("=" * 70)
                log(f"⏰ 时间保护:当前时间 {run_start.strftime('%H:%M')} 在竞价前")
                log(f"   程序将自动等待到 9:15 开始获取竞价数据(约 {wait_sec} 秒)...")
                log("=" * 70)
                time.sleep(wait_sec)
                log(f"✅ 已到 9:15,开始获取数据")
                _is_auction = True
                if config is None:
                    config = {}
                config["_load_yesterday_ai"] = True

    log("=" * 70)
    log(f"  量化选股系统 v{VERSION} 启动")
    log(f"  启动时间: {run_start.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}")
    log("=" * 70)

    # ── 自更新检测 ──
    log("")
    log("── 自更新检测 ──")
    try:
        _upd_cfg = config
        if _upd_cfg is None:
            # fallback: 直接读 config.json
            if CONFIG_FILE.exists():
                _upd_cfg = json.loads(CONFIG_FILE.read_text(encoding="utf-8-sig"))
            else:
                _upd_cfg = {}
        if check_for_updates(_upd_cfg):
            # check_for_updates 返回 True 意味着已移交更新流程,主进程将退出
            log("更新流程已启动,当前进程退出")
            return
    except Exception as e:
        log(f"更新检测异常(跳过继续): {e}")
    log("当前已是最新版本,继续运行")

    # 会话信息
    log("")
    log("── 时间校准 ──")
    for line in format_session_info().split("\n"):
        log(line.strip().lstrip("  "))

    # 检查定时任务
    log("")
    log("── 定时任务状态 ──")
    check_scheduled_task()

    # Step 1: 获取股票列表
    log("")
    log("── Step 1/5: 获取 A 股列表 ──")
    stocks = fetch_stock_list()
    if not stocks:
        error("无法获取股票列表, 退出")
        return

    # Step 2: 获取实时行情(多源降级+交叉验证)
    log("")
    log("── Step 2/5: 获取实时行情 ──")
    df = fetch_quotes(stocks)

    # 所有数据源均失败 = 中止(极罕见)
    if df is None:
        error("❌ 所有数据源(东财/腾讯/新浪)均不可用,选股中止")
        if enable_feishu and feishu_webhook:
            alert_payload = {
                "msg_type": "text",
                "content": {
                    "text": f"⚠️ 量化选股告警\n时间: {get_now().strftime('%m-%d %H:%M')}\n原因: 所有数据源(东财/腾讯/新浪)均不可用"
                }
            }
            try:
                requests.post(feishu_webhook, json=alert_payload, timeout=5)
            except Exception as e:
                log(f"操作异常: {e}", "WARN")
        return

    # 降级/交叉验证告警推送
    if enable_feishu and feishu_webhook and _data_provenance:
        last_prov = _data_provenance[-1]
        extra = last_prov.get("额外信息", {})
        cv = extra.get("交叉验证", {})
        source = last_prov.get("数据来源", "")

        if source == SRC_FAILED:
            pass  # 已在上面处理
        elif cv.get("status") == "warn":
            # 交叉验证存疑,飞书告警
            alert_text = (
                f"⚠️ 量化选股交叉验证告警\n"
                f"时间: {get_now().strftime('%m-%d %H:%M')}\n"
                f"数据源: {source} (东财降级)\n"
                f"匹配率: {cv.get('match_rate', '?')}\n"
                f"均价偏差: {cv.get('avg_price_deviation', '?')}"
            )
            try:
                requests.post(feishu_webhook, json={
                    "msg_type": "text",
                    "content": {"text": alert_text}
                }, timeout=5)
            except Exception as e:
                log(f"操作异常: {e}", "WARN")

    # 数据清洗 - 增强版:过滤亏损股、北交所、低流动性、高估值
    original_count = len(df)
    df = df[df["最新价"] > 0]
    df = df[df["换手率"] > 0.01]
    df = df[~df["名称"].str.contains("ST|退", na=False)]

    # 过滤北交所股票(920/430/830/87/88开头),流动性差、散户难参与
    df = df[~df["代码"].str.match(r"^(920|430|830|87|88)", na=False)]

    # 过滤亏损股:市盈率动态 <= 0 或净利润为负
    if "市盈率动态" in df.columns:
        profitable_mask = df["市盈率动态"] > 0
        excluded_loss = (~profitable_mask).sum()
        df = df[profitable_mask]
    else:
        excluded_loss = 0

    # 过滤高估值股:市盈率动态 > 100,避免估值泡沫
    if "市盈率动态" in df.columns:
        pe_reasonable_mask = df["市盈率动态"] <= 100
        excluded_high_pe = (~pe_reasonable_mask).sum()
        df = df[pe_reasonable_mask]
    else:
        excluded_high_pe = 0

    # 过滤极小市值(< 20亿),避免庄股和流动性陷阱
    if "总市值" in df.columns:
        min_mv_mask = df["总市值"] >= 20_0000_0000  # 20亿
        excluded_small = (~min_mv_mask).sum()
        df = df[min_mv_mask]
    else:
        excluded_small = 0

    # 过滤低价股(< 3元),避免仙股
    df = df[df["最新价"] >= 3.0]

    log(f"清洗后: {len(df)} 只 (去除停牌/ST/退市/北交所/亏损股/高估值/小市值/低价股)")
    log(f"  排除: 北交所+亏损+高估值(PE>100)+小市值+低价 共 {original_count - len(df)} 只")

    # 保存原始行情供进化引擎使用
    df_quotes_global = df.copy()

    # Step 3: 获取概念板块
    log("")
    log("── Step 3/5: 获取概念板块 ──")
    all_codes = [{"code": row["代码"], "name": row["名称"]} for _, row in df.iterrows()]
    concept_map = fetch_concept_map(all_codes)

    # 自动发现新兴概念(只从东方财富/缓存数据中提取,关键词兜底不参与)
    _data_prov = _data_provenance[-1] if _data_provenance else {}
    src_name = _data_prov.get("数据来源", "")
    if src_name not in (SRC_LOCAL_KW, SRC_FAILED):
        _emerging_concept_sync(concept_map, src_name)

    # Step 4: 计算因子 + 打分
    log("")
    log("── Step 4/5: 因子计算 & 综合打分 ──")
    log(f"  因子体系: {len(FACTOR_WEIGHTS)} 个因子")
    log(f"  标准化: Rank 百分位")
    log(f"  权重: {json.dumps(FACTOR_WEIGHTS, ensure_ascii=False)}")

    df = compute_factors(df, concept_map, tech_limit)
    df = compute_composite_score(df, FACTOR_WEIGHTS)

    # ── AI 外围信息分析(新)──
    log("")
    log("── AI 外围信息分析 ──")
    _ai_cfg = _load_ai_cfg() if _ai_analysis_available else {"enabled": False}
    _ai_top_n = _ai_cfg.get("analyze_top_n", 30)
    _skip_ai = (config or {}).get("_skip_ai", False)
    _ai_only = (config or {}).get("_ai_only", False)
    _load_yesterday = (config or {}).get("_load_yesterday_ai", False)

    # 竞价推送/盘中模式：不做实时AI分析，只加载昨日结果（避免耗时过长）
    # daemon模式下由外部传入 _is_trading_run，单次main()默认不跳过AI
    _is_trading_run = (config or {}).get("_is_trading_run", False)
    if _is_trading_run:
        _load_yesterday = True
        _skip_ai = True
        log("  [调度] 竞价/盘中模式: 跳过实时AI分析, 使用昨日结果")

    if _skip_ai:
        log("  [调度] 跳过AI分析(省时间)")
    if _ai_only:
        log("  [调度] 晚间模式:仅做AI分析,不推送")

    # 加载昨日AI打分+关键词作为因子
    _yesterday_ai = {}
    _yesterday_keywords = {}
    if _load_yesterday:
        try:
            import json as _json_mod
            # AI分析结果在项目根目录的 agent_data/ 下
            _ai_file = BASE_DIR / "agent_data" / "latest_ai_analysis.json"
            if _ai_file.exists():
                _ai_data = _json_mod.loads(_ai_file.read_text(encoding="utf-8"))
                _results = _ai_data.get("results", [])
                for r in _results:
                    _code = str(r.get("code", "")).zfill(6)
                    _score = r.get("AI_评分", {})
                    if isinstance(_score, dict):
                        _s = _score.get("总分", 50)
                    else:
                        _s = int(_score) if _score else 50
                    _yesterday_ai[_code] = _s
                    _yesterday_keywords[_code] = r.get("AI_关键词", "")
                log(f"  [AI因子] 加载昨日AI打分: {len(_yesterday_ai)} 只")
            else:
                log("  [AI因子] 昨日AI分析文件不存在")
        except Exception as e:
            log(f"  [AI因子] 加载失败: {e}")

    if _ai_cfg.get("enabled", False) and _ai_analysis_available and not _skip_ai:
        try:
            log(f"  对 Top {_ai_top_n} 只股票进行 AI 外围分析...")
            df = _ai_analyze_df(df, top_n=_ai_top_n, cfg=_ai_cfg)
            # 将 AI 评分融入综合得分
            _ai_w = _ai_cfg.get("score_weight", 0.25)
            _quant_w = 1.0 - _ai_w
            # AI评分本身已是0-100，不做min-max拉伸
            _ai_scores = df["AI_评分"].fillna(50).values.astype(float)
            if len(_ai_scores) == 0 or np.all(np.isnan(_ai_scores)):
                log(f"  AI评分无有效数据，跳过融合", "WARN")
            else:
                _ai_norm = np.clip(_ai_scores, 0, 100)
                # 融合:新综合得分 = 量化 × (1-w) + AI × w
                df["综合得分_量化"] = df["综合得分"].copy()
                df["综合得分"] = df["综合得分"] * _quant_w + _ai_norm * _ai_w
                df = df.sort_values("综合得分", ascending=False).reset_index(drop=True)
                df["排名"] = range(1, len(df) + 1)
                log(f"  AI评分已融入综合得分(AI权重={_ai_w},量化权重={_quant_w})")
            log(f"  AI分析成功: {sum(1 for s in df['AI_分析成功'] if s)}/{_ai_top_n}")
        except Exception as e:
            log(f"  AI分析整体异常，跳过AI融合: {e}", "WARN")
            if "AI_评分" not in df.columns:
                df["AI_评分"] = 50.0
            if "AI_分析成功" not in df.columns:
                df["AI_分析成功"] = False
    
    # 记录 AI 预测到反馈系统（供后续验证）
    if _ai_feedback_available:
        try:
            snapshot_date = get_now().strftime("%Y-%m-%d")
            for idx, row in df.head(_ai_top_n).iterrows():
                if row.get("AI_分析成功"):
                    # AI_催化剂可能是JSON字符串，需要解析
                    catalysts = row.get("AI_催化剂", "[]")
                    if isinstance(catalysts, str):
                        try:
                            catalysts = json.loads(catalysts) if catalysts.startswith("[") else []
                        except:
                            catalysts = []
                    bearish = row.get("AI_利空因素", "[]")
                    if isinstance(bearish, str):
                        try:
                            bearish = json.loads(bearish) if bearish.startswith("[") else []
                        except:
                            bearish = []

                    _record_ai_prediction(
                        code=str(row["代码"]),
                        name=row["名称"],
                        snapshot_date=snapshot_date,
                        ai_score=int(row.get("AI_评分", 50)),
                        ai_result={
                            "行业定位": row.get("AI_行业定位", ""),
                            "催化剂": catalysts,
                            "利空因素": bearish,
                        },
                        composite_score=float(row["综合得分"]),
                        concept_list=concept_map.get(str(row["代码"]), []),
                    )
            log(f"  AI反馈记录: 已记录 {sum(1 for s in df.head(_ai_top_n)['AI_分析成功'] if s)} 条预测")
        except Exception as e:
            log(f"  AI反馈记录失败: {e}", "WARN")
    elif not _ai_analysis_available:
        log("  AI分析模块未安装,跳过(运行 pip install -r requirements.txt 安装)")
    elif not _ai_cfg.get("enabled", False):
        log("  AI分析未启用(config.json 中 ai_analysis.enabled=false)")

    # ── 竞价时段预提取竞价数据 ──
    # 在AI补全之前先抓竞价数据，避免AI补全耗时导致错过竞价窗口
    _now_pre = get_now()
    if _now_pre.weekday() < 5 and _now_pre.hour == 9 and 15 <= _now_pre.minute <= 25:
        log("  [竞价预提取] 当前为竞价时段, 从批量行情数据提取竞价数据")
        df["竞价涨幅"] = np.nan
        df["竞价额"] = np.nan
        _auction_pre_count = 0
        if "成交额" in df.columns and "昨收" in df.columns:
            for idx, row in df.iterrows():
                _pre_close = row.get("昨收", 0)
                _auction_price = row.get("最新价", 0)
                _auction_amt = row.get("成交额", 0)
                if _pre_close > 0 and _auction_amt > 0 and _auction_price > 0:
                    _jjzf = round((_auction_price - _pre_close) / _pre_close * 100, 2)
                    df.at[idx, "竞价涨幅"] = _jjzf
                    df.at[idx, "竞价额"] = _auction_amt
                    _auction_pre_count += 1
        log(f"  [竞价预提取] 提取 {_auction_pre_count}/{len(df)} 只竞价数据")

    # 融合昨日AI打分+关键词作为因子
    if _yesterday_ai:
        _ai_weight = _ai_cfg.get("score_weight", 0.08)
        df["AI因子"] = 50.0
        df["AI_关键词"] = ""
        _matched = 0
        for idx, row in df.iterrows():
            _code = str(row.get("代码", "")).zfill(6)
            if _code in _yesterday_ai:
                df.at[idx, "AI因子"] = _yesterday_ai[_code]
                df.at[idx, "AI_关键词"] = _yesterday_keywords.get(_code, "")
                _matched += 1
        log(f"  [AI因子] 匹配 {_matched}/{len(df)} 只,权重={_ai_weight}")

        # ── AI因子补全: 对未匹配的个股临时调用AI分析 ──
        # 昨日AI缓存只覆盖了当时的Top500，今日新进入榜单的个股没有AI因子
        # 对这些个股临时调用batch_analyze补全，避免AI因子缺失导致排名偏差
        _unmatched_codes = []
        for idx, row in df.iterrows():
            _code = str(row.get("代码", "")).zfill(6)
            if _code not in _yesterday_ai:
                _unmatched_codes.append((idx, _code, row))
        
        if _unmatched_codes and _ai_cfg.get("enabled", False) and _ai_analysis_available:
            _supplement_top = min(len(_unmatched_codes), _ai_top_n)
            log(f"  [AI补全] 未匹配 {len(_unmatched_codes)} 只, 对前{_supplement_top}只临时分析")
            try:
                _supplement_stocks = []
                for i, (idx, code, row) in enumerate(_unmatched_codes[:_supplement_top]):
                    _supplement_stocks.append({
                        "code": code,
                        "name": row.get("名称", ""),
                        "market_data": {},
                        "concepts": concept_map.get(code, []),
                    })
                _supplement_results = _ai_batch_analyze(_supplement_stocks, cfg=_ai_cfg, use_cache=True)
                _supplement_ok = 0
                for r in _supplement_results:
                    _s_code = str(r.get("code", "")).zfill(6)
                    _s_score = r.get("AI_评分", 50)
                    if isinstance(_s_score, dict):
                        _s_score = _s_score.get("总分", 50)
                    _s_keyword = r.get("AI_关键词", "")
                    if r.get("AI_分析成功"):
                        _yesterday_ai[_s_code] = int(_s_score) if _s_score else 50
                        _yesterday_keywords[_s_code] = _s_keyword
                        _supplement_ok += 1
                    # 更新df中的值
                    for idx2, row2 in df.iterrows():
                        if str(row2.get("代码", "")).zfill(6) == _s_code:
                            df.at[idx2, "AI因子"] = int(_s_score) if _s_score else 50
                            df.at[idx2, "AI_关键词"] = _s_keyword
                            break
                log(f"  [AI补全] 成功 {_supplement_ok}/{_supplement_top} 只")
                _matched += _supplement_ok
            except Exception as e:
                log(f"  [AI补全] 异常, 跳过: {e}", "WARN")

        # AI因子融合（AI评分本身已是0-100，不做min-max拉伸）
        _ai_scores = df["AI因子"].values.astype(float)
        if len(_ai_scores) == 0 or np.all(np.isnan(_ai_scores)):
            log(f"  [AI因子] 无有效AI因子数据，跳过融合", "WARN")
        else:
            _ai_norm = np.clip(_ai_scores, 0, 100)
            _quant_w2 = 1.0 - _ai_weight
            df["综合得分_量化"] = df["综合得分"].copy()
            df["综合得分"] = df["综合得分"] * _quant_w2 + _ai_norm * _ai_weight
            df = df.sort_values("综合得分", ascending=False).reset_index(drop=True)
            df["排名"] = range(1, len(df) + 1)
        log(f"  [AI因子] 融合完成: 量化={_quant_w2:.2f}, AI={_ai_weight:.2f}")

    # 保存全量因子得分供进化引擎使用
    df_scored_global = df.copy()

    # AI-only模式(晚间): 保存AI结果后提前返回
    if _ai_only:
        log("")
        log("── AI-only模式: 保存结果 ──")
        try:
            import json as _json_mod
            _ai_save = []
            for _, row in df.head(min(200, len(df))).iterrows():
                _ai_save.append({
                    "code": str(row.get("代码", "")).zfill(6),
                    "name": row.get("名称", ""),
                    "AI_评分": int(row.get("AI_评分", 50)),
                    "AI_行业定位": row.get("AI_行业定位", ""),
                    "AI_投资逻辑": row.get("AI_投资逻辑", ""),
                    "AI_催化剂": row.get("AI_催化剂", []),
                    "AI_利空因素": row.get("AI_利空因素", []),
                    "_ai_success": bool(row.get("AI_分析成功", False)),
                })
            _ai_file = BASE_DIR / "agent_data" / "latest_ai_analysis.json"
            _ai_data = {
                "date": get_now().strftime("%Y-%m-%d"),
                "timestamp": get_now().isoformat(),
                "total_stocks": len(_ai_save),
                "successful": sum(1 for r in _ai_save if r.get("_ai_success")),
                "results": _ai_save,
            }
            _ai_file.write_text(_json_mod.dumps(_ai_data, ensure_ascii=False, indent=2), encoding="utf-8")
            log(f"  AI分析结果已保存: {_ai_file} ({len(_ai_save)} 只)")
        except Exception as e:
            log(f"  AI结果保存失败: {e}", "WARN")

        log("✅ AI-only模式完成")
        return

    # Step 5: 输出 & 归档
    log("")
    log("── Step 5/5: 输出 & 归档 ──")

    # BUG修复: 清洗后0只股票时graceful退出，避免format_output/iloc[0]崩溃
    if len(df) == 0:
        log("⚠ 清洗后股票池为空，无法选股，跳过推送", "WARN")
        log("  可能原因: 竞价时段数据源返回异常值/PE/市值为0导致全量过滤", "WARN")
        # 保存空结果快照（防止scheduler反复重试）
        try:
            save_results(df, top_n)
        except Exception:
            pass
        return

    output = format_output(df, concept_map, top_n)
    print(output)

    # 数据溯源报告
    print_provenance_report()

    # 竞价数据补充(为飞书推送做准备)
    # 如果竞价时段已预提取过竞价数据，这里只补充TopN中缺失的
    if "竞价涨幅" not in df.columns or df["竞价涨幅"].isna().all():
        log("")
        log("── 竞价数据补充 ──")
        df = enrich_auction_data(df, top_n)
    else:
        _has_auction = df.head(top_n)["竞价涨幅"].notna().sum()
        log(f"  [竞价数据] 已有 {_has_auction}/{top_n} 只, 跳过重复获取")

    # 消息面/信号/筹码增强(a-stock-data 整合)
    # 含二次评分: 深度因子(成长/质量/研报/机构/景气)在此阶段填充真实值后重算综合得分
    log("")
    log("── 消息面/信号/筹码增强 ──")
    df = enrich_news_and_signals(df, top_n)

    # 保存结果 & 历史归档(回测核心) — 必须在二次评分之后，确保快照含最终分数
    save_results(df, top_n)
    archive_run(df, concept_map, {"top_n": top_n, "tech_limit": tech_limit})

    # 飞书推送
    if enable_feishu and feishu_webhook:
        push_to_feishu(feishu_webhook, df, {"top_n": top_n}, concept_map)

    # 最终统计
    run_elapsed = (get_now() - run_start).total_seconds()
    log("")
    log(f"📊 统计:")
    log(f"  扫描: {len(df)} 只")
    log(f"  最高分: {df['综合得分'].iloc[0]:.1f}")
    log(f"  最低分: {df['综合得分'].iloc[-1]:.1f}")
    log(f"  耗时: {run_elapsed:.1f} 秒")
    log(f"  数据溯源: {len(_data_provenance)} 条")

    in_session, sn, _ = get_market_session()
    log(f"  盘中运行: {'是' if in_session else '否'}")
    log(f"  数据源: 东方财富首选, 支持降级+交叉验证")

    log("")
    log("✅ 运行完成")
    log("")


def _load_config_overrides():
    """从 config.json 加载配置覆盖:权重、热门概念、飞书等"""
    global FACTOR_WEIGHTS, HOT_CONCEPTS

    if not CONFIG_FILE.exists():
        # 尝试父目录
        parent_cfg = CONFIG_FILE.parent.parent / "config.json"
        if parent_cfg.exists():
            import shutil
            log(f"  config.json 不存在,从父目录复制: {parent_cfg}", "INFO")
            shutil.copy(parent_cfg, CONFIG_FILE)
        else:
            log(f"  config.json 未找到 ({CONFIG_FILE}),使用默认配置", "WARN")
            return {}, ""

    try:
        cfg = json.loads(CONFIG_FILE.read_text(encoding="utf-8-sig"))

        # 加载权重(config.json 优先)
        json_weights = cfg.get("weights", {})
        if json_weights:
            # 映射 config.json 的英文权重键到内部中文因子键
            _weight_map = {
                "momentum":  ["动量_涨跌幅", "动量_5日涨幅"],
                "volume":    ["量能_换手率", "量能_量比"],
                "technical": ["趋势_均线位置", "趋势_RSI强度", "技术_MACD金叉"],
                "valuation": ["估值_PE反向", "估值_PB反向"],
                "concept":   ["概念_热度"],
                "quality":   ["质量_盈利"],
                "fund_flow": ["机构_北向资金", "机构_基金持仓"],
                "growth":    ["成长_净利润同比", "成长_营收同比"],
                "report":    ["研报_覆盖度"],
                "sentiment": ["景气_行业"],
            }

            new_weights = {}
            for eng_key, cn_keys in _weight_map.items():
                total_w = json_weights.get(eng_key, 0)
                if cn_keys and total_w > 0:
                    per_factor = total_w / len(cn_keys)
                    for ck in cn_keys:
                        new_weights[ck] = round(per_factor, 4)

            # 规模因子默认保留(降低权重)
            if "规模_小市值" not in new_weights:
                new_weights["规模_小市值"] = 0.02

            # 质量因子默认保留
            if "质量_盈利" not in new_weights:
                new_weights["质量_盈利"] = 0.03

            # 归一化
            total = sum(new_weights.values())
            if total > 0:
                new_weights = {k: round(v/total, 4) for k, v in new_weights.items()}
                FACTOR_WEIGHTS = new_weights

        # 加载热门概念
        json_concepts = cfg.get("hot_concepts", [])
        if json_concepts:
            HOT_CONCEPTS = json_concepts

        return cfg, cfg.get("feishu_webhook", "")
    except Exception as e:
        log(f"config.json 读取异常: {e}", "WARN")
        return {}, ""


# ============================================================
#  常驻守护模式 - 四阶段调度
# ============================================================

def _already_done_today_v1(phase: str) -> bool:
    """检查某个时段今天是否已执行过，避免重启后重复运行"""
    today = get_now().strftime("%Y-%m-%d")
    today_compact = get_now().strftime("%Y%m%d")

    if phase == "盘后AI分析":
        ai_file = BASE_DIR / "agent_data" / "latest_ai_analysis.json"
        if ai_file.exists():
            try:
                data = json.loads(ai_file.read_text(encoding="utf-8-sig"))
                return data.get("date", "") == today
            except Exception as e:
                log(f"操作异常: {e}", "WARN")
        return False
    elif phase == "竞价量化推送":
        return any(HISTORY_DIR.rglob(f"snapshot_{today_compact}_*.csv")) if HISTORY_DIR.exists() else False
    elif phase == "盘后回测迭代":
        reports_dir = BASE_DIR / "reports"
        return any(reports_dir.glob(f"回测报告_{today_compact}_*.json")) if reports_dir.exists() else False
    elif phase == "盘中模拟交易":
        return any(HISTORY_DIR.rglob(f"snapshot_{today_compact}_*.csv")) if HISTORY_DIR.exists() else False
    return False


def daemon_sleep():
    """
    计算下一次运行前的休眠秒数。
    四阶段调度:
      盘后回测 (15:08-15:28) → 2分钟后
      等待晚间 (15:28-19:57) → 算到19:58
      盘后AI分析 (19:58+) → 5分钟
      竞价量化 (9:23-9:27) → 1分钟
      盘中模拟 (9:30-15:00) → 5分钟
    """
    now = get_now()
    in_session, sn, trading = get_market_session(now)
    t = now.time()
    h, m = t.hour, t.minute

    if not trading:
        # 周末:等到下周一 9:21
        next_run = now + timedelta(days=1)
        while next_run.weekday() >= 5:
            next_run += timedelta(days=1)
        next_run = next_run.replace(hour=9, minute=21, second=0, microsecond=0)
        return max(60, int((next_run - now).total_seconds()))

    if in_session:
        # 盘中:5分钟后
        return 300

    # 盘后回测 (15:08-15:28): 2分钟后
    if h == 15 and 8 <= m < 28:
        return 120

    # 等待AI分析开始(15:28-19:57): 算到19:58
    if (h == 15 and m >= 28) or (16 <= h <= 19) or (h == 19 and m < 58):
        target = now.replace(hour=19, minute=58, second=0, microsecond=0)
        if target <= now:
            target = target + timedelta(days=1)
        return max(60, int((target - now).total_seconds()))

    # 盘后AI分析 (19:58+): 5分钟
    if h >= 20 or (h == 19 and m >= 58):
        return 300

    # 竞价 (9:23-9:27): 1分钟
    if h == 9 and 23 <= m < 28:
        return 60

    # 盘前 (9:18-9:22): 1分钟
    if h == 9 and 18 <= m < 23:
        return 60

    # 其他: 1分钟
    return 60


def run_daemon(top_n, tech_limit, enable_feishu, webhook, config=None):
    """
    常驻守护主循环 — 四阶段调度:
      盘后回测 (15:08-15:28): 验证排名/模拟/AI + 迭代
      盘后AI分析 (19:58+): 尽早开始,凌晨完成即可
      竞价量化推送 (9:23-9:27): 量化打分(含昨日AI因子) + 推送Top3
      盘中模拟交易 (9:30-15:00): 按排名模拟买入
    """
    print()
    print("=" * 70)
    print(f"  量化选股系统 v{VERSION} - 常驻守护模式")
    print("=" * 70)
    print(f"  启动时间: {get_now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  四阶段: 盘后回测(15:08) → 盘后AI(16:58+) → 竞价量化(9:21) → 盘中模拟(9:30)")
    print(f"  按 Ctrl+C 退出")
    print("=" * 70)

    run_count = 0
    while True:
        try:
            run_count += 1
            now = get_now()
            in_session, sn, trading = get_market_session(now)
            t = now.time()
            h, m = t.hour, t.minute

            # ── 判断当前阶段 ──
            if not trading:
                phase = None
            elif h == 15 and 8 <= m < 28:
                phase = "盘后回测迭代"
            elif h >= 20 or (h == 19 and m >= 58):
                phase = "盘后AI分析"
            elif h == 9 and 23 <= m < 28:
                phase = "竞价量化推送"
            elif in_session:
                phase = "盘中模拟交易"
            else:
                phase = None  # 空闲

            if phase is None:
                # 空闲时段,直接休眠
                sleep_sec = daemon_sleep()
                next_wake = get_now() + timedelta(seconds=sleep_sec)
                hours = sleep_sec // 3600
                mins = (sleep_sec % 3600) // 60
                print(f"[休眠] {now.strftime('%H:%M')} 非任务时段,下次 {next_wake.strftime('%H:%M')} ({hours}时{mins}分后)")
                time.sleep(sleep_sec)
                continue

            # 检查是否今天已执行过该时段任务
            if _already_done_today_v1(phase):
                print(f"[跳过] {phase} 今天已执行过")
                sleep_sec = daemon_sleep()
                time.sleep(sleep_sec)
                continue

            print()
            print(f"[第{run_count}轮] {now.strftime('%Y-%m-%d %H:%M:%S')} | {sn} | {phase}")
            print("-" * 70)

            _run_cfg = dict(config) if config else {}

            if phase == "盘后回测迭代":
                # ── 回测验证 + 迭代 ──
                try:
                    # 验证量化排名
                    from backtest_engine import run_backtest, format_backtest_report
                    bt = run_backtest(hold_days=[1, 3, 5], top_n=10, verbose=False)
                    if bt:
                        print(format_backtest_report(bt))

                    # 模拟交易更新
                    from paper_trader import update_positions, format_trade_report
                    update_positions()
                    print(format_trade_report())

                    # 进化迭代
                    from evolution import run_evolution_cycle
                    meta = {
                        "in_session": False,
                        "session_name": "盘后回测迭代",
                        "data_source": "东方财富优先",
                        "weights": FACTOR_WEIGHTS,
                        "hot_concepts": HOT_CONCEPTS,
                        "version": VERSION,
                    }
                    run_evolution_cycle(
                        df_scored=df_scored_global if 'df_scored_global' in dir() else None,
                        df_quotes=df_quotes_global if 'df_quotes_global' in dir() else None,
                        concept_map=concept_map,
                        meta=meta,
                    )
                    print("  [回测迭代] 完成")
                except Exception as e:
                    print(f"  [回测迭代] 异常: {e}")

            elif phase == "盘后AI分析":
                # ── 晚间AI分析全部个股 ──
                try:
                    from stock_ai_analysis import batch_analyze, load_ai_config
                    ai_cfg = load_ai_config()
                    if ai_cfg.get("enabled", False):
                        # 获取全量股票做AI分析
                        _run_cfg["_ai_only"] = True  # 标记:只做AI分析
                        main(top_n, tech_limit, False, "", config=_run_cfg)
                        print("  [AI分析] 完成")
                    else:
                        print("  [AI分析] 未启用,跳过")
                except Exception as e:
                    print(f"  [AI分析] 异常: {e}")

            elif phase == "竞价量化推送":
                # ── 量化打分(含昨日AI因子) + 飞书推送 ──
                _run_cfg["_load_yesterday_ai"] = True
                _run_cfg["_is_trading_run"] = True
                main(top_n, tech_limit, enable_feishu, webhook, config=_run_cfg)

            elif phase == "盘中模拟交易":
                # ── 盘中只模拟买入，不推送飞书 ──
                _run_cfg["_skip_ai"] = True
                main(top_n, tech_limit, False, "", config=_run_cfg)

            # 计算休眠时间
            sleep_sec = daemon_sleep()
            next_wake = get_now() + timedelta(seconds=sleep_sec)

            if sleep_sec > 600:
                hours = sleep_sec // 3600
                mins = (sleep_sec % 3600) // 60
                print(f"[休眠] 下次 {next_wake.strftime('%H:%M')} ({hours}时{mins}分后)")
            else:
                print(f"[等待] {sleep_sec}秒后下一轮...")

            time.sleep(sleep_sec)

        except KeyboardInterrupt:
            print()
            print(f"[退出] 共运行 {run_count} 轮,{get_now().strftime('%Y-%m-%d %H:%M:%S')}")
            break
        except Exception as e:
            log(f"[守护] 本轮异常: {e}", "ERROR")
            print(f"[守护] 60秒后重试...")
            time.sleep(60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=f"A股量化选股系统 v{VERSION}")
    parser.add_argument("-n", "--top", type=int, default=10, help="输出前N只 (默认10)")
    parser.add_argument("-t", "--tech", type=int, default=0, help="技术因子计算数量 (0=全量, 默认0)")
    parser.add_argument("-q", "--quiet", action="store_true", help="简洁输出")
    parser.add_argument("--feishu", action="store_true", help="启用飞书推送")
    parser.add_argument("--webhook", type=str, default="", help="飞书 Webhook URL")
    parser.add_argument("--no-evolve", action="store_true", help="跳过进化周期(回测+调优+模拟交易)")
    parser.add_argument("--backtest-only", action="store_true", help="仅运行回测(不选股)")
    parser.add_argument("--tune-only", action="store_true", help="仅运行因子调优(不选股)")
    parser.add_argument("--status", action="store_true", help="显示进化系统状态")
    parser.add_argument("--daemon", action="store_true", help="常驻守护模式(智能休眠,盘中5分钟/盘后等到次日)")

    args = parser.parse_args()

    if args.quiet:
        VERBOSE = False

    # 加载 config.json 覆盖默认值
    cfg, cfg_webhook = _load_config_overrides()

    webhook = args.webhook or cfg_webhook
    enable_feishu = args.feishu or cfg.get("enable_feishu", False)
    top_n = args.top if args.top != 10 else cfg.get("top_n", 10)
    tech_limit = args.tech

    # --status: 仅显示状态
    if args.status:
        try:
            from evolution import format_evolution_status
            print(format_evolution_status())
        except ImportError as e:
            print(f"进化模块加载失败: {e}")
        sys.exit(0)

    # --backtest-only: 仅回测
    if args.backtest_only:
        try:
            from backtest_engine import run_backtest, format_backtest_report
            report = run_backtest(hold_days=[1, 3, 5, 10], top_n=10)
            if report:
                print(format_backtest_report(report))
        except ImportError as e:
            print(f"回测模块加载失败: {e}")
        sys.exit(0)

    # --tune-only: 仅调优
    if args.tune_only:
        try:
            from factor_tuner import auto_tune, format_tuning_report
            report = auto_tune(dry_run=False)
            print(format_tuning_report(report))
        except ImportError as e:
            print(f"调优模块加载失败: {e}")
        sys.exit(0)

    # 常驻守护模式
    if args.daemon:
        run_daemon(top_n, tech_limit, enable_feishu, webhook, config=cfg)
        sys.exit(0)

    # 正常选股流程(单次)
    main(top_n, tech_limit, enable_feishu, webhook, config=cfg)

    # 进化周期(每次选股后自动触发)
    if not args.no_evolve:
        try:
            from evolution import run_evolution_cycle

            # 获取当前运行上下文
            in_session, sn, _ = get_market_session()
            meta = {
                "in_session": in_session,
                "session_name": sn,
                "data_source": SRC_EASTMONEY if in_session else "东方财富优先",
                "weights": FACTOR_WEIGHTS,
                "hot_concepts": HOT_CONCEPTS,
                "concept_source": _data_provenance[-1]["数据来源"] if _data_provenance else "未知",
                "version": VERSION,
            }

            run_evolution_cycle(
                df_scored=df_scored_global if 'df_scored_global' in dir() else None,
                df_quotes=df_quotes_global if 'df_quotes_global' in dir() else None,
                concept_map=concept_map,
                meta=meta,
            )
        except ImportError as e:
            print(f"\n  [进化] 模块未就绪: {e}")
            print(f"  [进化] 请确保 backtest_engine.py, factor_tuner.py, "
                  f"paper_trader.py, data_accumulator.py, evolution.py 在同一目录")
        except Exception as e:
            print(f"\n  [进化] 周期异常: {e}")