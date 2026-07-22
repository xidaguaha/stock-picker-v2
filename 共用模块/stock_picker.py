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

# ===================== 导入新日志系统 =====================
from logger import get_logger, init_logger, log_exceptions

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

# ── 因子权重 & 热门概念：从 factor_weights.json 加载（回测系统可写入） ──
_FACTOR_WEIGHTS_FILE = BASE_DIR / "factor_weights.json"


def _load_factor_weights():
    """从 factor_weights.json 加载因子权重和热门概念"""
    default_weights = {
        "动量_涨跌幅": 0.09, "动量_5日涨幅": 0.07,
        "趋势_均线位置": 0.07, "趋势_RSI强度": 0.05,
        "量能_换手率": 0.05, "量能_量比": 0.04,
        "估值_PE反向": 0.07, "估值_PB反向": 0.03,
        "规模_小市值": 0.02, "技术_MACD金叉": 0.07,
        "概念_热度": 0.05, "质量_盈利": 0.03,
        "成长_净利润同比": 0.07, "成长_营收同比": 0.05,
        "机构_北向资金": 0.04, "机构_基金持仓": 0.04,
        "研报_覆盖度": 0.03, "景气_行业": 0.02,
        "情绪_市场": 0.02, "情绪_板块": 0.02,
        "LLM_信号": 0.02, "ai_score": 0.04,
    }
    default_concepts = [
        "AI芯片", "光通信", "CPO", "算力", "数据中心", "半导体", "芯片",
        "机器人", "减速器", "伺服", "自动化",
        "可控核聚变", "超导", "核电",
        "低空经济", "飞行汽车", "商业航天", "卫星", "火箭",
        "固态电池", "储能", "新能源",
        "华为", "鸿蒙", "海思", "国产替代", "专精特新", "小巨人",
        "创新药", "CXO", "医疗器械",
        "量子科技", "6G", "脑机接口",
    ]

    if _FACTOR_WEIGHTS_FILE.exists():
        try:
            data = json.loads(_FACTOR_WEIGHTS_FILE.read_text(encoding="utf-8-sig"))
            weights = data.get("weights", {})
            concepts = data.get("hot_concepts", [])
            # nan 保护：加载时如果发现nan权重，回退到默认值
            import math
            _has_nan = any(
                isinstance(v, float) and math.isnan(v) for v in weights.values()
            )
            if _has_nan:
                log("  [权重保护] factor_weights.json 含nan, 回退到默认权重", "WARN")
                return default_weights, default_concepts
            if weights:
                return weights, concepts if concepts else default_concepts
        except Exception as e:
            log(f"操作异常: {e}", "WARN")
    return default_weights, default_concepts


def _save_factor_weights(weights, concepts=None):
    """将因子权重写回 factor_weights.json（回测迭代后调用）"""
    data = {"version": "1.0", "last_updated": get_now().strftime("%Y-%m-%d")}
    data["weights"] = weights
    if concepts is not None:
        data["hot_concepts"] = concepts
    try:
        _FACTOR_WEIGHTS_FILE.write_text(
            json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    except Exception as e:
        log(f"保存因子权重失败: {e}", "WARN")


# 初始加载
_LOADED_WEIGHTS, _LOADED_CONCEPTS = _load_factor_weights()

# 默认因子权重(运行时变量，回测系统可覆盖)
_DEFAULT_FACTOR_WEIGHTS = dict(_LOADED_WEIGHTS)
FACTOR_WEIGHTS = dict(_LOADED_WEIGHTS)

# 默认热门概念
_DEFAULT_HOT_CONCEPTS = list(_LOADED_CONCEPTS)
HOT_CONCEPTS = list(_LOADED_CONCEPTS)

# 默认配置
DEFAULT_CONFIG = {
    "feishu_webhook": "",
    "dingtalk_webhook": "",
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

def get_now():
    """获取当前精确时间(统一入口,方便回测时模拟)"""
    return datetime.now()

def is_trading_day(dt=None):
    """
    判断是否为 A 股交易日。
    使用 trading_calendar 模块（含中国节假日+调休）
    """
    try:
        from trading_calendar import is_trading_day as _is_td
        return _is_td(dt)
    except ImportError:
        # fallback: 仅排除周末
        if dt is None:
            dt = get_now()
        return dt.weekday() < 5

def _get_limit_up_threshold(code: str) -> float:
    """
    根据股票代码返回涨停阈值（百分比）。
    主板 10%，科创板/创业板 20%，北交所 30%。
    """
    code = str(code).zfill(6)
    if code.startswith(("688", "689")):      # 科创板
        return 19.8
    if code.startswith(("300", "301")):      # 创业板
        return 19.8
    if code.startswith(("8", "4")):          # 北交所
        return 29.8
    return 9.8                               # 主板


def get_market_session(dt=None):
    """
    返回 (is_in_session, session_name, is_trading_day)

    交易时段:
      上午盘: 9:30:00 - 11:30:00
      下午盘: 13:00:00 - 15:00:00
      集合竞价: 9:15 - 9:25 (不视为盘中交易)
    """
    if dt is None:
        dt = get_now()

    trading = is_trading_day(dt)

    if not trading:
        return False, "周末休市", False

    t = dt.time()
    # 上午盘
    if t.hour == 9 and t.minute >= 30:
        return True, "上午盘", True
    if t.hour == 10:
        return True, "上午盘", True
    if t.hour == 11 and t.minute <= 30:
        return True, "上午盘", True

    # 下午盘
    if t.hour in (13, 14):
        return True, "下午盘", True
    if t.hour == 15 and t.minute == 0 and t.second == 0:
        return True, "下午盘(收盘)", True

    # 非交易时段
    return False, "非交易时段(工作日)", True

def format_session_info():
    """生成会话信息字符串"""
    in_session, session_name, trading = get_market_session()
    now = get_now()
    ts = now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    lines = [
        f"  当前时间: {ts}",
        f"  星期: {['一','二','三','四','五','六','日'][now.weekday()]}",
        f"  交易日: {'是' if trading else '否'}",
        f"  交易时段: {session_name}",
    ]

    if in_session:
        lines.append(f"  📡 盘中模式 - 腾讯首选,不可用则降级+交叉验证")
    else:
        lines.append(f"  📡 盘后模式 - 腾讯首选,允许降级+交叉验证")

    return "\n".join(lines)


# ============================================================
#  自更新机制 - 启动时检测版本、下载替换、自动重启
# ============================================================

def _compare_versions(v1: str, v2: str) -> int:
    """比较语义版本号。返回 1(v1更新), 0(相同), -1(v2更新)"""
    def _parse(v):
        return [int(x) for x in v.replace("v", "").replace("V", "").split(".")]
    try:
        p1, p2 = _parse(v1), _parse(v2)
    except ValueError:
        return 0
    for a, b in zip(p1, p2):
        if a > b: return 1
        if a < b: return -1
    if len(p1) > len(p2): return 1
    if len(p1) < len(p2): return -1
    return 0

def _extract_source_version() -> str | None:
    """从同目录 stock_picker.py 源码中提取 VERSION 字符串"""
    src = BASE_DIR / "stock_picker.py"
    if not src.exists():
        return None
    try:
        with open(src, "r", encoding="utf-8") as f:
            for line in f:
                if line.startswith("VERSION"):
                    # VERSION = "5.9"
                    return line.split("=")[1].strip().strip('"').strip("'")
    except Exception as e:
        log(f"操作异常: {e}", "WARN")
    return None

def _write_restart_bat(new_file: Path, old_file: Path, exe_mode: bool):
    """写重启批处理:等待主进程退出 → 替换文件 → 重新启动"""
    bat_path = BASE_DIR / "_update.bat"
    if not exe_mode:
        # Python 源码模式 - 直接重启
        content = (
            "@echo off\r\n"
            "chcp 936 >nul\r\n"
            "echo Updating stock picker to latest version...\r\n"
            "timeout /t 3 /nobreak >nul\r\n"
            f'"{sys.executable}" "{src}" %*\r\n'
            "exit\r\n"
        )
    else:
        # EXE 模式 - 替换 EXE 后重启
        exe_name = old_file.name
        content = (
            "@echo off\r\n"
            "chcp 936 >nul\r\n"
            "echo Updating stock_picker.exe to latest version...\r\n"
            "echo Waiting for current process to exit...\r\n"
            "timeout /t 5 /nobreak >nul\r\n"
            f'if exist "{new_file}" (\r\n'
            f'    del /F /Q "{old_file}"\r\n'
            f'    move /Y "{new_file}" "{old_file}"\r\n'
            f'    echo Update complete, restarting...\r\n'
            f'    start "" "{old_file}" %*\r\n'
            f') else (\r\n'
            f'    echo Update file not found, aborting.\r\n'
            f'    pause\r\n'
            f')\r\n'
            "exit\r\n"
        )
    with open(bat_path, "w", encoding="gbk") as bat_f:
        bat_f.write(content)
    return bat_path

def _do_exe_update(download_url: str, new_version: str) -> bool:
    """EXE 模式:下载新 EXE → 写重启脚本 → 退出"""
    from pathlib import Path
    _is_frozen = getattr(sys, 'frozen', False)
    if not _is_frozen:
        return False

    exe_path = Path(sys.executable)
    new_exe = BASE_DIR / f"stock_picker_{new_version}.exe.new"

    log(f"发现新版本 v{new_version},正在下载...")
    print(f"\n{'='*60}")
    print(f"  发现新版本 v{new_version}(当前 v{VERSION})")
    print(f"  正在下载更新包...")
    print(f"{'='*60}\n")

    try:
        resp = requests.get(download_url, timeout=300, stream=True)
        if resp.status_code != 200:
            error(f"下载失败 HTTP {resp.status_code}")
            return False
        total = int(resp.headers.get("content-length", 0))
        downloaded = 0
        with open(new_exe, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
                downloaded += len(chunk)
                if total:
                    pct = downloaded * 100 // total
                    print(f"\r  下载进度: {pct}% ({downloaded//1024//1024}MB / {total//1024//1024}MB)", end="")
        print()
        log(f"下载完成: {new_exe} ({downloaded} bytes)")
    except Exception as e:
        error(f"下载异常: {e}")
        return False

    # 写重启脚本并启动
    restart_args = []
    for a in sys.argv[1:]:
        restart_args.append(a)

    bat = _write_restart_bat(new_exe, exe_path, exe_mode=True)
    log(f"重启脚本: {bat}")

    # 启动重启脚本(新控制台窗口)
    import subprocess
    subprocess.Popen(
        f'cmd /c "{bat}" {" ".join(restart_args)}',
        creationflags=subprocess.CREATE_NEW_CONSOLE if hasattr(subprocess, "CREATE_NEW_CONSOLE") else 0,
        shell=True
    )

    log("退出当前进程,移交更新控制权...")
    sys.exit(0)

def _do_source_restart(new_version: str):
    """Python 源码模式:源码版本更新 → 直接重启解释器"""
    src = BASE_DIR / "stock_picker.py"
    if not src.exists():
        return

    log(f"源码已更新至 v{new_version}(当前运行 v{VERSION}),正在重启...")
    print(f"\n{'='*60}")
    print(f"  检测到源码已更新 v{new_version}(当前 v{VERSION})")
    print(f"  自动重启中...")
    print(f"{'='*60}\n")

    import subprocess
    subprocess.Popen([sys.executable, str(src)] + sys.argv[1:])
    sys.exit(0)

def check_for_updates(config: dict):
    """启动时检查更新。返回 True 表示已移交更新流程(主流程应中止)"""
    update_cfg = config.get("update", {})
    if not update_cfg.get("enabled", True):
        return False

    _is_frozen = getattr(sys, 'frozen', False)

    # ====== 方法1:远程版本检测 ======
    check_url = update_cfg.get("check_url", "")
    if check_url:
        try:
            resp = requests.get(check_url, timeout=update_cfg.get("check_timeout", 10))
            if resp.status_code == 200:
                data = resp.json()
                remote_version = data.get("version", "")
                exe_download = data.get("download_exe", "")

                if _compare_versions(remote_version, VERSION) > 0:
                    log(f"远程新版本 v{remote_version} > 当前 v{VERSION}")

                    if _is_frozen and exe_download:
                        return _do_exe_update(exe_download, remote_version)
                    elif not _is_frozen:
                        # 源码模式 - 远程版本更新意味着需要 git pull
                        changelog = data.get("changelog", "")
                        print(f"\n{'='*60}")
                        print(f"  远程新版本 v{remote_version} 可用(当前 v{VERSION})")
                        if changelog:
                            print(f"  更新内容: {changelog}")
                        print(f"  请执行 git pull 获取最新源码后重新运行")
                        print(f"{'='*60}\n")
                        return False  # 不阻断,继续运行当前版本
        except requests.ConnectionError:
            if not update_cfg.get("skip_if_offline", True):
                warn(f"更新检查失败: 网络不可达")
        except Exception as e:
            log(f"远程更新检查异常: {e}")

    # ====== 方法2:本地源码版本检测(仅非冻结模式)======
    if not _is_frozen:
        src_version = _extract_source_version()
        if src_version and _compare_versions(src_version, VERSION) > 0:
            _do_source_restart(src_version)
            return True

    return False


# ============================================================
#  日志系统 - 全链路时间戳
# ============================================================

_log_file_path = None

def _init_log():
    global _log_file_path
    now = get_now()
    _log_file_path = LOG_DIR / f"run_{now.strftime('%Y%m%d_%H%M%S')}.log"

# ===================== 日志系统(v6.0 重构)=====================
# 使用 logger.py 的新日志系统,分层记录:
#   logs/runs/       - 每次运行的完整日志
#   logs/errors/     - 所有错误和警告
#   logs/bugs/       - Bug 追踪(含完整堆栈)
#   logs/data_source/- 数据源获取日志(成功/失败)
#   logs/performance/- 性能日志

# 初始化日志
_logger = init_logger()
_log_file_path = LOG_DIR / f"run_{_logger.run_id}.log"  # 保持向后兼容

def log(msg, level="INFO"):
    """带毫秒时间戳的日志(向后兼容)"""
    global _log_file_path
    if level == "DEBUG":
        _logger.debug(msg)
    elif level == "INFO":
        _logger.info(msg)
    elif level == "WARN":
        _logger.warn(msg)
    elif level == "ERROR":
        _logger.error(msg)
    elif level == "FATAL":
        _logger.fatal(msg)
    else:
        _logger.info(msg)

    # 同时保持旧的输出方式(写入 log 文件)
    if _log_file_path is None:
        _init_log()
    ts = get_now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    line = f"[{ts}] [{level:5s}] {msg}"
    if VERBOSE or level in ("ERROR", "WARN"):
        print(line)
    try:
        with open(_log_file_path, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception as e:
        log(f"操作异常: {e}", "WARN")

def warn(msg):
    log(msg, "WARN")

def error(msg, exception=None):
    """错误日志(支持异常对象)"""
    if exception:
        _logger.error(msg, exception=exception)
    else:
        log(msg, "ERROR")


# ============================================================
#  数据溯源记录 - 每条数据可追溯
# ============================================================

_data_provenance = []  # 数据溯源记录列表

def record_provenance(item_name, source, fetch_time, is_market, extra=None):
    """
    记录一条数据溯源。

    Args:
        item_name: 数据项名称(如 "实时行情")
        source: 数据来源(如 "东方财富")
        fetch_time: datetime 对象,数据抓取时间
        is_market: 是否盘中抓取
        extra: 额外信息 dict(如延迟秒数、记录数等)
    """
    record = {
        "数据项":   item_name,
        "数据来源": source,
        "抓取时间": fetch_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
        "盘中抓取": is_market,
        "延迟秒数": f"{(get_now() - fetch_time).total_seconds():.1f}",
    }
    if extra:
        record.update(extra)
    _data_provenance.append(record)

def print_provenance_report():
    """打印数据溯源报告"""
    lines = []
    lines.append("")
    lines.append("╔" + "═" * 78 + "╗")
    lines.append("║  📋 数据溯源报告")
    lines.append("╠" + "═" * 78 + "╣")
    for r in _data_provenance:
        src = r["数据来源"]
        extra = r.get("额外信息", {})
        cv = extra.get("交叉验证", {})

        # 确定图标
        if src == SRC_EASTMONEY:
            icon = "✓"  # 首选源
        elif cv.get("status") == "ok":
            icon = "✅" # 降级但交叉验证通过
        elif cv.get("status") == "warn":
            icon = "⚠"  # 降级且交叉验证存疑
        elif src == SRC_FAILED:
            icon = "✗"  # 失败
        else:
            icon = "⚠"  # 降级无验证

        cv_info = ""
        if cv:
            cv_info = f" 交叉验证:{cv.get('match_rate','?')}"

        lines.append(
            f"║  {icon} {r['数据项']:10s} │ 来源: {src:8s} │ "
            f"抓取: {r['抓取时间']} │ 盘中: {'是' if r['盘中抓取'] else '否'}"
            f"{cv_info}"
        )
    lines.append("╚" + "═" * 78 + "╝")
    report = "\n".join(lines)
    print(report)
    log("数据溯源报告已生成", "INFO")


# ============================================================
#  股票列表获取
# ============================================================

def fetch_stock_list():
    """获取全量 A 股列表"""
    cache_file = CACHE_DIR / "stock_list.json"
    today = get_now().strftime("%Y%m%d")
    fetch_time = get_now()

    # 读缓存（24小时内有效，支持盘前预热在晚上执行、次日早上复用）
    if cache_file.exists():
        try:
            data = json.loads(cache_file.read_text(encoding="utf-8"))
            cache_date = data.get("date", "")
            cache_dt = datetime.strptime(cache_date, "%Y%m%d")
            age_hours = (fetch_time - cache_dt).total_seconds() / 3600
            if age_hours < 24:
                log(f"股票列表: 缓存 {len(data['stocks'])} 只 [{data.get('source','?')}] 距今{age_hours:.1f}小时")
                record_provenance("股票列表", data.get("source", SRC_CACHE),
                                  datetime.strptime(data.get("fetch_time", today+" 00:00:00"),
                                                    "%Y%m%d %H:%M:%S"),
                                  False, {"记录数": len(data['stocks'])})
                return data["stocks"]
        except Exception as e:
            log(f"缓存读取失败: {e}", "WARN")

    # 东方财富
    stocks = _get_stocks_eastmoney(fetch_time)
    if stocks and len(stocks) >= 1000:
        return stocks

    # 东方财富返回不足1000只（API限制每次100条），降级到新浪
    if stocks and len(stocks) < 1000:
        log(f"东方财富仅返回 {len(stocks)} 只（API限制），降级到新浪...", "WARN")

    # 新浪降级
    stocks = _get_stocks_sina(fetch_time)
    if stocks:
        return stocks

    # 所有 API 均失败，尝试使用过期缓存作为最后兜底
    if cache_file.exists():
        try:
            data = json.loads(cache_file.read_text(encoding="utf-8"))
            cached_stocks = data.get("stocks", [])
            if cached_stocks:
                log(f"所有API失败，使用过期缓存兜底: {len(cached_stocks)} 只 [{data.get('source','?')}]", "WARN")
                return cached_stocks
        except Exception:
            pass

    error("所有数据源都无法获取股票列表!")
    return []


def _get_stocks_eastmoney(fetch_time):
    """东财: 全量 A 股列表"""
    try:
        r = requests.get(EASTMONEY_API, params={
            "pn": "1", "pz": "6000", "po": "1", "np": "1",
            "fltt": "2", "invt": "2", "fid": "f12",
            "fs": "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23",
            "fields": "f12,f14"
        }, headers={"User-Agent": UA}, timeout=15)
        items = r.json().get("data", {}).get("diff", [])
        stocks = []
        for item in items:
            code = item.get("f12", "")
            name = item.get("f14", "")
            if code.startswith(("60", "00", "30", "68")):
                if "ST" not in name and "退" not in name:
                    stocks.append({"code": code, "name": name})

        is_market = get_market_session()[0]
        _save_stocks(stocks, SRC_EASTMONEY, fetch_time)
        record_provenance("股票列表", SRC_EASTMONEY, fetch_time, is_market,
                          {"记录数": len(stocks), "排除": "ST/退市"})
        log(f"股票列表: 东方财富 {len(stocks)} 只")
        return stocks
    except Exception as e:
        warn(f"东方财富股票列表失败: {e}")
        return []


def _get_stocks_sina(fetch_time):
    """新浪: 分页获取"""
    stocks = []
    for exchange in ["sh_a", "sz_a"]:
        for page in range(1, 60):
            try:
                r = requests.get(
                    "http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData",
                    params={"page": page, "num": 100, "sort": "symbol", "asc": 1,
                            "node": exchange, "symbol": "", "_s_r_a": "init"},
                    headers={"User-Agent": UA, "Referer": "https://finance.sina.com.cn"},
                    timeout=10)
                data = json.loads(r.text)
                if not data:
                    break
                for s in data:
                    code = s["code"]
                    name = s["name"]
                    if code.startswith(("60", "00", "30", "68")) and "ST" not in name and "退" not in name:
                        stocks.append({"code": code, "name": name})
            except Exception as e:
                log(f"循环异常: {e}", "WARN")
                break
        time.sleep(0.1)
    seen = set()
    unique = [s for s in stocks if s["code"] not in seen and not seen.add(s["code"])]

    is_market = get_market_session()[0]
    _save_stocks(unique, SRC_SINA, fetch_time)
    record_provenance("股票列表", SRC_SINA, fetch_time, is_market,
                      {"记录数": len(unique), "备注": "东方财富不可用后降级"})
    log(f"股票列表: 新浪 {len(unique)} 只 ⚠ 降级源")
    return unique


def _save_stocks(stocks, source, fetch_time):
    data = {
        "date": get_now().strftime("%Y%m%d"),
        "fetch_time": fetch_time.strftime("%Y%m%d %H:%M:%S"),
        "source": source,
        "stocks": stocks
    }
    (CACHE_DIR / "stock_list.json").write_text(
        json.dumps(data, ensure_ascii=False), encoding="utf-8")


# ============================================================
#  实时行情获取 - 多源降级 + 交叉验证
# ============================================================

def fetch_quotes(stocks):
    """
    获取全量实时行情。
    数据源优先级:腾讯财经(首选)→新浪→东方财富(兜底)。
    腾讯 qt.gtimg.cn 不受防火墙限制,field 46=量比,是稳定的主数据源。
    push2.eastmoney.com 批量行情接口存在TCP阻断,降为末位尝试。
    """
    in_session, session_name, trading = get_market_session()
    fetch_time = get_now()
    mode_str = f"[{'盘中' if in_session else '盘后'}]{session_name}"

    log(f"📡 {mode_str} - 数据获取: 腾讯财经(首选)→新浪→efinance→东方财富(兜底)")

    # 动态阈值：至少获取传入股票列表的 50%，且不少于 100 只
    min_count = max(100, len(stocks) // 2) if stocks else 500

    # === 第1层: 腾讯财经(首选,稳定可用) ===
    df = _get_quotes_tencent(stocks)
    if df is not None and len(df) >= min_count:
        # 交叉验证:抽样对比新浪
        cv_result = _cross_validate_sample(df, stocks, in_session)
        if cv_result["status"] == "ok":
            record_provenance("实时行情", SRC_TENCENT, fetch_time, in_session,
                              {"记录数": len(df), "备注": "腾讯首选,交叉验证通过",
                               "交叉验证": cv_result})
        else:
            record_provenance("实时行情", SRC_TENCENT, fetch_time, in_session,
                              {"记录数": len(df), "备注": "腾讯首选,交叉验证存疑",
                               "交叉验证": cv_result, "⚠": "数据可能不准"})
        return df

    warn(f"⚠ 腾讯财经不可用({mode_str}), 降级到新浪财经...")

    # === 第2层: 新浪财经 ===
    df = _get_quotes_sina()
    if df is not None and len(df) >= min_count:
        cv_result = _cross_validate_sample(df, stocks, in_session)
        if cv_result["status"] == "ok":
            record_provenance("实时行情", SRC_SINA, fetch_time, in_session,
                              {"记录数": len(df), "备注": "腾讯降级,交叉验证通过",
                               "交叉验证": cv_result, "缺失": "量比/市值可能不准"})
        else:
            record_provenance("实时行情", SRC_SINA, fetch_time, in_session,
                              {"记录数": len(df), "备注": "腾讯降级,交叉验证存疑",
                               "交叉验证": cv_result, "⚠": "数据存疑,缺失量比"})
        return df

    warn(f"⚠ 新浪不可用({mode_str}), 尝试efinance...")

    # === 第3层: efinance(东财专用库,接口更稳定) ===
    df = _get_quotes_efinance()
    if df is not None and len(df) >= min_count:
        record_provenance("实时行情", "efinance", fetch_time, in_session,
                          {"记录数": len(df), "备注": "新浪降级,efinance兜底"})
        return df

    warn(f"⚠ efinance不可用({mode_str}), 尝试东方财富兜底...")

    # === 第4层: 东方财富(末位兜底,push2可能被阻断) ===
    df = _get_quotes_eastmoney_with_retry(retries=1)
    if df is not None and len(df) >= min_count:
        record_provenance("实时行情", SRC_EASTMONEY, fetch_time, in_session,
                          {"记录数": len(df), "模式": "末位兜底"})
        return df

    error(f"❌ 所有数据源都无法获取行情! ({mode_str})")
    record_provenance("实时行情", SRC_FAILED, fetch_time, in_session,
                      {"错误": "腾讯/新浪/东方财富全部不可用"})
    return None


def _get_quotes_eastmoney_with_retry(retries=3):
    """东方财富行情获取,支持重试"""
    for attempt in range(retries):
        try:
            df = _get_quotes_eastmoney()
            if df is not None and len(df) > 100:
                return df
            if retries > 1:
                log(f"  东方财富第 {attempt+1}/{retries} 次返回数据不足", "WARN")
        except Exception as e:
            if retries > 1:
                log(f"  东方财富第 {attempt+1}/{retries} 次异常: {e}", "WARN")
        if attempt < retries - 1:
            time.sleep(2)
    return None


# ============================================================
#  交叉验证 - 降级后用另一数据源抽样比对
# ============================================================

def _cross_validate_sample(df_main, stocks, in_session=False):
    """
    降级后交叉验证:从主力数据源抽 sample_size 只,用另一源获取并比对。

    Args:
        df_main: 主力数据源返回的 DataFrame(已确定为降级数据)
        stocks: 股票列表
        in_session: 是否盘中

    Returns:
        dict: {"status": "ok"|"warn"|"skip", "match_rate": 0.95, ...}
    """
    import random

    try:
        # 从 df_main 中随机抽取样本
        codes = df_main["代码"].tolist()
        sample_codes = random.sample(codes, min(CROSS_VALIDATE_SAMPLE, len(codes)))

        # 用新浪做校验源(新浪相对独立且稳定)
        sample_stocks = [s for s in stocks if s["code"] in sample_codes]
        if len(sample_stocks) < 5:
            return {"status": "skip", "reason": "样本不足"}

        df_check = _get_quotes_tencent(sample_stocks)
        if df_check is None or len(df_check) < 3:
            # 腾讯不可用则降级到新浪批量
            df_check = _fetch_sina_sample(sample_stocks)

        if df_check is None or len(df_check) < 3:
            return {"status": "skip", "reason": "校验源不可用", "match_rate": None}

        # 比对关键字段
        matches = 0
        total = 0
        price_diffs = []
        change_diffs = []

        for _, check_row in df_check.iterrows():
            code = check_row["代码"]
            main_row = df_main[df_main["代码"] == code]
            if main_row.empty:
                continue
            main_row = main_row.iloc[0]

            # 比对最新价
            price_main = main_row["最新价"]
            price_check = check_row["最新价"]
            if price_main > 0 and price_check > 0:
                price_diff = abs(price_main - price_check) / price_main
                price_diffs.append(price_diff)

            # 比对涨跌幅
            change_main = main_row["涨跌幅"]
            change_check = check_row["涨跌幅"]
            change_diff = abs(change_main - change_check)
            change_diffs.append(change_diff)

            total += 1
            if price_diff <= CROSS_VALIDATE_PRICE_TOL and change_diff <= CROSS_VALIDATE_CHANGE_TOL:
                matches += 1

        if total == 0:
            return {"status": "skip", "reason": "无可比对样本"}

        match_rate = matches / total
        avg_price_diff = sum(price_diffs) / len(price_diffs) if price_diffs else 0
        avg_change_diff = sum(change_diffs) / len(change_diffs) if change_diffs else 0
        max_price_diff = max(price_diffs) if price_diffs else 0

        result = {
            "status": "ok" if match_rate >= 0.85 else "warn",
            "match_rate": round(match_rate, 3),
            "sample_size": total,
            "avg_price_deviation": f"{avg_price_diff*100:.2f}%",
            "avg_change_deviation": f"{avg_change_diff:.2f}pp",
            "max_price_deviation": f"{max_price_diff*100:.2f}%",
        }

        if result["status"] == "ok":
            log(f"  ✅ 交叉验证通过 (匹配率 {match_rate:.0%}, 样本 {total})")
        else:
            warn(f"  ⚠ 交叉验证存疑 (匹配率 {match_rate:.0%}, 样本 {total}, "
                 f"均价偏差 {avg_price_diff*100:.2f}%)")

        return result

    except Exception as e:
        return {"status": "skip", "reason": f"验证异常: {e}", "match_rate": None}


def _fetch_sina_sample(stocks):
    """用小批量接口获取样本数据(用于交叉验证) — 使用统一K线获取层"""
    from kline_fetcher import KlineFetcher
    fetcher = KlineFetcher()
    all_data = []
    for s in stocks:
        try:
            code = s["code"]
            df = fetcher.get_kline(code, days=5, adjust="qfq")
            if df is not None and len(df) > 0:
                last = df.iloc[-1]
                all_data.append({
                    "代码": code,
                    "名称": s.get("name", ""),
                    "最新价": float(last.get("收盘", 0)),
                    "涨跌幅": 0,
                    "换手率": 0,
                    "量比": 1.0,
                    "最高": float(last.get("最高", 0)),
                    "最低": float(last.get("最低", 0)),
                    "今开": float(last.get("开盘", 0)),
                    "昨收": float(last.get("收盘", 0)),
                    "总市值": 0,
                    "流通市值": 0,
                    "行业": "",
                    "市盈率动态": 0,
                })
        except Exception as e:
            log(f"循环异常: {e}", "WARN")
            continue
    if not all_data:
        return None
    return pd.DataFrame(all_data)


def _fetch_industry_eastmoney_datacenter(codes: list) -> dict:
    """用东方财富 datacenter API 获取行业分类（全量分页，几秒内完成）
    返回 {code: "通用设备"} 格式（取第二级细分行业）
    """
    result = {}
    code_set = {str(c).zfill(6) for c in codes}
    try:
        for page in range(1, 60):
            r = requests.get(
                "https://datacenter-web.eastmoney.com/api/data/v1/get",
                params={
                    "reportName": "RPT_F10_BASIC_ORGINFO",
                    "columns": "SECURITY_CODE,EM2016",
                    "pageSize": "500",
                    "pageNumber": str(page),
                },
                headers={"User-Agent": UA},
                timeout=15
            )
            data = r.json()
            if not data.get("success") or not data.get("result", {}).get("data"):
                break
            for item in data["result"]["data"]:
                code = str(item.get("SECURITY_CODE", "")).zfill(6)
                industry = item.get("EM2016", "")
                if code in code_set and industry:
                    # "机械设备-通用设备-制冷空调设备" → "通用设备"
                    parts = industry.split("-")
                    result[code] = parts[1] if len(parts) >= 2 else industry
            # 如果已找到所有需要的code，提前退出
            if code_set.issubset(result.keys()):
                break
            time.sleep(0.05)
        log(f"东方财富行业API: 获取 {len(result)}/{len(code_set)} 只")
    except Exception as e:
        log(f"东方财富行业获取异常: {e}", "WARN")
    return result


def _fetch_industry_eastmoney(codes: list) -> dict:
    """用东方财富批量API获取行业分类（只取f100字段，速度快）"""
    result = {}
    code_set = {str(c).zfill(6) for c in codes}
    try:
        for page in range(1, 10):
            r = requests.get(EASTMONEY_API, params={
                "pn": str(page), "pz": "1000", "po": "1", "np": "1",
                "fltt": "2", "invt": "2", "fid": "f3",
                "fs": "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23",
                "fields": "f12,f100"
            }, headers={"User-Agent": UA}, timeout=15)
            items = r.json().get("data", {}).get("diff", [])
            if not items:
                break
            for item in items:
                code = str(item.get("f12", "")).zfill(6)
                industry = item.get("f100", "")
                if code in code_set and industry:
                    result[code] = industry
            # 如果已经找到了所有需要的code，提前退出
            if code_set.issubset(result.keys()):
                break
            time.sleep(0.1)
    except Exception as e:
        log(f"东方财富行业获取异常: {e}", "WARN")
    return result


def _get_quotes_eastmoney():
    """东财实时行情"""
    try:
        all_data = []
        for page in range(1, 6):
            r = requests.get(EASTMONEY_API, params={
                "pn": str(page), "pz": "1000", "po": "1", "np": "1",
                "fltt": "2", "invt": "2", "fid": "f3",
                "fs": "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23",
                "fields": "f2,f3,f5,f6,f7,f8,f10,f12,f14,f15,f16,f17,f18,f20,f21,f100,f115"
            }, headers={"User-Agent": UA}, timeout=15)
            items = r.json().get("data", {}).get("diff", [])
            if not items:
                break
            for item in items:
                name = item.get("f14", "")
                if "ST" in name or "退" in name:
                    continue
                try:
                    all_data.append({
                        "代码": item.get("f12", ""),
                        "名称": name,
                        "最新价": float(item.get("f2", 0) or 0),
                        "涨跌幅": float(item.get("f3", 0) or 0),
                        "成交量": float(item.get("f5", 0) or 0),
                        "成交额": float(item.get("f6", 0) or 0),
                        "振幅":   float(item.get("f7", 0) or 0),
                        "换手率": float(item.get("f8", 0) or 0),
                        "量比":   float(item.get("f10", 0) or 1.0),
                        "最高":   float(item.get("f15", 0) or 0),
                        "最低":   float(item.get("f16", 0) or 0),
                        "今开":   float(item.get("f17", 0) or 0),
                        "昨收":   float(item.get("f18", 0) or 0),
                        "总市值": float(item.get("f20", 0) or 0),
                        "流通市值": float(item.get("f21", 0) or 0),
                        "行业":   item.get("f100", ""),
                        "市盈率动态": float(item.get("f115", 0) or 0),
                    })
                except (ValueError, TypeError):
                    continue
            time.sleep(0.15)

        if not all_data:
            return None
        df = pd.DataFrame(all_data)
        log(f"实时行情: 东方财富 {len(df)} 只")
        return df
    except Exception as e:
        warn(f"东方财富行情获取异常: {e}")
        return None


def _get_quotes_tencent(stocks):
    """腾讯批量行情"""
    all_data = []
    for i in range(0, len(stocks), 80):
        batch = stocks[i:i+80]
        codes = ",".join([
            f"sh{s['code']}" if s['code'].startswith(("60","68")) else f"sz{s['code']}"
            for s in batch
        ])
        try:
            r = requests.get(TENCENT_API.format(codes=codes),
                           headers={"User-Agent": UA}, timeout=10)
            for line in r.text.strip().split(";"):
                if "=" not in line:
                    continue
                _, val = line.split("=", 1)
                f = val.strip('";\n').split("~")
                if len(f) < 40:
                    continue
                try:
                    all_data.append({
                        "代码": f[2], "名称": f[1],
                        "最新价": float(f[3]) if f[3] else 0,
                        "涨跌幅": float(f[32]) if f[32] else 0,
                        "换手率": float(f[38]) if f[38] else 0,
                        "量比": float(f[46]) if len(f) > 46 and f[46] and 0 < float(f[46]) < 100 else 1.0,
                        "最高": float(f[33]) if f[33] else 0,
                        "最低": float(f[34]) if f[34] else 0,
                        "今开": float(f[5]) if f[5] else 0,
                        "昨收": float(f[4]) if f[4] else 0,
                        "总市值": float(f[45])*1e8 if f[45] else 0,
                        "流通市值": float(f[47])*1e8 if f[47] else 0,
                        "行业": "",
                        "市盈率动态": float(f[39]) if f[39] else 0,
                        "市净率": float(f[44]) if len(f) > 44 and f[44] else 0,
                    })
                except (ValueError, IndexError):
                    continue
        except Exception as e:
            log(f"循环异常: {e}", "WARN")
            continue
    if not all_data:
        return None
    df = pd.DataFrame(all_data)
    lb_valid = (df['量比'] != 1.0).sum()
    log(f"实时行情: 腾讯 {len(df)} 只 ⚠ 量比有效: {lb_valid}/{len(df)}")
    return df


def _get_quotes_sina():
    """新浪行情(兜底)"""
    all_data = []
    for page in range(1, 50):
        try:
            r = requests.get(
                "http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData",
                params={"page": page, "num": 80, "sort": "changepercent", "asc": 0,
                        "node": "hs_a", "symbol": "", "_s_r_a": "init"},
                headers={"User-Agent": UA, "Referer": "https://finance.sina.com.cn"},
                timeout=10)
            data = json.loads(r.text)
            if not data:
                break
            for s in data:
                if "ST" in s.get("name", "") or "退" in s.get("name", ""):
                    continue
                all_data.append({
                    "代码": s["code"], "名称": s["name"],
                    "最新价": float(s.get("trade", 0)),
                    "涨跌幅": float(s.get("changepercent", 0)),
                    "换手率": float(s.get("turnoverratio", 0)),
                    "量比": 1.0,
                    "最高": float(s.get("high", 0)),
                    "最低": float(s.get("low", 0)),
                    "今开": float(s.get("open", 0)),
                    "昨收": float(s.get("settlement", 0)),
                    "总市值": float(s.get("mktcap", 0))*10000 if s.get("mktcap") else 0,
                    "流通市值": float(s.get("nmc", 0))*10000 if s.get("nmc") else 0,
                    "行业": "",
                    "市盈率动态": float(s.get("per", 0)),
                })
        except Exception as e:
            log(f"循环异常: {e}", "WARN")
            break
        time.sleep(0.2)
    if not all_data:
        return None
    df = pd.DataFrame(all_data)
    log(f"实时行情: 新浪 {len(df)} 只 ⚠ 兜底源")
    return df


def _get_quotes_efinance():
    """efinance实时行情 — 东方财富专用库，接口更稳定"""
    try:
        import efinance as ef
    except ImportError:
        return None

    try:
        df = ef.stock.get_realtime_quotes()
        if df is None or len(df) == 0:
            return None
        # efinance列名映射
        col_map = {
            "股票代码": "代码", "股票名称": "名称",
            "涨跌幅": "涨跌幅", "最新价": "最新价",
            "最高": "最高", "最低": "最低", "今开": "今开",
            "涨跌额": "_涨跌额", "换手率": "换手率",
            "量比": "量比", "动态市盈率": "市盈率动态",
            "成交量": "成交量", "成交额": "成交额",
            "总市值": "总市值", "流通市值": "流通市值",
        }
        df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
        # 确保数值类型
        for col in ["最新价", "涨跌幅", "换手率", "最高", "最低", "今开", "市盈率动态"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
        # 昨收 = 最新价 - 涨跌额（efinance没有昨收列）
        if "昨收" not in df.columns and "最新价" in df.columns and "_涨跌额" in df.columns:
            df["昨收"] = df["最新价"] - pd.to_numeric(df["_涨跌额"], errors="coerce").fillna(0)
        elif "昨收" not in df.columns:
            df["昨收"] = df.get("最新价", 0)
        # 过滤无效数据
        df = df[df["最新价"] > 0]
        log(f"实时行情: efinance {len(df)} 只")
        return df
    except Exception as e:
        log(f"efinance行情异常: {e}", "WARN")
        return None


def _get_quotes_baidu():
    """百度股市通实时行情 — 单只查询，作为补充源"""
    try:
        import requests
    except ImportError:
        return None

    try:
        # 百度股市通只能单只查询，先获取A股列表再批量查
        # 这里简化为：从已知的股票池中抽样查询
        # 实际使用时需要传入股票列表，但为保持接口一致，这里返回None
        # 如果要实现全市场，需要维护一个股票代码列表循环查询
        return None
    except Exception as e:
        log(f"百度股市通行情异常: {e}", "WARN")
        return None


def _get_baidu_single(code):
    """百度股市通单只股票盘口数据"""
    try:
        import requests
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": "https://gushitong.baidu.com/",
        }
        params = {
            "openapi": "1", "dspName": "iphone", "tn": "tangram",
            "client": "app", "query": code, "code": code, "word": code,
            "resource_id": "5429", "ma_ver": "4", "finClientType": "pc",
        }
        r = requests.get("https://gushitong.baidu.com/opendata", params=params, headers=headers, timeout=10)
        if r.status_code != 200:
            return None
        data = r.json()
        pankou = data['Result'][1]['DisplayData']['resultData']['tplData']['result']['minute_data']['pankouinfos']['origin_pankou']
        return {
            "代码": code,
            "名称": "",
            "最新价": float(pankou.get("currentPrice", 0)),
            "涨跌幅": 0,  # 百度接口没有直接返回涨跌幅
            "换手率": float(pankou.get("turnoverRatio", 0)),
            "量比": float(pankou.get("volumeRatio", 0)),
            "最高": float(pankou.get("high", 0)),
            "最低": float(pankou.get("low", 0)),
            "今开": float(pankou.get("open", 0)),
            "昨收": float(pankou.get("preClose", 0)),
            "总市值": float(pankou.get("capitalization", 0)),
            "流通市值": float(pankou.get("currencyValue", 0)),
            "市盈率动态": float(pankou.get("peratio", 0)),
            "行业": "",
        }
    except Exception:
        return None


# ============================================================
#  历史 K 线获取（统一接口，多源自动降级）
# ============================================================
from kline_fetcher import KlineFetcher

_kline_fetcher_stock = KlineFetcher()


def fetch_kline(code, days=60):
    """获取单只股票日K线（统一接口，多源自动降级）"""
    return _kline_fetcher_stock.get_kline(code, days=days, adjust="qfq")


# ============================================================
#  概念板块映射
# ============================================================

def fetch_concept_map(all_codes):
    """获取股票→概念标签映射 - 直接跳过被阻断的东财API"""
    cache_file = CACHE_DIR / "concept_map.json"
    fetch_time = get_now()
    market = get_market_session()[0]

    # ── 直接读缓存(当日有效) ──
    if cache_file.exists():
        try:
            data = json.loads(cache_file.read_text(encoding="utf-8"))
            cache_date = data.get("date", "")
            today = get_now().strftime("%Y%m%d")
            if cache_date == today:
                cache_map = data.get("map", {})
                log(f"概念映射: 缓存 {len(cache_map)} 只")
                record_provenance("概念板块", SRC_CACHE, fetch_time, market,
                              {"覆盖股票": len(cache_map)})
                return cache_map
        except Exception as e:
            log(f"操作异常: {e}", "WARN")

    # ── 缓存无效,使用增强关键词匹配 ──
    warn("东方财富概念API不可用(push2被阻断),使用增强关键词匹配(40+类)+硬编码")
    result = _keyword_concept_map(all_codes)
    record_provenance("概念板块", SRC_LOCAL_KW, fetch_time, market,
                  {"覆盖股票": len(result), "备注": "增强版40+类关键词+知名个股硬编码"})
    return result


def _keyword_concept_map(all_codes):
    """本地关键词兜底 - 增强版(40+类) + 知名个股硬编码 + 缓存"""
    import json
    from datetime import datetime

    # ── 知名个股硬编码(覆盖大部分热门股) ──
    # 支持从 cache/well_known_stocks.json 加载扩展映射，方便手动维护
    _wks_file = CACHE_DIR / "well_known_stocks.json"
    _loaded_wks = {}
    if _wks_file.exists():
        try:
            import json as _json
            _loaded_wks = _json.loads(_wks_file.read_text(encoding="utf-8"))
        except Exception as e:
            log(f"操作异常: {e}", "WARN")

    WELL_KNOWN_STOCK_MAP = {
        # 白酒
        "600519": ["白酒", "消费"], "000858": ["五粮液", "白酒", "消费"], "000568": ["白酒", "消费"],
        "600809": ["白酒", "消费"], "000596": ["白酒", "消费"], "002304": ["白酒", "消费"],
        # 新能源
        "300750": ["新能源", "锂电池", "新能源汽车"], "002594": ["新能源", "新能源汽车", "比亚迪"],
        "300274": ["新能源", "光伏", "逆变器"], "601012": ["新能源", "光伏", "单晶硅"],
        "600438": ["新能源", "光伏"], "688223": ["新能源", "光伏"],
        "300763": ["新能源", "光伏"], "688599": ["新能源", "光伏"],
        # 半导体
        "688981": ["半导体", "芯片", "晶圆代工"], "002371": ["半导体", "芯片", "设备"],
        "603501": ["半导体", "芯片", "设计"], "600703": ["半导体", "光电", "LED"],
        "300661": ["半导体", "芯片"], "688012": ["半导体", "芯片设备"],
        "688008": ["半导体", "芯片"], "603986": ["半导体", "芯片"],
        "300782": ["半导体", "芯片"],
        # AI/算力
        "000977": ["AI", "算力", "服务器"], "002230": ["AI", "语音", "人工智能"],
        "300308": ["AI", "光通信", "光模块"], "300502": ["AI", "光通信", "光模块"],
        "688041": ["AI", "算力", "芯片"], "603019": ["AI", "算力", "服务器"],
        "000938": ["AI", "算力"], "688256": ["AI", "芯片", "寒武纪"],
        "300624": ["AI", "应用"], "300418": ["AI", "应用"],
        # 光通信/CPO
        "002491": ["光通信", "光纤", "光缆", "通信"], "600487": ["光通信", "光纤", "光缆"],
        "600498": ["光通信", "5G", "通信设备"], "000063": ["5G", "通信设备", "通信"],
        "300394": ["光通信", "CPO"], "300502": ["光通信", "CPO", "光模块"],
        "300308": ["光通信", "CPO", "光模块"], "688313": ["光通信", "CPO"],
        # 机器人
        "002230": ["机器人", "AI", "语音"], "300124": ["机器人", "伺服", "自动化"],
        "688160": ["机器人", "减速器"], "002747": ["机器人", "减速器"],
        "300024": ["机器人", "自动化"], "002906": ["机器人", "视觉"],
        # 军工/低空
        "600760": ["军工", "航空装备"], "600893": ["军工", "航空发动机"],
        "603261": ["军工", "航空装备", "低空经济"], "002085": ["低空经济", "飞行汽车"],
        "000099": ["低空经济", "航空"], "600862": ["低空经济", "航空装备"],
        "600185": ["军工", "航空装备"], "600391": ["军工", "航空装备"],
        "600879": ["军工", "航天"],
        # 医药
        "600276": ["创新药", "医药"], "300760": ["医疗器械", "医疗"],
        "000538": ["中药", "医药"], "000963": ["医药商业", "医药"],
        "300122": ["CXO", "医药"], "603259": ["CXO", "医药"],
        "300015": ["医疗", "眼科"], "300347": ["医疗", "医美"],
        "000661": ["创新药", "医药"],
        # 金融
        "601318": ["保险", "金融"], "600036": ["银行", "金融"],
        "601398": ["银行", "金融"], "601939": ["银行", "金融"],
        "601288": ["银行", "金融"], "601988": ["银行", "金融"],
        "600000": ["银行", "金融"], "600030": ["证券", "券商"],
        "601211": ["证券", "券商"], "601688": ["证券", "券商"],
        "600837": ["证券", "券商"], "300059": ["证券", "互联网金融"],
        # 地产/基建
        "000002": ["房地产", "地产"], "601668": ["建筑", "基建"],
        "601390": ["建筑", "基建"], "601186": ["建筑", "基建"],
        "601800": ["建筑", "基建"],
        # 家电
        "000333": ["家电", "白电"], "000651": ["家电", "白电"],
        "002032": ["家电", "厨电"], "000100": ["家电", "面板"],
        # 食品/消费
        "600887": ["乳业", "食品饮料"], "603288": ["调味品", "食品饮料"],
        "000895": ["食品", "食品饮料"],
        # 周期/资源
        "601088": ["煤炭", "周期"], "600188": ["煤炭", "周期"],
        "600348": ["煤炭", "周期"], "601899": ["有色金属", "黄金"],
        "600547": ["黄金", "有色金属"], "600019": ["钢铁", "周期"],
        # 汽车
        "600104": ["汽车", "整车"], "000625": ["汽车", "整车"],
        "601238": ["汽车", "整车"], "600741": ["汽车零部件"],
        "002920": ["汽车零部件"], "601689": ["汽车零部件"],
        # 科技其他
        "002415": ["安防", "AI"], "002555": ["互联网", "游戏"],
        "600588": ["软件", "信创"], "002439": ["软件", "信创"],
        "600941": ["通信", "运营商"], "600050": ["通信", "运营商"],
        "600028": ["石油", "国企"], "601857": ["石油", "国企"],
        # 电力/公用
        "600900": ["电力", "水电"], "601985": ["核电", "电力"],
        "600886": ["电力"], "600011": ["电力", "火电"],
        "600025": ["电力", "水电"], "600905": ["电力", "风电"],
        # 交通物流
        "601919": ["航运", "交通"], "601006": ["铁路", "交通"],
        "600029": ["航空", "交通"], "002352": ["快递", "物流"],
        # 储能/固态电池
        "300750": ["储能", "锂电池", "新能源"], "300274": ["储能", "逆变器"],
        "002074": ["储能", "锂电池"],
        # 国产替代
        "688981": ["国产替代", "半导体"], "002371": ["国产替代", "半导体"],
        "000977": ["国产替代", "AI"],
    }
    # 合并外部持久化映射（JSON 中的条目优先级更高，允许覆盖和扩展）
    if _loaded_wks:
        WELL_KNOWN_STOCK_MAP = {**WELL_KNOWN_STOCK_MAP, **_loaded_wks}
    else:
        # 首次运行：将硬编码默认值持久化到 JSON，方便后续手动扩展
        try:
            import json as _json
            _wks_file.write_text(_json.dumps(WELL_KNOWN_STOCK_MAP, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception as e:
            log(f"操作异常: {e}", "WARN")

    # ── 扩展概念关键词(55+类) ──
    ENHANCED_KEYWORDS = {
        # 科技 (15类)
        "AI": ["智能", "AI", "人工智能", "数据", "算力", "算法", "大模型", "视觉", "语音", "识图", "机器"],
        "半导体": ["半导体", "芯片", "晶圆", "封测", "微电子", "集成电路", "IC", "光刻", "EDA"],
        "机器人": ["机器人", "自动化", "减速器", "伺服", "数控", "机器视觉", "机械手", "工业母机"],
        "光通信": ["光通信", "光模块", "光纤", "光缆", "光器件", "光芯片", "光互连"],
        "通信": ["通信", "电信", "5G", "6G", "基站", "射频", "星链", "卫星通信"],
        "软件": ["软件", "SAAS", "SaaS", "ERP", "OA", "CRM", "信息化", "数字化"],
        "信创": ["信创", "自主可控", "国产系统", "国产软件", "操作系统", "数据库"],
        "消费电子": ["消费电子", "手机", "耳机", "可穿戴", "屏幕", "面板", "VR", "AR", "MR"],
        "算力": ["算力", "服务器", "数据中心", "IDC", "GPU", "CPU"],
        "PCB": ["PCB", "电路板", "印制电路", "覆铜板"],
        "CPO": ["CPO", "共封装", "硅光"],
        "数字经济": ["数字", "数据要素", "数字经济"],
        "华为": ["华为", "鸿蒙", "海思", "昇腾", "鲲鹏"],
        "国产替代": ["国产替代", "进口替代", "自主"],
        "储能": ["储能", "储能系统", "PCS", "BMS", "EMS"],
        # 新能源 (6类)
        "新能源": ["新能源", "风光", "光伏", "风电", "储能", "氢能", "锂电", "电池", "能源"],
        "锂电池": ["锂电", "锂电池", "正极", "负极", "电解液", "隔膜", "锂矿", "碳酸锂"],
        "光伏": ["光伏", "太阳能", "单晶硅", "多晶硅", "电池片", "组件", "逆变器", "硅片"],
        "新能源汽车": ["新能源汽车", "电动车", "充电桩", "换电", "电动"],
        "氢能": ["氢能", "氢气", "制氢", "储氢", "加氢"],
        "固态电池": ["固态电池", "凝聚态", "半固态"],
        # 高端制造 (5类)
        "军工": ["军工", "航天", "航空", "导弹", "卫星", "雷达", "船舶", "军用", "防务"],
        "低空经济": ["低空", "飞行汽车", "eVTOL", "无人机"],
        "商业航天": ["航天", "火箭", "卫星", "星座", "太空"],
        "高端装备": ["高端装备", "精密", "数控机床", "工业母机", "减速器", "重工"],
        "新材料": ["新材料", "碳纤维", "复合材料", "磁性材料", "高分子", "纳米"],
        # 医药 (6类)
        "创新药": ["创新药", "新药", "靶向", "抗体", "ADC", "双抗", "CAR-T", "单抗"],
        "医疗器械": ["医疗", "器械", "诊断", "耗材", "IVD", "POCT", "CT", "核磁"],
        "CXO": ["CXO", "CRO", "CDMO", "医药外包", "临床"],
        "生物医药": ["生物", "基因", "制药", "疫苗", "血制品", "重组"],
        "中药": ["中药", "中药材", "中成药", "国药"],
        "医美": ["医美", "美容", "玻尿酸", "胶原蛋白"],
        # 消费 (6类)
        "白酒": ["白酒", "酒", "茅台", "五粮液", "泸州", "汾酒", "古井", "酒鬼"],
        "食品饮料": ["食品", "饮料", "啤酒", "乳业", "调味品", "预制菜", "速冻", "烘焙"],
        "家电": ["家电", "白电", "黑电", "小家电", "厨电", "空调", "冰箱", "洗衣机"],
        "电商": ["电商", "电子商务", "跨境", "直播", "网购"],
        "教育": ["教育", "培训", "在线教育", "职业教育"],
        "旅游": ["旅游", "酒店", "景区", "餐饮", "免税"],
        # 周期 (6类)
        "煤炭": ["煤炭", "焦煤", "无烟煤", "动力煤"],
        "钢铁": ["钢铁", "钢材", "特钢", "不锈钢"],
        "有色金属": ["有色", "铜", "铝", "锌", "铅", "镍", "钴", "锂", "稀土", "黄金", "钨", "钼"],
        "石油化工": ["石油", "石化", "化工", "炼化", "PTA", "MDI", "钛白粉", "化肥"],
        "房地产": ["房地产", "地产", "物业"],
        "建筑": ["建筑", "基建", "工程", "施工", "路桥", "水利", "钢结构"],
        # 基础设施 (4类)
        "电力": ["电力", "发电", "电网", "火电", "水电", "核电", "风电", "光伏发电"],
        "交通": ["运输", "铁路", "公路", "港口", "航运", "航空", "物流", "快递", "机场"],
        "环保": ["环保", "水务", "污水处理", "碳中和", "节能"],
        "农业": ["农业", "种业", "种植", "养殖", "农林", "农化", "农机"],
        # 金融 (3类)
        "银行": ["银行", "商业银行", "农商行", "城商行", "股份制"],
        "证券": ["证券", "券商", "投行", "期货"],
        "保险": ["保险", "人寿", "财险", "再保险"],
        # TMT/其他 (5类)
        "互联网": ["互联网", "网络", "平台", "云", "云计算", "大数据"],
        "游戏": ["游戏", "手游", "网游", "电竞", "云游戏"],
        "安防": ["安防", "安全", "监控"],
        "传媒": ["传媒", "广告", "影视", "出版", "广电"],
        "电子": ["电子", "元器件", "连接器", "传感器", "MLCC"],
        # 政策主题 (6类)
        "国企改革": ["国企", "央企", "国有", "国资", "中字头", "改革"],
        "一带一路": ["一带一路", "丝绸之路"],
        "专精特新": ["专精特新", "小巨人", "单项冠军"],
        "高股息": ["高股息", "红利", "分红"],
        "核聚变": ["聚变", "核聚变", "可控核聚变"],
        "量子科技": ["量子", "量子计算", "量子通信", "量子芯片"],
        # 新概念 (5类)
         "可控核聚变": ["聚变", "核聚变", "可控核聚变", "人造太阳"],
         "智能驾驶": ["智能驾驶", "自动驾驶", "无人驾驶", "ADAS", "激光雷达"],
         "液冷": ["液冷", "散热", "温控"],
    }

    # ── 尝试从缓存读取 ──
    concept_cache_file = CACHE_DIR / "concept_map.json"
    if concept_cache_file.exists():
        try:
            data = json.loads(concept_cache_file.read_text(encoding="utf-8"))
            cache_date = data.get("date", "")
            today = datetime.now().strftime("%Y%m%d")
            if cache_date == today:
                cache_map = data.get("map", {})
                # 补足缺失的
                result = dict(cache_map)
                needs = [(s["code"], s["name"]) for s in all_codes if s["code"] not in result]
                if needs:
                    for code, name in needs:
                        concepts = set()
                        if code in WELL_KNOWN_STOCK_MAP:
                            concepts.update(WELL_KNOWN_STOCK_MAP[code])
                        name_lower = name.lower()
                        for concept, kws in ENHANCED_KEYWORDS.items():
                            if any(kw.lower() in name_lower for kw in kws):
                                concepts.add(concept)
                        if concepts:
                            result[code] = list(concepts)
                log(f"概念映射: 缓存+补全 {len(result)} 只")
                return result
        except Exception as e:
            log(f"操作异常: {e}", "WARN")

    # ── 重新构建 ──
    result = {}
    for s in all_codes:
        code, name = s["code"], s["name"]
        concepts = set()
        if code in WELL_KNOWN_STOCK_MAP:
            concepts.update(WELL_KNOWN_STOCK_MAP[code])
        name_lower = name.lower()
        for concept, kws in ENHANCED_KEYWORDS.items():
            if any(kw.lower() in name_lower for kw in kws):
                concepts.add(concept)
        if concepts:
            result[code] = list(concepts)

    # ── 写缓存 ──
    try:
        concept_cache_file.write_text(
            json.dumps({"date": datetime.now().strftime("%Y%m%d"), "map": result}, ensure_ascii=False),
            encoding="utf-8")
    except Exception as e:
        log(f"操作异常: {e}", "WARN")

    # ── 用 Baostock 行业分类补充硬编码映射 ──
    try:
        from baostock_data import BaostockData
        bd = BaostockData()
        industry_map = bd.get_industry_map(list(result.keys()))
        for code, industry in industry_map.items():
            if code in result and industry and industry not in result[code]:
                result[code].append(industry)
        if industry_map:
            log(f"概念映射: Baostock 行业分类补充 {len(industry_map)} 只")
    except Exception as e:
        log(f"操作异常: {e}", "WARN")

    log(f"概念映射: 增强关键词匹配 {len(result)} 只")
    return result


def _emerging_concept_sync(concept_map, data_source_name):
    """
    自动发现新兴概念并合并到 HOT_CONCEPTS。
    原理:扫描东方财富返回的所有概念名称,统计每只概念覆盖了多少股票。
    高频出现(>=3只)且不在当前 HOT_CONCEPTS 中的 → 自动补充。
    同时写回 config.json 持久化。
    """
    global HOT_CONCEPTS

    if not concept_map:
        return

    # 统计每个概念的覆盖股票数
    concept_freq = {}
    for code, concepts in concept_map.items():
        for c in concepts:
            concept_freq[c] = concept_freq.get(c, 0) + 1

    # 找出新兴概念:>=3只股票覆盖,且不在当前的 HOT_CONCEPTS 和 _DEFAULT_HOT_CONCEPTS 中
    all_known = set(HOT_CONCEPTS) | set(_DEFAULT_HOT_CONCEPTS)
    emerging = []
    for c_name, freq in sorted(concept_freq.items(), key=lambda x: -x[1]):
        if c_name not in all_known and freq >= 3:
            emerging.append((c_name, freq))

    if not emerging:
        return

    # 加入运行时 HOT_CONCEPTS
    new_names = [e[0] for e in emerging]
    HOT_CONCEPTS.extend(new_names)
    log(f"🌱 发现 {len(emerging)} 个新兴概念(数据源: {data_source_name}):")
    for name, freq in emerging[:10]:
        log(f"   + {name}(覆盖 {freq} 只)")

    # 同步到 config.json
    _write_hot_concepts_to_config()


def _write_hot_concepts_to_config():
    """把当前 HOT_CONCEPTS 写回 config.json 的 hot_concepts 字段"""
    global HOT_CONCEPTS

    if not CONFIG_FILE.exists():
        return

    try:
        cfg = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
        cfg["hot_concepts"] = list(dict.fromkeys(HOT_CONCEPTS))  # 去重保序
        CONFIG_FILE.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
        log(f"   📝 已同步 {len(HOT_CONCEPTS)} 个概念到 config.json")
    except Exception as e:
        log(f"   ⚠ 写入 config.json 失败: {e}", "WARN")


# ============================================================
#  技术指标计算
# ============================================================

def calc_technical_indicators(code):
    """计算单只股票技术指标(MACD/RSI/BOLL/均线)"""
    df = fetch_kline(code, 60)
    if df is None or len(df) < 20:
        return {}

    close = df["收盘"].values

    # 均线
    ma5  = pd.Series(close).rolling(5).mean().iloc[-1]
    ma10 = pd.Series(close).rolling(10).mean().iloc[-1]
    ma20 = pd.Series(close).rolling(20).mean().iloc[-1]
    ma60 = pd.Series(close).rolling(60).mean().iloc[-1] if len(close) >= 60 else 0
    last_close = close[-1]

    # MACD
    ema12 = pd.Series(close).ewm(span=12, adjust=False).mean()
    ema26 = pd.Series(close).ewm(span=26, adjust=False).mean()
    dif = ema12 - ema26
    dea = dif.ewm(span=9, adjust=False).mean()
    macd_bar = 2 * (dif.iloc[-1] - dea.iloc[-1])

    # RSI
    delta = pd.Series(close).diff()
    gain = delta.clip(lower=0).ewm(com=13, adjust=False).mean().iloc[-1]
    loss = (-delta.clip(upper=0)).ewm(com=13, adjust=False).mean().iloc[-1]
    rsi = 100 - 100 / (1 + gain / (loss + 1e-9))

    # 布林带
    mid = pd.Series(close).rolling(20).mean().iloc[-1]
    std = pd.Series(close).rolling(20).std().iloc[-1]
    bb_upper = mid + 2 * std
    bb_lower = mid - 2 * std
    bb_pct = (last_close - bb_lower) / (bb_upper - bb_lower + 1e-9)

    # 5日涨幅
    chg_5d = (last_close / close[-6] - 1) if len(close) >= 6 else 0

    return {
        "ma5": ma5, "ma10": ma10, "ma20": ma20, "ma60": ma60,
        "close": last_close,
        "macd_dif": dif.iloc[-1], "macd_dea": dea.iloc[-1], "macd_bar": macd_bar,
        "rsi": rsi,
        "bb_upper": bb_upper, "bb_lower": bb_lower, "bb_pct": bb_pct,
        "chg_5d": chg_5d,
    }


# ============================================================
#  因子计算 - 百分位标准化 + 加权求和
# ============================================================

def _calc_one_tech(idx, code, name):
    """并行计算单只股票技术指标"""
    indicators = calc_technical_indicators(code)
    if not indicators:
        return idx, None
    close  = indicators["close"]
    ma20   = indicators["ma20"]
    m_bar  = indicators["macd_bar"]
    rsi    = indicators["rsi"]
    chg_5d = indicators["chg_5d"]
    result = {}
    if ma20 > 0:
        result["趋势_均线位置"] = min(max((close / ma20 - 1) * 10, -1), 1) * 0.5 + 0.5
    else:
        result["趋势_均线位置"] = 0.5
    result["趋势_RSI强度"] = np.clip(rsi / 100.0 if not pd.isna(rsi) else 0.5, 0, 1)
    result["技术_MACD金叉"] = 1.0 if m_bar > 0 else 0.3
    result["动量_5日涨幅"] = np.clip(chg_5d / 0.2 + 0.5, 0, 1)
    return idx, result


def compute_factors(df_quotes, concept_map, tech_limit=0):
    """
    多因子计算,Rank 百分位标准化。
    tech_limit=0 表示全量计算技术因子(使用并行线程池)。
    """
    q = df_quotes.copy()
    if len(q) == 0:
        return q

    # === 行业补充：腾讯行情不返回行业，用东方财富 datacenter API 批量补充 ===
    if "行业" in q.columns:
        empty_industry = q["行业"].isna() | (q["行业"] == "")
        if empty_industry.any():
            # 先从 concept_map 的第一个概念提取行业
            filled = 0
            for idx in q[empty_industry].index:
                code = str(q.loc[idx, "代码"]).zfill(6)
                concepts = concept_map.get(code, [])
                if concepts:
                    q.loc[idx, "行业"] = concepts[0]
                    filled += 1
            if filled > 0:
                log(f"行业补充: 从概念映射补充 {filled} 只")

            # 仍有空的，用东方财富 datacenter API 批量补充
            still_empty = q["行业"].isna() | (q["行业"] == "")
            if still_empty.any():
                try:
                    missing_codes = q.loc[still_empty, "代码"].astype(str).str.zfill(6).tolist()
                    em_industry = _fetch_industry_eastmoney_datacenter(missing_codes)
                    if em_industry:
                        for code, industry in em_industry.items():
                            mask = q["代码"].astype(str).str.zfill(6) == code
                            q.loc[mask, "行业"] = industry
                        log(f"行业补充: 东方财富补充 {len(em_industry)} 只")
                except Exception as e:
                    log(f"行业补充: 东方财富失败: {e}", "WARN")

            # 剩余仍为空的填"其他"
            q["行业"] = q["行业"].fillna("其他")
            q.loc[q["行业"] == "", "行业"] = "其他"

    # === 基础因子(全量计算,速度极快)===
    # 1. 动量因子: 昨日涨跌幅(Rank百分位) — 避免前瞻偏差
    # 注：涨跌幅字段在盘后快照时是当日涨幅，盘中快照时是截至快照时间的涨幅
    # 为避免前瞻偏差，使用涨跌幅的滞后值（如果有的话），否则用当前值但标记为近似
    if "昨日涨跌幅" in q.columns:
        q["动量_涨跌幅"] = q["昨日涨跌幅"].rank(pct=True)
    else:
        # 降级方案：使用当日涨跌幅，但降低权重（通过后续权重调整）
        q["动量_涨跌幅"] = q["涨跌幅"].rank(pct=True)
    q["动量_涨跌幅"] = np.clip(q["动量_涨跌幅"].fillna(0.5), 0, 1)

    # 2. 量能因子: 换手率
    q["量能_换手率"] = q["换手率"].rank(pct=True)
    q["量能_换手率"] = np.clip(q["量能_换手率"].fillna(0.5), 0, 1)

    # 3. 量能因子: 量比
    q["量能_量比"] = q["量比"].rank(pct=True)
    q["量能_量比"] = np.clip(q["量能_量比"].fillna(0.5), 0, 1)

    # 4. 估值因子: PE 反向(越低越好,但亏损股已在清洗阶段过滤)
    pe = q["市盈率动态"].copy()
    pe = pe.mask(pe <= 0, np.nan)
    q["估值_PE反向"] = 1 - pe.rank(pct=True)
    q["估值_PE反向"] = np.clip(q["估值_PE反向"].fillna(0.3), 0, 1)  # fillna 0.3 而非 0.5,对无PE数据略有惩罚

    # 5. 估值因子: PB（优先使用行情中的市净率，Baostock 作为降级）
    if "市净率" in q.columns:
        pb = q["市净率"].copy()
        pb = pd.to_numeric(pb, errors="coerce")
        pb = pb.mask(pb <= 0, np.nan)
        pb_valid = pb.notna().sum()
        if pb_valid > len(q) * 0.3:
            q["估值_PB反向"] = 1 - pb.rank(pct=True)
            q["估值_PB反向"] = np.clip(q["估值_PB反向"].fillna(0.3), 0, 1)
            log(f"估值PB: 行情数据 {pb_valid}/{len(q)} 只有效")
        else:
            # 行情中PB不足30%，降级到Baostock（Top500）
            try:
                from baostock_data import BaostockData
                bd = BaostockData()
                pb_codes = q["代码"].astype(str).tolist()
                if len(pb_codes) > 500:
                    pb_codes = q.nlargest(500, "总市值")["代码"].astype(str).tolist()
                    log(f"估值PB: Baostock 限制查询Top500（按市值），原{len(q)}只")
                pb_map = bd.get_pb_for_codes(pb_codes)
                if pb_map:
                    q["pb_raw"] = q["代码"].astype(str).str.zfill(6).map(pb_map)
                    pb_bs = q["pb_raw"].copy()
                    pb_bs = pb_bs.mask(pb_bs <= 0, np.nan)
                    q["估值_PB反向"] = 1 - pb_bs.rank(pct=True)
                    q["估值_PB反向"] = np.clip(q["估值_PB反向"].fillna(0.3), 0, 1)
                    log(f"估值PB: Baostock PB 替补 {len(pb_map)} 只")
                else:
                    q["估值_PB反向"] = q["估值_PE反向"]
            except Exception:
                q["估值_PB反向"] = q["估值_PE反向"]
    else:
        q["估值_PB反向"] = q["估值_PE反向"]  # 降级回退

    # 6. 规模因子: 小市值偏好(降低权重后,小市值得分不过度膨胀)
    # ── Baostock 市值替补: 若实时行情源大量缺失(>50%为0或NaN),用Baostock计算 ──
    mv_series = q.get("总市值", pd.Series([0] * len(q)))
    missing_ratio = (mv_series.isna() | (mv_series <= 0)).sum() / max(len(q), 1)
    if missing_ratio > 0.5:
        try:
            from baostock_data import BaostockData
            bd = BaostockData()
            cap_map = bd.get_cap_for_codes(q["代码"].astype(str).tolist())
            if cap_map:
                if "总市值" not in q.columns:
                    q["总市值"] = 0.0
                    q["流通市值"] = 0.0
                for code, caps in cap_map.items():
                    mask = q["代码"].astype(str).str.zfill(6) == code
                    q.loc[mask, "总市值"] = caps["总市值"]
                    q.loc[mask, "流通市值"] = caps["流通市值"]
                log(f"规模因子: Baostock 市值替补 {len(cap_map)} 只")
        except Exception as e:
            log(f"操作异常: {e}", "WARN")

    mv = q["总市值"].copy()
    mv = mv.mask(mv <= 0, np.nan)
    q["规模_小市值"] = 1 - mv.rank(pct=True)
    q["规模_小市值"] = np.clip(q["规模_小市值"].fillna(0.3), 0, 1)  # fillna 0.3,对无市值数据略有惩罚

    # 7. 质量因子: 盈利能力(ROE > 15% 得高分)
    # ── Baostock ROE 替补: 若主数据源缺失(>30%),用Baostock季频数据 ──
    need_roe_backup = "净资产收益率" not in q.columns
    if not need_roe_backup:
        roe_tmp = pd.to_numeric(q["净资产收益率"], errors="coerce")
        need_roe_backup = roe_tmp.isna().sum() / max(len(q), 1) > 0.3
    if need_roe_backup:
        try:
            from baostock_data import BaostockData
            bd = BaostockData()
            # 限制只查询市值前500只，避免全量查询耗时过长（原1000+只需20+分钟）
            roe_codes = q["代码"].astype(str).tolist()
            if len(roe_codes) > 500:
                roe_codes = q.nlargest(500, "总市值")["代码"].astype(str).tolist()
                log(f"质量因子: Baostock ROE 限制查询Top500（按市值），原{len(q)}只")
            roe_map = bd.get_roe_for_codes(roe_codes)
            if roe_map:
                if "净资产收益率" not in q.columns:
                    q["净资产收益率"] = np.nan
                q.loc[:, "净资产收益率"] = q["代码"].astype(str).str.zfill(6).map(roe_map)
                log(f"质量因子: Baostock ROE 替补 {len(roe_map)} 只")
        except Exception as e:
            log(f"操作异常: {e}", "WARN")

    if "净资产收益率" in q.columns:
        roe = q["净资产收益率"].copy()
        roe = pd.to_numeric(roe, errors="coerce")
        roe = roe.mask(roe <= 0, np.nan)
        q["质量_盈利"] = roe.rank(pct=True)
        q["质量_盈利"] = np.clip(q["质量_盈利"].fillna(0.3), 0, 1)
    else:
        q["质量_盈利"] = 0.5

    # === 概念热度因子(向量化)===
    q = q.reset_index(drop=True)
    _hot_set = set(HOT_CONCEPTS)
    def _concept_heat(code):
        concepts = concept_map.get(code, [])
        return min(sum(1 for c in concepts if any(h in c for h in _hot_set)), 10) / 10.0
    q["概念_热度"] = q["代码"].map(_concept_heat)

    log(f"基础因子: 全量 {len(q)} 只计算完成(PE/PB/涨跌/换手/量比/市值/概念)")

    # === 深度因子占位(默认0.5)===
    q["成长_净利润同比"] = 0.5
    q["成长_营收同比"] = 0.5
    q["机构_北向资金"] = 0.5
    q["机构_基金持仓"] = 0.5
    q["研报_覆盖度"] = 0.5
    q["景气_行业"] = 0.5

    # === AI分析因子(向量化加载)===
    q["ai_score"] = 0.5
    try:
        from stock_ai_analysis import load_ai_cache
        cache = load_ai_cache()
        if cache:
            _ai_map = {}
            for k, v in cache.items():
                _ai_map[k] = max(0, min(1, v.get("ai_score", 50) / 100.0))
            def _get_ai(code_name):
                return _ai_map.get(code_name, 0.5)
            q["ai_score"] = (q["代码"].str.zfill(6) + "_" + q["名称"]).map(_get_ai).fillna(0.5)
    except Exception as e:
        log(f"  AI因子加载失败: {e}", "WARN")

    # === 技术因子(需拉K线,使用并行线程池加速)===
    q["趋势_均线位置"] = 0.5
    q["趋势_RSI强度"] = 0.5
    q["技术_MACD金叉"] = 0.5
    q["动量_5日涨幅"] = 0.5

    # 确定计算范围
    calc_count = len(q) if tech_limit == 0 else min(tech_limit, len(q))

    # 按换手率降序排列(流动性好的股票更值得技术分析)
    if "换手率" in q.columns:
        q_sorted = q.sort_values("换手率", ascending=False)
    else:
        q_sorted = q

    calc_df = q_sorted.head(calc_count)

    log(f"技术因子: 启动并行计算 {len(calc_df)} 只 (线程池20路)...")

    tech_count = 0
    success_count = 0
    t0 = time.time()
    batch_tasks = []

    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {}
        for idx, row in calc_df.iterrows():
            f = executor.submit(_calc_one_tech, idx, row["代码"], row["名称"])
            futures[f] = (idx, row["代码"])
            batch_tasks.append(f)
            # 每100只打一次进度
            if len(batch_tasks) % 100 == 0:
                pass  # 进度由下面汇总

        completed = 0
        for f in as_completed(futures):
            completed += 1
            idx, code = futures[f]
            try:
                row_idx, result = f.result()
                tech_count += 1  # 总是计数（任务完成）
                if result:
                    for k, v in result.items():
                        q.at[row_idx, k] = v
                    success_count += 1
            except Exception as e:
                log(f"操作异常: {e}", "WARN")

            if completed % 200 == 0 or completed == len(futures):
                elapsed = time.time() - t0
                rate = completed / elapsed if elapsed > 0 else 0
                eta = (len(futures) - completed) / rate if rate > 0 else 0
                log(f"  技术因子进度: {completed}/{len(futures)} ({completed*100//len(futures)}%) "
                    f"速度 {rate:.0f}只/秒  ETA {eta:.0f}秒")

    tech_elapsed = time.time() - t0
    log(f"技术因子: {tech_count} 只完成, {success_count} 只成功写入, {len(calc_df)} 只计算, 耗时 {tech_elapsed:.1f}秒")

    # 舆情因子（akshare市场情绪+板块热度）
    try:
        from sentiment_data import compute_sentiment_factor
        q = compute_sentiment_factor(q, concept_map)
        log(f"舆情因子: 市场情绪+板块热度已纳入")
    except Exception:
        q["情绪_市场"] = 0.5
        q["情绪_板块"] = 0.5

    # LLM因子信号（大模型挖掘的市场模式）
    try:
        from llm_factor_miner import compute_llm_factor
        q = compute_llm_factor(q, concept_map)
        log(f"LLM因子: 市场模式信号已纳入")
    except Exception:
        q["LLM_信号"] = 0.5

    return q


def compute_composite_score(df, weights=None):
    """综合打分: 优先LightGBM ML打分，回退加权求和 + 归一化到 0-100"""
    if weights is None:
        weights = FACTOR_WEIGHTS

    # 尝试LightGBM ML打分
    ml_used = False
    try:
        from ml_scorer import predict_scores, load_model
        loaded = load_model()
        if loaded is not None:
            model, meta = loaded
            df, ml_used = predict_scores(df, model, meta)
            if ml_used:
                df["综合得分"] = df["综合得分_ML"]
                df = df.drop(columns=["综合得分_ML"], errors="ignore")
                log(f"  打分模式: LightGBM ML (训练数据={meta.get('train_days', '?')}天)")
    except Exception as e:
        log(f"操作异常: {e}", "WARN")

    # 回退: 加权求和（市场环境自适应）
    if not ml_used:
        # 尝试市场环境自适应权重
        regime = "sideways"
        try:
            from market_regime import get_regime_weights
            regime, regime_weights = get_regime_weights(default_weights=weights)
            # 检查返回的权重key是否匹配因子列名
            matched = sum(1 for k in regime_weights if k in df.columns)
            if matched >= 3:
                weights = regime_weights
            else:
                log(f"  市场环境={regime}, 但regime权重key不匹配因子列({matched}个匹配), 使用手工权重", "WARN")
            log(f"  打分模式: 加权平均 (市场环境={regime})")
        except Exception:
            log(f"  打分模式: 加权平均 (手工权重)")

        df["综合得分"] = 0.0
        for factor, w in weights.items():
            if factor in df.columns:
                df["综合得分"] += df[factor].fillna(0.5) * w

        # 因子已是rank百分位(0-1)，加权求和后乘100即为绝对百分制
        # 不做min-max拉伸，让分值反映真实水平而非强制拉到100
        df["综合得分"] = df["综合得分"] * 100

    df = df.sort_values("综合得分", ascending=False).reset_index(drop=True)
    df["排名"] = df.index + 1

    return df


def select_diversified_topn(df, top_n=3, max_per_industry=2):
    """
    行业分散选股：从综合得分排名中选取TopN，同行业最多max_per_industry只。

    Args:
        df: 已按综合得分排序的DataFrame（需含 行业 列）
        top_n: 选取数量
        max_per_industry: 同行业最大数量

    Returns:
        list: 选中的行索引列表
    """
    if "行业" not in df.columns or len(df) == 0:
        return list(range(min(top_n, len(df))))

    industry_count = {}
    selected = []

    for idx, row in df.iterrows():
        industry = str(row.get("行业", "未知"))
        count = industry_count.get(industry, 0)

        if count < max_per_industry:
            selected.append(idx)
            industry_count[industry] = count + 1

            if len(selected) >= top_n:
                break

    # 如果分散后数量不足，补充剩余
    if len(selected) < top_n:
        remaining = [i for i in range(len(df)) if i not in selected]
        for idx in remaining[:top_n - len(selected)]:
            selected.append(idx)

    return selected


# ============================================================
#  历史沉淀 - 完整归档供回测
# ============================================================

def archive_run(df, concept_map, meta):
    """
    将本次选股结果完整归档到 history/ 目录。
    每次归档包含: 原始行情数据 + 因子得分 + 排序结果 + 元数据。
    """
    now = get_now()
    snapshot_dir = HISTORY_DIR / now.strftime("%Y%m")
    snapshot_dir.mkdir(exist_ok=True)

    snapshot_name = now.strftime("snapshot_%Y%m%d_%H%M%S")

    # 选出要保存的列
    save_cols = ["排名", "代码", "名称", "综合得分", "涨跌幅", "换手率", "量比",
              "总市值", "市盈率动态", "行业", "AI_关键词",
              "动量_涨跌幅", "动量_5日涨幅", "趋势_均线位置", "趋势_RSI强度",
              "量能_换手率", "量能_量比", "估值_PE反向", "估值_PB反向",
              "规模_小市值", "技术_MACD金叉", "概念_热度", "质量_盈利",
              "成长_净利润同比", "成长_营收同比", "机构_北向资金", "机构_基金持仓",
              "研报_覆盖度", "景气_行业"]
    available_cols = [c for c in save_cols if c in df.columns]

    # CSV 存档
    csv_path = snapshot_dir / f"{snapshot_name}.csv"
    df[available_cols].to_csv(csv_path, index=False, encoding="utf-8-sig")

    # JSON 存档(含元数据)
    json_path = snapshot_dir / f"{snapshot_name}.json"
    top10 = df.head(10)[available_cols].to_dict("records")

    archive = {
        "版本": VERSION,
        "抓取时间": now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
        "交易日": is_trading_day(now),
        "交易时段": get_market_session(now)[1],
        "盘中运行": get_market_session(now)[0],
        "数据溯源": [p for p in _data_provenance],
        "配置": {
            "top_n": meta.get("top_n", 10),
            "tech_limit": meta.get("tech_limit", 200),
            "因子权重": FACTOR_WEIGHTS,
            "热门概念": HOT_CONCEPTS,
        },
        "统计": {
            "扫描股票": len(df),
            "最高分": round(float(df["综合得分"].max()), 6) if len(df) > 0 else 0,
            "最低分": round(float(df["综合得分"].min()), 6) if len(df) > 0 else 0,
            "平均分": round(float(df["综合得分"].mean()), 6) if len(df) > 0 else 0,
        },
        "Top10": top10,
    }
    json_path.write_text(json.dumps(archive, ensure_ascii=False, indent=2), encoding="utf-8")

    log(f"历史归档: {snapshot_name} → history/{now.strftime('%Y%m')}/")
    log(f"  CSV: {csv_path.name} ({len(df)} 行)")
    log(f"  JSON: {json_path.name} (含完整元数据+溯源)")

    return snapshot_name


# ============================================================
#  竞价数据采集 - 个股API获取竞价涨幅 & 竞价额
# ============================================================

# 当日竞价缓存(首次采集后缓存,当日不再重复请求)
_auction_cache = {}   # {code: {"竞价涨幅": float, "竞价额": float}}


def enrich_auction_data(df, top_n=10):
    """
    为 top N 股票补充竞价数据(竞价涨幅/竞价额)。
    机制:
      竞价时段(9:15-9:25): 批量API的 f6=竞价额,f2=竞价实时价
      交易时段(9:30+):  个股 stock/get API 快照,计算竞价涨幅=(今开-昨收)/昨收
    """
    global _auction_cache

    now = get_now()
    in_session, _, _ = get_market_session(now)
    is_auction = (now.hour == 9 and 15 <= now.minute <= 25)

    top_codes = df.head(top_n)["代码"].tolist()

    # 初始化列
    df["竞价涨幅"] = np.nan
    df["竞价额"] = np.nan

    # 竞价时段:直接从批量API提取(f6=竞价额,f2=竞价实时价)
    if is_auction and "成交额" in df.columns and "昨收" in df.columns:
        for idx, row in df.head(top_n).iterrows():
            code = row["代码"]
            pre_close = row.get("昨收", 0)
            auction_price = row.get("最新价", 0)  # 竞价时段的最新价=竞价价
            auction_amt = row.get("成交额", 0)     # 竞价时段=竞价额
            if pre_close > 0 and auction_amt > 0:
                jjzf = round((auction_price - pre_close) / pre_close * 100, 2)
                df.at[idx, "竞价涨幅"] = jjzf
                df.at[idx, "竞价额"] = auction_amt
                _auction_cache[code] = {"竞价涨幅": jjzf, "竞价额": auction_amt}
        if _auction_cache:
            log(f"竞价数据: 批量API提取 {len(_auction_cache)} 只(竞价时段)")
        return df

    # 非竞价时段:优先用缓存,否则计算今开-昨收涨幅 + 尝试个股API
    new_codes = [c for c in top_codes if c not in _auction_cache]

    if new_codes:
        # 尝试个股 stock/get API 获取竞价快照
        auction_data = _fetch_stock_auction_batch(new_codes)

        for code in new_codes:
            row = df[df["代码"] == code]
            if len(row) == 0:
                continue
            idx = row.index[0]
            pre_close = row.iloc[0].get("昨收", 0)
            open_price = row.iloc[0].get("今开", 0)

            if code in auction_data:
                # 个股API提供了竞价数据
                ad = auction_data[code]
                if pre_close > 0:
                    jjzf = round((ad.get("price", open_price) - pre_close) / pre_close * 100, 2)
                else:
                    jjzf = 0
                df.at[idx, "竞价涨幅"] = jjzf
                df.at[idx, "竞价额"] = ad.get("amount", 0)
                _auction_cache[code] = {"竞价涨幅": jjzf, "竞价额": ad.get("amount", 0)}
            elif pre_close > 0 and open_price > 0:
                # 个股API失败,用今开-昨收估算
                jjzf = round((open_price - pre_close) / pre_close * 100, 2)
                df.at[idx, "竞价涨幅"] = jjzf
                # 竞价额:已无法获取竞价时段数据,用成交额近似标记
                df.at[idx, "竞价额"] = 0  # 标记为无数据

        if auction_data:
            log(f"竞价数据: 个股API获取 {len(auction_data)} 只")
        else:
            log(f"竞价数据: 今开-昨收估算 {len(new_codes)} 只 (竞价额不可用)")

    # 从缓存回填
    for code in top_codes:
        if code in _auction_cache and code not in [c for c in top_codes if c in new_codes]:
            row = df[df["代码"] == code]
            if len(row) > 0:
                idx = row.index[0]
                if pd.isna(df.at[idx, "竞价涨幅"]):
                    df.at[idx, "竞价涨幅"] = _auction_cache[code]["竞价涨幅"]
                    df.at[idx, "竞价额"] = _auction_cache[code]["竞价额"]

    return df


# ============================================================
#  基础数据层 - 通达信 mootdx(F10 公司资料 / 季报财务)
# ============================================================

_tdx_client = None

def _tdx_connect():
    """获取或创建通达信 TCP 连接(懒加载,复用)"""
    global _tdx_client
    if not _mootdx_available:
        return None
    if _tdx_client is None:
        try:
            _tdx_client = Quotes.factory(market='std', timeout=10)
            # 测试连通性
            _tdx_client.minutes(symbol='000001', market=0)
            log("通达信 TCP 连接成功 (mootdx)")
        except Exception as e:
            log(f"通达信 TCP 连接失败: {e}", "WARN")
            _tdx_client = None
    return _tdx_client


def _tdx_finance():
    """获取通达信财务查询实例"""
    if not _mootdx_available:
        return None
    try:
        return Finance()
    except Exception as e:
        log(f"操作异常: {e}", "WARN")
        return None


def get_f10_company(code):
    """
    获取 F10 公司概况信息(通达信 mootdx)。
    返回: {"行业": str, "市盈率": float, "市净率": float, "总股本": int, ...}
    """
    client = _tdx_connect()
    if not client:
        return {}
    try:
        # 市场代码: 0=深圳, 1=上海
        market = 0 if code.startswith(('0', '3')) else 1
        info = client.finance(code, market)
        if not info:
            return {}
        result = {}
        for row in info:
            if not isinstance(row, dict) or 'field' not in row or 'value' not in row:
                continue
            key = row['field']
            val = row['value']
            # 翻译关键字段
            field_map = {
                '行业': '行业',
                'ipoDate': '上市日期',
                'pe': '市盈率',
                'pb': '市净率',
                'totalShares': '总股本',
                'circulatingShares': '流通股本',
                'mainBusiness': '主营业务',
                'province': '所在省份',
            }
            if key in field_map:
                result[field_map[key]] = val
        return result
    except Exception as e:
        log(f"F10获取失败 {code}: {e}", "DEBUG")
        return {}


def get_finance_latest(code):
    """
    获取最新季报财务数据(通达信 mootdx)。
    返回: {"净利润同比": float, "营收同比": float, "ROE": float, "每股收益": float, "最新季度": str}
    """
    fin = _tdx_finance()
    if not fin:
        return {}
    try:
        # 尝试获取最新分红/财务数据
        market = 0 if code.startswith(('0', '3')) else 1
        data = fin.get('CWXX', code, market)
        if data is None or data.empty:
            return {}

        result = {}
        for _, row in data.iterrows():
            if not isinstance(row, pd.Series):
                continue
            field = row.get('field', str(row.iloc[0]) if len(row) > 0 else '')
            val = row.get('value', row.iloc[1] if len(row) > 1 else None)
            if pd.isna(val):
                continue

            # 关键财务字段(通达信字段名)
            key_fields = {
                '净利润同比增长率': '净利润同比',
                '营业总收入同比增长率': '营收同比',
                '净资产收益率': 'ROE',
                '每股收益': '每股收益',
                '最新报告期': '最新季度',
            }
            for kw, label in key_fields.items():
                if kw in str(field):
                    try:
                        result[label] = float(val.replace('%', '').strip()) if isinstance(val, str) and '%' in val else float(val)
                    except (ValueError, TypeError):
                        result[label] = str(val)
        return result
    except Exception as e:
        # 很多股票没有完整财务数据,静默跳过
        return {}


def get_finance_xjll(code):
    """
    获取公司利润表 / 现金流量表关键指标(通达信 mootdx)。
    返回: {"净利润(亿)": float, "营业收入(亿)": float, "经营现金流(亿)": float, "净利润同比": float, "营收同比": float}
    """
    fin = _tdx_finance()
    if not fin:
        return {}
    try:
        market = 0 if code.startswith(('0', '3')) else 1
        data = fin.get('profit', code, market)
        if data is None or data.empty:
            return {}

        result = {}
        # 取最近两期,计算同比
        try:
            rows = data.sort_index(ascending=False).head(2) if isinstance(data, pd.DataFrame) else data
        except Exception:
            return result

        # 简化处理:提取收入/利润字段
        for col in rows.columns:
            col_lower = str(col).lower()
            if '净利润' in str(col) or 'net_profit' in col_lower:
                try:
                    vals = pd.to_numeric(rows[col].head(2), errors='coerce').dropna()
                    if len(vals) == 2:
                        result['净利润同比'] = (vals.iloc[0] / vals.iloc[1] - 1) * 100 if vals.iloc[1] != 0 else 0
                    if len(vals) >= 1:
                        result['净利润(亿)'] = round(vals.iloc[0] / 1e8, 2)
                except Exception as e:
                    log(f"操作异常: {e}", "WARN")
            if '营业' in str(col) or 'revenue' in col_lower:
                try:
                    vals = pd.to_numeric(rows[col].head(2), errors='coerce').dropna()
                    if len(vals) == 2:
                        result['营收同比'] = (vals.iloc[0] / vals.iloc[1] - 1) * 100 if vals.iloc[1] != 0 else 0
                    if len(vals) >= 1:
                        result['营业收入(亿)'] = round(vals.iloc[0] / 1e8, 2)
                except Exception as e:
                    log(f"操作异常: {e}", "WARN")
        return result
    except Exception:
        return {}


# ============================================================
#  消息面/信号/筹码 批量增强(a-stock-data 整合)
# ============================================================

def enrich_news_and_signals(df, top_n=10):
    """
    为 top N 股票批量补充消息面、研报、龙虎榜、筹码、基础数据(F10/季报)。
    新增列: 新闻信号, 研报信号, 龙虎榜信号, 北向信号, 筹码信号, 基础信号
    """
    top_codes = df.head(top_n)["代码"].tolist()
    now = get_now()

    # 初始化列
    for col in ["新闻信号", "研报信号", "龙虎榜信号", "北向信号", "筹码信号", "基础信号"]:
        df[col] = ""

    # 1. 北向资金信号(全局一次)
    northbound_dir = get_northbound_direction()
    if northbound_dir not in ("无数据", "北向持平", "北向小幅流入", "北向小幅流出"):
        for idx in df.head(top_n).index:
            df.at[idx, "北向信号"] = northbound_dir
        log(f"北向资金: {northbound_dir}")

    # 2. 龙虎榜(全局一次)
    dragon_stocks = {}
    dt_data = daily_dragon_tiger(now.strftime("%Y-%m-%d"))
    if dt_data["total_records"] > 0:
        for s in dt_data["stocks"]:
            dragon_stocks[s["code"]] = s["reason"]
        log(f"龙虎榜: 当日 {dt_data['total_records']} 只上榜")

    # 3. 逐只查询:新闻 + 研报 + 筹码 + F10/季报(并发)
    def _enrich_one(code):
        result = {"code": code, "news": [], "reports": [], "chip": [], "finance": {}}

        # 个股新闻(最近10条)
        news = eastmoney_stock_news(code, 10)
        result["news"] = news

        # 研报(最近3页)
        reports = eastmoney_reports(code, 3)
        result["reports"] = reports

        # 股东户数 + 融资融券
        holders = holder_num_change(code, 3)
        margins = margin_trading(code, 5)
        result["chip"] = {"holders": holders, "margins": margins}

        # ============ v5.8: 基础数据层 - F10/季报(通达信 mootdx) ============
        fin_data = get_finance_latest(code)
        if not fin_data:
            fin_data = get_finance_xjll(code)
        result["finance"] = fin_data

        return result

    # 串行查询避免触发东财风控(em_get 已有 1s 间隔)
    enriched = {}
    for code in top_codes:
        enriched[code] = _enrich_one(code)

    # 4. 回填信号列
    for idx, row in df.head(top_n).iterrows():
        code = row["代码"]
        ed = enriched.get(code, {})
        signals = []

        # 新闻信号:深度解读新闻标题,提取实质性信号
        news_items = ed.get("news", [])
        today_str = now.strftime("%Y-%m-%d")
        news_title_keywords = {
            # 业绩/财报
            "业绩预增": ["业绩预增", "净利润预增", "盈利预增", "业绩大增", "净利润大增"],
            "业绩暴增": ["业绩暴增", "净利润暴增", "同比增长", "翻倍"],
            "营收高增": ["营收增长", "营业收入增长", "营收大增"],
            # 订单/合同
            "大额订单": ["中标", "签订合约", "签订协议", "大单", "重大合同", "订单"],
            "项目落地": ["项目开工", "项目投产", "项目落地", "基地建设"],
            # 收购/重组
            "收购兼并": ["收购", "并购", "资产重组", "借壳", "注入"],
            "战略合作": ["战略合作", "战略协议", "战略入股", "合资"],
            # 政策/补贴
            "政策利好": ["补贴", "政策扶持", "专项资金", "税收优惠", "规划"],
            "国产替代": ["国产替代", "自主可控", "国产化"],
            # 人员/高管
            "高管变动": ["董事长", "总裁", "总经理", "辞职", "更换", "新任"],
            "股权激励": ["股权激励", "员工持股", "限制性股票"],
            # 技术/产品
            "技术突破": ["突破", "量产", "认证", "获批", "临床", "取证"],
            "新品发布": ["新品", "新产品", "新一代", "发布"],
            # 股东/增持
            "股东增持": ["增持", "回购", "举牌"],
            "定增": ["定增", "增发", "配股"],
        }
        matched_news_signals = []
        for n in news_items[:10]:
            title = n.get("title", "")
            for sig_name, kws in news_title_keywords.items():
                if any(kw in title for kw in kws):
                    matched_news_signals.append(sig_name)
                    break

        # 去重,取前3个最强信号
        seen = set()
        unique_news_sigs = []
        for s in matched_news_signals:
            if s not in seen:
                seen.add(s)
                unique_news_sigs.append(s)
        for sig in unique_news_sigs[:3]:
            signals.append(("新闻信号", sig))

        # 研报信号:最近有买入/增持评级 / 评级上调
        reports = ed.get("reports", [])
        buy_ratings = {"买入", "增持", "强烈推荐", "推荐", "优于大市", "强于大市"}
        upgrade_keywords = ["上调", "调高", "首次覆盖"]
        for rpt in reports:
            rating = rpt.get("emRatingName", "")
            title = rpt.get("title", "")
            if rating in buy_ratings and not any(s[1] == "机构看好" for s in signals):
                signals.append(("研报信号", "机构看好"))
            if any(kw in (title + rating) for kw in upgrade_keywords) and not any(s[1] == "研报上调" for s in signals):
                signals.append(("研报信号", "研报上调"))

        # 龙虎榜信号
        if code in dragon_stocks:
            signals.append(("龙虎榜信号", "龙虎榜上榜"))

        # 筹码信号:股东户数连续下降 → 筹码集中
        chip = ed.get("chip", {})
        holders = chip.get("holders", [])
        if len(holders) >= 2:
            changes = [h.get("change_ratio", 0) for h in holders[:2]]
            if all(c < 0 for c in changes if c):
                signals.append(("筹码信号", "筹码集中"))

        # 融资信号:融资余额连续增加
        margins = chip.get("margins", [])
        if len(margins) >= 2:
            m_inc = [m.get("rzye", 0) for m in margins[:2]]
            if m_inc[0] > m_inc[1] and m_inc[1] > 0:
                signals.append(("筹码信号", "融资加仓"))

        # ============ v5.8 基础信号:F10/季报 ============
        fin = ed.get("finance", {})
        profit_yoy = fin.get("净利润同比", None)
        revenue_yoy = fin.get("营收同比", None)
        roe = fin.get("ROE", None)
        eps = fin.get("每股收益", None)

        # 净利润同比增长
        if profit_yoy is not None:
            try:
                py = float(profit_yoy)
                if py > 100:
                    signals.append(("基础信号", "业绩暴增"))
                elif py > 30:
                    signals.append(("基础信号", "业绩增长"))
                elif py < -50:
                    signals.append(("基础信号", "业绩预警"))
            except (ValueError, TypeError):
                pass

        # 营收同比增长
        if revenue_yoy is not None:
            try:
                ry = float(revenue_yoy)
                if ry > 50:
                    if not any(s[1] == "营收高增" for s in signals):
                        signals.append(("基础信号", "营收高增"))
                elif ry > 20:
                    if not any(s[1].startswith("营收") for s in signals):
                        signals.append(("基础信号", "营收增长"))
            except (ValueError, TypeError):
                pass

        # 高 ROE(绩优股)
        if roe is not None:
            try:
                roe_val = float(roe)
                if roe_val > 20:
                    signals.append(("基础信号", "高ROE"))
                elif roe_val > 15:
                    if not any(s[1] == "高ROE" for s in signals):
                        signals.append(("基础信号", "绩优股"))
            except (ValueError, TypeError):
                pass

        # 每股收益
        if eps is not None:
            try:
                eps_val = float(eps)
                if eps_val > 2.0:
                    signals.append(("基础信号", "高收益"))
                elif eps_val <= 0 and eps_val is not None:
                    signals.append(("基础信号", "亏损"))
            except (ValueError, TypeError):
                pass

        # 回填
        for sig_type, sig_val in signals:
            df.at[idx, sig_type] = sig_val

        # ── 2026-06-23 深度因子填充:成长/机构/研报/景气 ──
        # 净利润同比增长率 → 成长因子 (归一化:同比>50%=1.0, >30%=0.8, >0%=0.5, <0%=0.2)
        if profit_yoy is not None:
            try:
                py = float(profit_yoy)
                if py > 50:    df.at[idx, "成长_净利润同比"] = 1.0
                elif py > 30:  df.at[idx, "成长_净利润同比"] = 0.8
                elif py > 0:   df.at[idx, "成长_净利润同比"] = 0.5
                elif py > -30: df.at[idx, "成长_净利润同比"] = 0.3
                else:          df.at[idx, "成长_净利润同比"] = 0.1
            except (ValueError, TypeError):
                pass
        # 营收同比增长率
        if revenue_yoy is not None:
            try:
                ry = float(revenue_yoy)
                if ry > 50:    df.at[idx, "成长_营收同比"] = 1.0
                elif ry > 20:  df.at[idx, "成长_营收同比"] = 0.8
                elif ry > 0:   df.at[idx, "成长_营收同比"] = 0.5
                elif ry > -20: df.at[idx, "成长_营收同比"] = 0.3
                else:          df.at[idx, "成长_营收同比"] = 0.1
            except (ValueError, TypeError):
                pass
        # ROE 高分 → 质量盈利增强
        if roe is not None:
            try:
                roe_val = float(roe)
                if roe_val > 20: df.at[idx, "质量_盈利"] = 1.0
                elif roe_val > 15: df.at[idx, "质量_盈利"] = 0.8
                elif roe_val > 10: df.at[idx, "质量_盈利"] = 0.6
                elif roe_val > 0:  df.at[idx, "质量_盈利"] = 0.4
                else:              df.at[idx, "质量_盈利"] = 0.2
            except (ValueError, TypeError):
                pass
        # 研报覆盖度:按研报数量评分
        report_count = len(ed.get("reports", []))
        if report_count >= 5:         df.at[idx, "研报_覆盖度"] = 1.0
        elif report_count >= 3:       df.at[idx, "研报_覆盖度"] = 0.8
        elif report_count >= 1:       df.at[idx, "研报_覆盖度"] = 0.6
        else:                         df.at[idx, "研报_覆盖度"] = 0.3
        # 机构持仓:利用北向方向 + 龙虎榜信号估算
        if northbound_dir in ("北向大幅流入", "北向持续流入"):
            df.at[idx, "机构_北向资金"] = 0.8
        elif northbound_dir in ("北向小幅流入",):
            df.at[idx, "机构_北向资金"] = 0.6
        elif northbound_dir in ("北向持平",):
            df.at[idx, "机构_北向资金"] = 0.5
        elif northbound_dir in ("北向小幅流出",):
            df.at[idx, "机构_北向资金"] = 0.3
        elif northbound_dir in ("北向大幅流出", "北向持续流出"):
            df.at[idx, "机构_北向资金"] = 0.2
        # 龙虎榜上榜 → 机构关注度提升
        if code in dragon_stocks:
            val = df.at[idx, "机构_基金持仓"]
            if pd.isna(val):
                val = 0.5
            df.at[idx, "机构_基金持仓"] = max(val, 0.6)
        # 概念热度 + 换手率 → 行业景气度代理
        concept_heat = df.at[idx, "概念_热度"]
        turnover = df.at[idx, "量能_换手率"]
        df.at[idx, "景气_行业"] = np.clip(concept_heat * 0.6 + turnover * 0.4, 0, 1)

    # 统计信号分布
    counts = {}
    for col in ["新闻信号", "研报信号", "龙虎榜信号", "北向信号", "筹码信号", "基础信号"]:
        nz = (df[col] != "").sum()
        if nz > 0:
            counts[col] = nz
    if counts:
        log(f"消息面增强: {counts}")

    # ── 2026-06-23 二次评分:将深度因子纳入综合得分 ──
    # 由于深度因子在 enrich 阶段才填充真实值,需要重新计算综合得分
    df = compute_composite_score(df, FACTOR_WEIGHTS)
    log(f"二次评分完成: {len(df)} 只 (深度因子已纳入)")

    return df


def _fetch_stock_auction_batch(codes):
    """通过新浪财经API批量获取竞价/实时快照数据
    东方财富push2已封禁，push2his不支持实时快照，改用新浪
    """
    result = {}
    for code in codes:
        try:
            # 新浪行情API: sh=上海, sz=深圳
            prefix = "sh" if code.startswith(("60", "68")) else "sz"
            url = f"http://hq.sinajs.cn/list={prefix}{code}"
            r = requests.get(url, headers={"Referer": "https://finance.sina.com.cn/",
                                           "User-Agent": UA}, timeout=8)
            r.encoding = "gbk"
            text = r.text.strip()
            if "=" in text and '"' in text:
                fields = text.split('"')[1].split(",")
                if len(fields) >= 10:
                    open_price = float(fields[1]) if fields[1] else 0  # 今开
                    pre_close = float(fields[2]) if fields[2] else 0   # 昨收
                    volume = float(fields[8]) if fields[8] else 0       # 成交量(手)
                    amount = float(fields[9]) if fields[9] else 0       # 成交额(元)
                    result[code] = {
                        "price": open_price if open_price > 0 else pre_close,
                        "amount": amount,
                        "volume": volume,
                    }
        except Exception:
            continue
        time.sleep(0.05)
    return result


# ============================================================
#  新闻层 - a-stock-data 整合(个股新闻 + 全球快讯)
# ============================================================

def eastmoney_stock_news(code: str, page_size: int = 10) -> list[dict]:
    """
    东财个股新闻(JSONP 接口)。
    返回: [{title, content, time, source, url}]
    """
    import re
    cb = "jQuery_news"
    url = "https://search-api-web.eastmoney.com/search/jsonp"
    inner_params = json.dumps({
        "uid": "",
        "keyword": code,
        "type": ["cmsArticleWebOld"],
        "client": "web",
        "clientType": "web",
        "clientVersion": "curr",
        "param": {"cmsArticleWebOld": {"searchScope": "default", "sort": "default",
                  "pageIndex": 1, "pageSize": page_size, "preTag": "", "postTag": ""}},
    }, separators=(',', ':'))
    params = {"cb": cb, "param": inner_params}
    headers = {"User-Agent": UA, "Referer": "https://so.eastmoney.com/"}
    try:
        r = em_get(url, params=params, headers=headers, timeout=15)
        text = r.text
        json_str = text[text.index("(") + 1 : text.rindex(")")]
        d = json.loads(json_str)
        rows = []
        articles = d.get("result", {}).get("cmsArticleWebOld", []) or []
        for a in articles:
            rows.append({
                "title": re.sub(r'<[^>]+>', '', a.get("title", "")),
                "content": re.sub(r'<[^>]+>', '', a.get("content", ""))[:200],
                "time": a.get("date", ""),
                "source": a.get("mediaName", ""),
                "url": a.get("url", ""),
            })
        return rows
    except Exception:
        return []

def eastmoney_global_news(page_size: int = 30) -> list[dict]:
    """
    东方财富全球财经资讯(7x24 滚动)。
    返回: [{title, summary, time}]
    """
    url = "https://np-weblist.eastmoney.com/comm/web/getFastNewsList"
    params = {
        "client": "web", "biz": "web_724",
        "fastColumn": "102", "sortEnd": "",
        "pageSize": str(page_size),
        "req_trace": str(uuid.uuid4()),
    }
    headers = {"User-Agent": UA, "Referer": "https://kuaixun.eastmoney.com/"}
    try:
        r = em_get(url, params=params, headers=headers, timeout=10)
        d = r.json()
        rows = []
        for item in d.get("data", {}).get("fastNewsList", []):
            rows.append({
                "title": item.get("title", ""),
                "summary": item.get("summary", "")[:200],
                "time": item.get("showTime", ""),
            })
        return rows
    except Exception:
        return []


# ============================================================
#  研报层 - a-stock-data 整合(个股研报+评级+EPS预测)
# ============================================================

REPORT_API = "https://reportapi.eastmoney.com/report/list"

def eastmoney_reports(code: str, max_pages: int = 3) -> list[dict]:
    """
    拉取指定股票的个股研报列表(qType=0)。
    返回字段: title, publishDate, orgSName(机构), predictThisYearEps, emRatingName(评级), infoCode
    """
    all_records = []
    for page in range(1, max_pages + 1):
        params = {
            "industryCode": "*", "pageSize": "100", "industry": "*",
            "rating": "*", "ratingChange": "*",
            "beginTime": "2024-01-01", "endTime": "2030-01-01",
            "pageNo": str(page), "fields": "", "qType": "0",
            "orgCode": "", "code": code, "rcode": "",
            "p": str(page), "pageNum": str(page), "pageNumber": str(page),
        }
        try:
            r = em_get(REPORT_API, params=params,
                       headers={"Referer": "https://data.eastmoney.com/"}, timeout=30)
            d = r.json()
            rows = d.get("data") or []
            if not rows:
                break
            all_records.extend(rows)
            if page >= (d.get("TotalPage", 1) or 1):
                break
        except Exception:
            break
    return all_records


# ============================================================
#  信号层 - a-stock-data 整合(北向资金 + 龙虎榜)
# ============================================================

def hsgt_realtime() -> dict | None:
    """
    沪深股通当日实时分钟流向(含集合竞价 09:10-15:00)。
    返回: {time: [str...], hgt_yi: [float...], sgt_yi: [float...]} - 单位: 亿元
    """
    HSGT_HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Host": "data.hexin.cn",
        "Referer": "https://data.hexin.cn/",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "X-Requested-With": "XMLHttpRequest",
    }
    url = "https://data.hexin.cn/market/hsgtApi/method/dayChart/"
    try:
        r = requests.get(url, headers=HSGT_HEADERS, timeout=10)
        if r.status_code == 403:
            log("同花顺北向资金API返回403，尝试备用源(东方财富)", "WARN")
            return _fetch_hsgt_eastmoney()
        d = r.json()
        return {
            "time": d.get("time", []),
            "hgt_yi": d.get("hgt", []),
            "sgt_yi": d.get("sgt", []),
        }
    except Exception as e:
        log(f"操作异常: {e}", "WARN")
        return None


def _fetch_hsgt_eastmoney():
    """东财北向资金备用源"""
    try:
        url = "https://push2.eastmoney.com/api/qt/stock/get?secid=1.000001&fields=f43,f44,f45,f46,f47,f48,f50,f51,f52,f57,f58,f60,f107,f108,f115,f116,f117,f118,f119,f120,f121,f122,f123,f124,f125,f126,f127,f128,f129,f130,f131,f132,f133,f134,f135,f136,f137,f138,f139,f140,f141,f142,f143,f144,f145,f146,f147,f148,f149,f150"
        r = requests.get(url, timeout=10)
        if r.status_code == 200:
            return {"time": [], "hgt_yi": [], "sgt_yi": []}  # 东财接口格式不同，需适配
    except Exception:
        pass
    return None


def get_northbound_direction() -> str:
    """获取当日北向资金方向:北向大幅流入/北向净流入/北向持平/北向净流出/北向大幅流出"""
    data = hsgt_realtime()
    if not data or not data.get("hgt_yi"):
        return "无数据"
    hgt_values = [v for v in data["hgt_yi"] if v is not None]
    sgt_values = [v for v in data["sgt_yi"] if v is not None]
    if not hgt_values or not sgt_values:
        return "无数据"
    total = hgt_values[-1] + sgt_values[-1]
    if total > 30:
        return "北向大幅流入"
    elif total > 5:
        return "北向净流入"
    elif total < -30:
        return "北向大幅流出"
    elif total < -5:
        return "北向净流出"
    else:
        return "北向持平"


def daily_dragon_tiger(trade_date: str = None, min_net_buy: float = None) -> dict:
    """
    全市场龙虎榜。
    trade_date: YYYY-MM-DD(默认当日)
    返回: {date, total_records, stocks: [{code, name, reason, net_buy_wan, buy_wan, sell_wan}]}
    """
    if trade_date is None:
        trade_date = get_now().strftime("%Y-%m-%d")

    data = eastmoney_datacenter(
        "RPT_DAILYBILLBOARD_DETAILSNEW",
        filter_str=f"(TRADE_DATE>='{trade_date}')(TRADE_DATE<='{trade_date}')",
        page_size=500,
        sort_columns="BILLBOARD_NET_AMT", sort_types="-1",
    )
    if not data:
        return {"date": trade_date, "total_records": 0, "stocks": [],
                "note": "无数据(非交易日或盘后未更新)"}

    actual_date = str(data[0].get("TRADE_DATE", ""))[:10] if data else trade_date
    stocks = []
    for row in data:
        net_buy = (row.get("BILLBOARD_NET_AMT") or 0) / 10000
        if min_net_buy is not None and net_buy < min_net_buy:
            continue
        stocks.append({
            "code": row.get("SECURITY_CODE", ""),
            "name": row.get("SECURITY_NAME_ABBR", ""),
            "reason": row.get("EXPLANATION", ""),
            "net_buy_wan": round(net_buy, 1),
            "buy_wan": round((row.get("BILLBOARD_BUY_AMT") or 0) / 10000, 1),
            "sell_wan": round((row.get("BILLBOARD_SELL_AMT") or 0) / 10000, 1),
        })
    return {"date": actual_date, "total_records": len(stocks), "stocks": stocks}


# ============================================================
#  筹码层 - a-stock-data 整合(融资融券+大宗交易+股东户数)
# ============================================================

def margin_trading(code: str, page_size: int = 10) -> list[dict]:
    """融资融券明细(日级)。返回: [{date, rzye(融资余额), rzmre(融资买入), rqye(融券余额), ...}]"""
    data = eastmoney_datacenter(
        "RPTA_WEB_RZRQ_GGMX",
        filter_str=f'(SCODE="{code}")',
        page_size=page_size,
        sort_columns="DATE", sort_types="-1",
    )
    rows = []
    for row in data:
        rows.append({
            "date": str(row.get("DATE", ""))[:10],
            "rzye": row.get("RZYE", 0),
            "rzmre": row.get("RZMRE", 0),
            "rzche": row.get("RZCHE", 0),
            "rqye": row.get("RQYE", 0),
        })
    return rows


def block_trade(code: str, page_size: int = 5) -> list[dict]:
    """大宗交易记录。返回: [{date, price, premium_pct, buyer, seller}]"""
    data = eastmoney_datacenter(
        "RPT_DATA_BLOCKTRADE",
        filter_str=f'(SECURITY_CODE="{code}")',
        page_size=page_size,
        sort_columns="TRADE_DATE", sort_types="-1",
    )
    rows = []
    for row in data:
        close = row.get("CLOSE_PRICE") or 0
        deal_price = row.get("DEAL_PRICE") or 0
        premium = ((deal_price / close - 1) * 100) if close else 0
        rows.append({
            "date": str(row.get("TRADE_DATE", ""))[:10],
            "price": deal_price,
            "premium_pct": round(premium, 2),
            "amount": row.get("DEAL_AMT", 0),
            "buyer": row.get("BUYER_NAME", ""),
            "seller": row.get("SELLER_NAME", ""),
        })
    return rows


def holder_num_change(code: str, page_size: int = 4) -> list[dict]:
    """股东户数变化(季度级)。返回: [{date, holder_num, change_num, change_ratio}]"""
    data = eastmoney_datacenter(
        "RPT_HOLDERNUMLATEST",
        filter_str=f'(SECURITY_CODE="{code}")',
        page_size=page_size,
        sort_columns="END_DATE", sort_types="-1",
    )
    rows = []
    for row in data:
        rows.append({
            "date": str(row.get("END_DATE", ""))[:10],
            "holder_num": row.get("HOLDER_NUM", 0),
            "change_num": row.get("HOLDER_NUM_CHANGE", 0),
            "change_ratio": row.get("HOLDER_NUM_RATIO", 0),
        })
    return rows


# ============================================================
#  飞书推送
# ============================================================

# 格式化辅助
def _fmt_amt(amt):
    """格式化金额:万/亿"""
    if amt <= 0:
        return "-"
    if amt >= 1e8:
        return f"{amt/1e8:.2f}亿"
    return f"{amt/1e4:.2f}万"

def _fmt_concepts(code, concept_map):
    """格式化概念标签。第一概念为主分类,其余用+拼接"""
    concepts = concept_map.get(code, [])
    if not concepts:
        return "其他"
    # 去重并保持顺序
    seen = set()
    unique = []
    for c in concepts:
        if c not in seen:
            seen.add(c)
            unique.append(c)
    main = unique[0] if unique else "其他"
    tags = "+".join(unique[1:6]) if len(unique) > 1 else ""
    if tags:
        return f"{main}|{tags}"
    return main


def _extract_keywords(row, concept_map=None):
    """
    提取精准简洁的选股关键词（飞书推送用）。
    优先级: AI分析 > 概念标签 > 量化因子
    格式: 行业|关键词1+关键词2+关键词3（最多6个，每个≤6字）
    """
    code = row.get("代码", "")
    name = row.get("名称", "")

    # 1. 收集AI分析信号
    ai_pos = str(row.get("AI_行业定位", ""))
    ai_logic = str(row.get("AI_投资逻辑", ""))
    ai_catalysts = row.get("AI_催化剂", "")
    if isinstance(ai_catalysts, str):
        try:
            ai_catalysts = json.loads(ai_catalysts) if ai_catalysts.startswith("[") else []
        except Exception:
            ai_catalysts = []

    # 2. 收集量化因子信号
    signals = []
    factor_map = {
        "动量_涨跌幅":    [(0.88, "强势"), (0.75, "资金驱动")],
        "趋势_均线位置":  [(0.88, "多头排列"), (0.72, "站上均线")],
        "技术_MACD金叉":  [(0.92, "MACD金叉")],
        "概念_热度":      [(0.70, "热门概念")],
        "量能_换手率":    [(0.88, "交投活跃")],
        "量能_量比":      [(0.88, "放量")],
        "成长_净利润同比": [(0.80, "业绩高增"), (0.50, "业绩增长")],
        "机构_北向资金":  [(0.70, "外资青睐")],
    }
    for factor, rules in factor_map.items():
        if factor not in row.index:
            continue
        score_val = row[factor]
        if pd.isna(score_val):
            continue
        for threshold, kw in rules:
            if score_val >= threshold and kw not in signals:
                signals.append(kw)
                break

    # 3. 资金面信号
    north = str(row.get("北向信号", ""))
    if "流入" in north:
        signals.append("北向流入")
    dragon = str(row.get("龙虎榜信号", ""))
    if dragon and dragon not in ("", "nan"):
        signals.append("龙虎榜")

    # 4. 概念标签
    concepts = concept_map.get(code, []) if concept_map else []
    concept_tags = [c for c in concepts[:3] if len(c) <= 8]

    # 5. 组装: 行业 + 关键词
    # 行业: 优先AI行业定位，否则取第一个概念
    industry = ""
    if ai_pos and ai_pos not in ("", "nan"):
        industry = ai_pos.split("|")[0].split("，")[0][:6]
    elif concept_tags:
        industry = concept_tags[0]

    # 关键词: AI催化剂 + 量化信号 + 概念（去重，最多6个）
    kw_parts = []
    for cat in ai_catalysts[:2]:
        if isinstance(cat, dict):
            event = cat.get("事件", cat.get("因素", ""))
            if event and len(event) <= 10:
                kw_parts.append(event[:6])
    for s in signals[:3]:
        if s not in kw_parts:
            kw_parts.append(s)
    for c in concept_tags[1:3]:
        if c not in kw_parts and len(c) <= 8:
            kw_parts.append(c)

    # 去重、限制6个
    seen = set()
    final_kw = []
    for k in kw_parts:
        if k not in seen and k != industry:
            seen.add(k)
            final_kw.append(k)
        if len(final_kw) >= 6:
            break

    if not final_kw:
        final_kw.append("综合优选")

    # 格式: 行业|关键词1+关键词2
    if industry:
        return f"{industry}|{'+'.join(final_kw)}"
    return "+".join(final_kw)


def _get_selection_keywords(row, concept_map=None, news_items=None):
    """
    生成参考图片格式的选股理由。

    格式:行业定位|细分赛道+核心产品/技术+应用场景+催化剂

    参考案例:
      - 光通信|光纤/光棒+康宁+数据中心
      - 其他|摘帽+大飞机+可控核聚变+低空经济
      - 算力|token工厂+算力资产注入猜想+减肥药+儿童用药+NMN
      - 钨|六氟化钨+电子特气+氦气+供货长江存储+光纤级四氯化硅
    """
    code = row.get("代码", "")
    name = row.get("名称", "")
    industry = str(row.get("行业", "其他"))

    # ── 收集所有可用信号 ──
    news_sig = str(row.get("新闻信号", ""))
    report_sig = str(row.get("研报信号", ""))
    dragon_sig = str(row.get("龙虎榜信号", ""))
    north_sig = str(row.get("北向信号", ""))
    chip_sig = str(row.get("筹码信号", ""))
    base_sig = str(row.get("基础信号", ""))
    chg = row.get("涨跌幅", 0)
    turnover = row.get("换手率", 0)
    vol_ratio = row.get("量比", 1.0)

    # ============================================================
    #  1. 确定「行业定位」- | 之前的部分
    # ============================================================
    concepts = []
    if concept_map and code in concept_map:
        concepts = concept_map[code]

    position = "其他"  # 默认行业定位
    if concepts:
        # 取第一个概念作为行业定位
        position = concepts[0]
    elif industry and industry not in ("", "nan", "其他"):
        position = industry

    # ============================================================
    #  2. 构建「细分赛道+产品+应用+催化剂」
    # ============================================================
    detail_parts = []

    # ── 2a. 从概念映射提取产品和细分 ──
    if concepts:
        # 概念列表中的第2-5项作为细分产品标签
        for c in concepts[1:]:
            if c not in detail_parts and len(c) <= 12:
                detail_parts.append(c)
        # 如果概念不够,用行业作为补充
        if len(detail_parts) < 2:
            # 从行业关键词中提取产品特征
            pass

    # ── 2b. 从消息面信号提取催化剂 ──
    if news_sig and news_sig not in ("", "nan"):
        # 提取具体新闻动作
        if "利好" in news_sig: detail_parts.append("利好公告")
        if "业绩" in news_sig: detail_parts.append("业绩预增")
        if "中标" in news_sig: detail_parts.append("大单中标")
        if "订单" in news_sig: detail_parts.append("重大订单")
        if "回购" in news_sig: detail_parts.append("股份回购")
        if "减持" in news_sig: detail_parts.append("减持利空")
        if "摘帽" in news_sig: detail_parts.append("摘帽")

    if report_sig and report_sig not in ("", "nan"):
        detail_parts.append("机构看好")

    # ── 2c. 从因子得分提取核心逻辑 ──
    factor_tags = []
    factor_map = {
        "动量_涨跌幅":    [(0.88, "强势"), (0.75, "资金驱动")],
        "趋势_均线位置":  [(0.88, "多头排列"), (0.72, "站上均线")],
        "技术_MACD金叉":  [(0.92, "MACD金叉")],
        "概念_热度":      [(0.70, "热门概念")],
        "量能_换手率":    [(0.88, "交投活跃")],
        "量能_量比":      [(0.88, "放量")],
        "成长_净利润同比": [(0.80, "业绩高增"), (0.50, "业绩增长")],
        "成长_营收同比":   [(0.80, "营收高增"), (0.50, "营收增长")],
        "机构_北向资金":  [(0.70, "外资青睐")],
    }
    for factor, rules in factor_map.items():
        if factor not in row.index: continue
        score = row[factor]
        if pd.isna(score): continue
        for threshold, kw in rules:
            if score >= threshold and kw not in factor_tags:
                factor_tags.append(kw)
                break
    # 最多取2个核心因子标签
    for tag in factor_tags[:2]:
        if tag not in detail_parts:
            detail_parts.append(tag)

    # ── 2d. 资金面信号 ──
    if north_sig and north_sig not in ("", "nan", "无数据"):
        if "流入" in north_sig: detail_parts.append("北向流入")
        elif "流出" in north_sig: detail_parts.append("北向流出")
    if dragon_sig and dragon_sig not in ("", "nan"):
        detail_parts.append("龙虎榜")
    if chip_sig and chip_sig not in ("", "nan"):
        detail_parts.append(chip_sig[:6])

    # ── 2e. 基础面信号 ──
    if base_sig and base_sig not in ("", "nan"):
        if "业绩" in base_sig: detail_parts.append("业绩向好")
        elif "ROE" in base_sig or "高" in base_sig: detail_parts.append("高ROE")
        else: detail_parts.append(base_sig[:8])

    # ── 2f. 价格量能信号(作为最后补充) ──
    if not pd.isna(chg):
        limit = _get_limit_up_threshold(row.get("代码", ""))
        if chg >= limit - 0.3: detail_parts.append("涨停")
        elif chg >= 5: detail_parts.append(f"涨{chg:.0f}%")
    if not pd.isna(turnover) and turnover > 15:
        detail_parts.append("巨量换手")
    elif not pd.isna(vol_ratio) and vol_ratio > 3:
        detail_parts.append("量比放大")

    # ============================================================
    #  3. 去重、限制长度、组合
    # ============================================================
    seen = set()
    unique_parts = []
    for p in detail_parts:
        if p not in seen and p != position:
            seen.add(p)
            unique_parts.append(p)

    if not unique_parts:
        unique_parts.append("综合优选")

    # 行业定位 + | + 具体细节(最多8个,用+连接)
    detail_str = "+".join(unique_parts[:8])
    result = f"{position}|{detail_str}"

    # 限制总长度
    if len(result) > 60:
        result = result[:57] + "..."

    return result





def _push_to_dingtalk(webhook_url, payload):
    """钉钉机器人推送（使用与飞书相同的markdown文本）"""
    if not webhook_url or "未填写" in webhook_url or "xxxxxx" in webhook_url:
        return "skip"
    try:
        # 钉钉webhook格式: { "msgtype": "markdown", "markdown": { "text": "..." } }
        # 从飞书payload中提取markdown内容
        text = ""
        if payload.get("msg_type") == "interactive":
            elements = payload.get("card", {}).get("elements", [])
            for el in elements:
                if el.get("tag") == "markdown":
                    text = el.get("content", "")
                    break
        elif payload.get("msg_type") == "text":
            text = payload.get("content", {}).get("text", "")

        if not text:
            return "skip"

        dd_payload = {
            "msgtype": "markdown",
            "markdown": {
                "title": "量化选股",
                "text": text
            }
        }
        r = requests.post(webhook_url, json=dd_payload, timeout=10)
        if r.status_code == 200 and r.json().get("errcode") == 0:
            log("钉钉推送成功 ✓")
            return True
        else:
            log(f"钉钉推送失败: {r.status_code} {r.text[:200]}", "WARN")
            return False
    except Exception as e:
        log(f"钉钉推送异常: {e}", "WARN")
        return False


def push_to_feishu(webhook_url, df, meta, concept_map=None, dingtalk_webhook=""):
    """推送选股结果到飞书+钉钉（双通道，任一成功即可）"""
    # ── 构建飞书payload（与原逻辑完全一致）──
    if not webhook_url or "未填写" in webhook_url or "xxxxxx" in webhook_url:
        log("飞书/钉钉 Webhook 均未配置, 跳过推送", "WARN")
        return False

    now = get_now()
    top_n = meta.get("top_n", 3)

    # 如果选股结果为空，推送告警
    if df is None or len(df) == 0:
        alert_payload = {
            "msg_type": "text",
            "content": {
                "text": f"⚠️ [{now.strftime('%H:%M')}] 今日无符合条件的股票\n"
                        f"可能原因: 竞价/盘后数据不完整、清洗条件过严、或数据源异常。\n"
                        f"建议: 检查日志确认数据源状态。"
            }
        }
        try:
            requests.post(webhook_url, json=alert_payload, timeout=5)
            log("飞书推送: 无符合条件的股票，已发送告警")
        except Exception as e:
            log(f"飞书告警推送失败: {e}", "WARN")
        # 钉钉同步告警
        _push_to_dingtalk(dingtalk_webhook, alert_payload)
        return False

    # 构建消息
    lines = []
    lines.append(f"📊 Top{top_n} 选股结果")
    lines.append("")

    df_top = df.head(top_n)

    for i, (_, row) in enumerate(df_top.iterrows(), 1):
        code = row["代码"]
        name = row["名称"]
        score = row["综合得分"]

        _base_kw = _get_selection_keywords(row, concept_map)
        _concepts = concept_map.get(code, []) if concept_map else []
        _ai_kw = str(row.get("AI_关键词", ""))
        if not _ai_kw or _ai_kw in ("", "nan"):
            _ai_kw = ""

        if _ai_kw:
            if "|" in _base_kw:
                keywords = f"{_base_kw}+{_ai_kw}"
            else:
                keywords = f"{_base_kw}|{_ai_kw}"
        else:
            keywords = _base_kw

        if len(keywords) > 65:
            keywords = keywords[:62] + "..."

        jjzf = row.get("竞价涨幅", np.nan)
        jjzf_str = f"{jjzf:+.2f}%" if not pd.isna(jjzf) and jjzf != 0 else "-"
        jje = row.get("竞价额", np.nan)
        jje_str = _fmt_amt(jje) if not pd.isna(jje) and jje > 0 else (
            _fmt_amt(row.get("成交额", 0)) if row.get("成交额", 0) > 0 else "-"
        )

        lines.append(f"Top{i} {name} ({code})")
        lines.append(f"{keywords}")
        _score_display = f"{score:.1f}" if score > 10 else f"{score * 100:.1f}"
        lines.append(f"竞价涨幅 {jjzf_str} | 竞价额 {jje_str} | 打分 {_score_display}")

        is_limit_up = False
        if "涨跌幅" in row.index and pd.notna(row["涨跌幅"]):
            change_pct = float(row["涨跌幅"])
            limit = _get_limit_up_threshold(code)
            is_limit_up = change_pct >= limit

        if is_limit_up:
            lines.append(f"⚠️ 今日涨停，次日可能高开或买不到，建议设条件单")
        else:
            lines.append(f"📌 次日开盘挂单买入，仓位10%，持有3-5天或触及止损-8%退出")
        lines.append("")

    in_session, session_name, _ = get_market_session()
    time_str = now.strftime('%m-%d %H:%M')
    lines.append(f"-- {now.strftime('%Y-%m-%d')} {time_str} | {session_name} | v{VERSION}")

    if _data_provenance:
        last = _data_provenance[-1]
        src = last["数据来源"]
        extra = last.get("额外信息", {})
        cv = extra.get("交叉验证", {})
        if cv and cv.get("status") == "warn":
            lines.append(f"⚠数据源: {src}(东财降级,交叉验证存疑)")
        elif src not in (SRC_EASTMONEY,):
            lines.append(f"数据源: {src}(已降级)")

    lines.append("")
    lines.append("⚠️ **风险提示**")
    try:
        from paper_trader import get_trade_summary
        _ts = get_trade_summary()
        _closed = _ts.get("已平仓数", 0)
        if _closed > 0:
            _max_loss = _ts.get("最大亏损", 0)
            _max_profit = _ts.get("最大盈利", 0)
            _win_rate = _ts.get("总胜率", 0)
            _avg_pnl = _ts.get("平均盈亏率", 0)
            lines.append(
                f"- 模拟交易 {_closed} 笔 | 胜率 {_win_rate*100:.0f}% | "
                f"平均收益 {_avg_pnl*100:+.1f}% | "
                f"单笔最大亏损 {_max_loss*100:.1f}% | 单笔最大盈利 {_max_profit*100:+.1f}%"
            )
        else:
            lines.append("- 策略运行中，暂无已平仓交易数据")
    except Exception:
        lines.append("- 策略运行中，暂无历史统计数据")
    lines.append("- 本策略为高频交易，不适合风险厌恶型投资者")
    lines.append("- 投资有风险，入市需谨慎")

    payload = {
        "msg_type": "interactive",
        "card": {
            "header": {
                "title": {"tag": "plain_text", "content": f"📊 量化选股 v{VERSION}"},
                "template": "blue"
            },
            "elements": [
                {"tag": "markdown", "content": "\n".join(lines)}
            ]
        }
    }

    # ── 双通道推送：飞书 + 钉钉 ──
    feishu_ok = False
    dingtalk_ok = False

    # 飞书推送
    try:
        r = requests.post(webhook_url, json=payload, timeout=10)
        if r.status_code == 200 and r.json().get("code") == 0:
            log("飞书推送成功 ✓")
            feishu_ok = True
        else:
            log(f"飞书推送失败: {r.status_code} {r.text[:200]}", "WARN")
    except Exception as e:
        log(f"飞书推送异常: {e}", "WARN")

    # 钉钉推送（同步，使用相同消息内容）
    if dingtalk_webhook:
        dd_result = _push_to_dingtalk(dingtalk_webhook, payload)
        if dd_result is True:
            dingtalk_ok = True

    # 任一成功即为推送成功
    if feishu_ok or dingtalk_ok:
        return True
    log("飞书+钉钉推送均失败", "WARN")
    return False


# ============================================================
#  格式化输出
# ============================================================

def format_output(df, concept_map, top_n=10):
    """格式化终端输出 - v5.5 含竞价+概念+选他原因关键词"""
    lines = []
    lines.append("")
    lines.append("╔" + "═" * 78 + "╗")
    lines.append(f"║  量化选股系统 v{VERSION}  |  {get_now().strftime('%Y-%m-%d %H:%M:%S')}  |  Top {top_n}")

    in_session, session_name, _ = get_market_session()
    session_str = f"{session_name} (盘中)" if in_session else session_name
    lines.append(f"║  时段: {session_str}")
    lines.append("╠" + "═" * 78 + "╣")

    # 直接按综合得分排序取TopN（不强制行业分散）
    df_top = df.head(top_n)

    for idx, row in df_top.iterrows():
        code   = row["代码"]
        name   = row["名称"]
        score  = row["综合得分"]

        # 竞价涨幅
        jjzf = row.get("竞价涨幅", np.nan)
        jjzf_str = f"{jjzf:+.2f}%" if not pd.isna(jjzf) and jjzf != 0 else "-"

        # 竞价额
        jje = row.get("竞价额", np.nan)
        jje_str = _fmt_amt(jje) if not pd.isna(jje) and jje > 0 else "-"

        # 概念
        concepts_str = _fmt_concepts(code, concept_map)
        if len(concepts_str) > 50:
            concepts_str = concepts_str[:48] + ".."

        # 选他原因关键词: 优先用AI
        ai_pos = str(row.get("AI_行业定位", ""))
        ai_logic = str(row.get("AI_投资逻辑", ""))
        if ai_pos and ai_pos not in ("", "nan"):
            keywords = ai_pos
            if ai_logic and ai_logic not in ("", "nan"):
                keywords += "|" + ai_logic[:40]
        else:
            keywords = _get_selection_keywords(row, concept_map)

        # ── 2026-06-23 新增深度因子展示 ──
        growth_items = []
        for f in ["成长_净利润同比", "成长_营收同比", "机构_北向资金", "机构_基金持仓", "研报_覆盖度", "景气_行业"]:
            v = row.get(f, None)
            if v is not None and isinstance(v, (int, float)) and v != 0.5:
                label = f.replace("成长_", "").replace("机构_", "").replace("研报_", "").replace("景气_", "")
                growth_items.append(f"{label}={v:.2f}")
        growth_str = " | ".join(growth_items) if growth_items else ""

        lines.append(f"║  Top{idx+1}  {name} ({code})")
        lines.append(f"║  概念  {concepts_str}")
        lines.append(f"║  竞价涨幅 {jjzf_str}  |  竞价额 {jje_str}  |  打分 {score:.4f}")
        lines.append(f"║  选他原因  {keywords}")
        if growth_str:
            lines.append(f"║  📊深度因子  {growth_str}")
        if idx < top_n - 1:
            lines.append("║  " + "─" * 76)

    lines.append("╠" + "═" * 78 + "╣")
    lines.append("║  💡 得分 = 多因子量化排序 | 按分数买入 Top3")
    lines.append("╚" + "═" * 78 + "╝")
    return "\n".join(lines)


def save_results(df, top_n=10):
    """保存结果到 output/"""
    today = get_now().strftime("%Y%m%d_%H%M%S")
    out_csv = OUT_DIR / f"选股结果_{today}.csv"
    out_json = OUT_DIR / f"选股结果_{today}.json"

    # 保存完整列,供 agent_bridge 使用
    save_cols = ["排名", "代码", "名称", "综合得分", "涨跌幅", "换手率", "量比",
                  "总市值", "市盈率动态", "行业",
                  "动量_涨跌幅", "动量_5日涨幅", "趋势_均线位置", "趋势_RSI强度",
                  "量能_换手率", "量能_量比", "估值_PE反向", "估值_PB反向",
                  "规模_小市值", "技术_MACD金叉", "概念_热度", "质量_盈利",
                  "成长_净利润同比", "成长_营收同比", "机构_北向资金", "机构_基金持仓",
                  "研报_覆盖度", "景气_行业",
                  "新闻信号", "研报信号", "龙虎榜信号", "北向信号", "筹码信号", "基础信号"]
    available = [c for c in save_cols if c in df.columns]

    # 直接按综合得分排序取TopN（不强制行业分散）
    df_top = df.head(top_n)

    df_top[available].to_csv(out_csv, index=False, encoding="utf-8-sig")

    result_data = df_top[available].to_dict("records")
    out_json.write_text(json.dumps(result_data, ensure_ascii=False, indent=2), encoding="utf-8")

    log(f"结果已保存:")
    log(f"  CSV:  {out_csv.name}")
    log(f"  JSON: {out_json.name}")


# ============================================================
#  Windows 定时任务管理
# ============================================================

def check_scheduled_task():
    """检查 Windows 定时任务是否已安装"""
    import subprocess
    try:
        result = subprocess.run(
            ["schtasks", "/query", "/tn", "QuantStockPicker"],
            capture_output=True, text=True, timeout=5
        )
        if result.returncode == 0:
            log("✓ Windows 定时任务 'QuantStockPicker' 已安装")
            return True
        else:
            log("✗ Windows 定时任务未安装 (运行 安装定时任务.bat 可安装)")
            return False
    except FileNotFoundError:
        log("i 非 Windows 环境或 schtasks 不可用")
        return None
    except Exception as e:
        log(f"定时任务检查失败: {e}", "WARN")
        return None


# ============================================================
#  主流程
# ============================================================

concept_map = {}  # 全局概念映射
df_scored_global = None  # 进化引擎需要的全量因子得分
df_quotes_global = None  # 进化引擎需要的原始行情


def main(top_n=10, tech_limit=0, enable_feishu=False, feishu_webhook="", config=None):
    global concept_map, _data_provenance, df_scored_global, df_quotes_global

    # 从config读取钉钉webhook（与飞书webhook并列）
    dingtalk_webhook = (config or {}).get("dingtalk_webhook", "")

    _data_provenance = []
    run_start = get_now()

    # ── 时间保护:9:00-9:29 运行时警告并自动等待 ──
    # 东方财富/腾讯/新浪等数据源在 9:30 开盘后才更新当日数据
    # 但如果 config 中有 _load_yesterday_ai（竞价阶段），跳过等待
    _is_auction = (config or {}).get("_load_yesterday_ai", False)
    if not _is_auction and run_start.weekday() < 5:
        if run_start.hour == 9 and run_start.minute < 30:
            wait_until = run_start.replace(minute=31, second=0, microsecond=0)
            wait_sec = int((wait_until - run_start).total_seconds())
            if wait_sec > 0:
                log("=" * 70)
                log(f"⏰ 时间保护:当前时间 {run_start.strftime('%H:%M')} 在开盘前")
                log(f"   东方财富等数据源在 9:30 开盘后才更新当日数据")
                log(f"   程序将自动等待到 9:31 再开始获取数据(约 {wait_sec} 秒)...")
                log("=" * 70)
                time.sleep(wait_sec)
                log(f"✅ 已到 9:31,开始获取数据")

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
    # 注意: 竞价时段(09:15-09:25)和盘后实时行情换手率/PE/市值可能为0,
    #       此时放宽条件避免全部过滤
    original_count = len(df)
    in_session_now, session_now, _ = get_market_session()
    is_auction = (session_now == "非交易时段(工作日)" and
                  9 <= get_now().hour <= 9 and 15 <= get_now().minute <= 25)

    df = df[df["最新价"] > 0]

    # 换手率: 盘中要求 >1%, 竞价/盘后放宽为 >0 (允许集合竞价数据)
    turnover_min = 0.01 if in_session_now and not is_auction else -0.01
    df = df[df["换手率"] > turnover_min]

    df = df[~df["名称"].str.contains("ST|退", na=False)]

    # 过滤北交所股票(920/430/830/87/88开头),流动性差、散户难参与
    df = df[~df["代码"].str.match(r"^(920|430|830|87|88)", na=False)]

    # 过滤亏损股: 盘中要求PE>0, 竞价/盘后跳过PE过滤(数据可能不准)
    if "市盈率动态" in df.columns and in_session_now and not is_auction:
        profitable_mask = df["市盈率动态"] > 0
        excluded_loss = (~profitable_mask).sum()
        df = df[profitable_mask]
    else:
        excluded_loss = 0

    # 过滤高估值股: 盘中要求PE<=100, 竞价/盘后跳过
    if "市盈率动态" in df.columns and in_session_now and not is_auction:
        pe_reasonable_mask = df["市盈率动态"] <= 100
        excluded_high_pe = (~pe_reasonable_mask).sum()
        df = df[pe_reasonable_mask]
    else:
        excluded_high_pe = 0

    # 过滤极小市值: 盘中要求>=20亿, 竞价/盘后跳过
    if "总市值" in df.columns and in_session_now and not is_auction:
        min_mv_mask = df["总市值"] >= 20_0000_0000  # 20亿
        excluded_small = (~min_mv_mask).sum()
        df = df[min_mv_mask]
    else:
        excluded_small = 0

    # 过滤低价股(< 3元),避免仙股 — 任何时段都保留
    df = df[df["最新价"] >= 3.0]

    # 过滤涨跌停股票 — 涨停买不进、跌停卖不出，选出来无操作意义
    # ⚠ 竞价时段跳过: 集合竞价的涨跌幅是昨天的收盘数据，不代表今日实际涨跌停状态
    excluded_limit = 0
    if "涨跌幅" in df.columns and in_session_now and not is_auction:
        _limit_mask = df.apply(
            lambda r: float(r.get("涨跌幅", 0)) >= _get_limit_up_threshold(r.get("代码", "")) - 0.5,
            axis=1
        )
        excluded_limit = _limit_mask.sum()
        df = df[~_limit_mask]

    # 跌停股也过滤（跌停卖不出，买入无意义）— 对称处理
    if "涨跌幅" in df.columns and in_session_now and not is_auction:
        _limit_down_mask = df.apply(
            lambda r: float(r.get("涨跌幅", 0)) <= -(_get_limit_up_threshold(r.get("代码", "")) - 0.5),
            axis=1
        )
        excluded_limit += _limit_down_mask.sum()
        df = df[~_limit_down_mask]

    # 竞价阶段已涨停/跌停：竞价涨幅超阈值
    if "竞价涨幅" in df.columns:
        _auction_limit = df.apply(
            lambda r: abs(float(r.get("竞价涨幅", 0) or 0)) >= _get_limit_up_threshold(r.get("代码", "")) - 0.5,
            axis=1
        )
        excluded_limit += _auction_limit.sum()
        df = df[~_auction_limit]

    log(f"清洗后: {len(df)} 只 (去除停牌/ST/退市/北交所/亏损股/高估值/小市值/低价股/涨停股)")
    log(f"  排除: 北交所+亏损+高估值(PE>100)+小市值+低价+涨停 共 {original_count - len(df)} 只 (其中涨停股 {excluded_limit} 只)")
    if len(df) == 0:
        warn("⚠ 清洗后股票池为空! 请检查数据源或放宽过滤条件")

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
    # 只有调用方明确标记 _is_trading_run 才跳过AI（由竞价推送run.py传入）
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
                log(f"  [AI因子] 加载昨日AI打分: {len(_yesterday_ai)} 只, 关键词: {sum(1 for v in _yesterday_keywords.values() if v)} 只")
            else:
                log("  [AI因子] 昨日AI分析文件不存在")
        except Exception as e:
            log(f"  [AI因子] 加载失败: {e}")

    if _ai_cfg.get("enabled", False) and _ai_analysis_available and not _skip_ai:
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
        if "AI_分析成功" in df.columns:
            log(f"  AI分析成功: {sum(1 for s in df['AI_分析成功'] if s)}/{_ai_top_n}")
    
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
                        code=str(row.get("代码", "")),
                        name=row.get("名称", ""),
                        snapshot_date=snapshot_date,
                        ai_score=int(row.get("AI_评分", 50) if pd.notna(row.get("AI_评分", 50)) else 50),
                        ai_result={
                            "行业定位": row.get("AI_行业定位", ""),
                            "催化剂": catalysts,
                            "利空因素": bearish,
                        },
                        composite_score=float(row.get("综合得分", 0) if pd.notna(row.get("综合得分", 0)) else 0),
                        concept_list=concept_map.get(str(row.get("代码", "")), []),
                    )
            _ai_success_count = 0
            if "AI_分析成功" in df.columns:
                _ai_success_count = sum(1 for s in df.head(_ai_top_n)["AI_分析成功"] if s)
            log(f"  AI反馈记录: 已记录 {_ai_success_count} 条预测")
        except Exception as e:
            log(f"  AI反馈记录失败: {e}", "WARN")
    elif not _ai_analysis_available:
        log("  AI分析模块未安装,跳过(运行 pip install -r requirements.txt 安装)")
    elif not _ai_cfg.get("enabled", False):
        log("  AI分析未启用(config.json 中 ai_analysis.enabled=false)")

    # 融合昨日AI打分+关键词作为因子
    if _yesterday_ai:
        _ai_weight = _ai_cfg.get("score_weight", 0.08)
        df["AI因子"] = 50.0
        df["AI_关键词"] = ""
        _matched = 0
        _matched_kw = 0
        for idx, row in df.iterrows():
            _code = str(row.get("代码", "")).zfill(6)
            if _code in _yesterday_ai:
                df.at[idx, "AI因子"] = _yesterday_ai[_code]
                _matched += 1
                kw = _yesterday_keywords.get(_code, "")
                if kw:
                    df.at[idx, "AI_关键词"] = kw
                    _matched_kw += 1
        log(f"  [AI因子] 匹配 {_matched}/{len(df)} 只, 关键词 {_matched_kw} 只, 权重={_ai_weight}")

        # 归一化 + 融合
        _ai_scores = df["AI因子"].values.astype(float)
        if len(_ai_scores) == 0 or np.all(np.isnan(_ai_scores)):
            log(f"  [AI因子] 无有效AI因子数据，跳过融合", "WARN")
        else:
            _ai_min, _ai_max = _ai_scores.min(), _ai_scores.max()
            if _ai_max > _ai_min:
                _ai_norm = (_ai_scores - _ai_min) / (_ai_max - _ai_min) * 100
            else:
                _ai_norm = np.full(len(_ai_scores), 50.0)
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
    output = format_output(df, concept_map, top_n)
    print(output)

    # 数据溯源报告
    print_provenance_report()

    # 竞价数据补充(为飞书推送做准备)
    log("")
    log("── 竞价数据补充 ──")
    df = enrich_auction_data(df, top_n)

    # 消息面/信号/筹码增强(a-stock-data 整合)
    # 含二次评分: 深度因子(成长/质量/研报/机构/景气)在此阶段填充真实值后重算综合得分
    log("")
    log("── 消息面/信号/筹码增强 ──")
    df = enrich_news_and_signals(df, top_n)

    # 保存结果 & 历史归档(回测核心) — 必须在二次评分之后，确保快照含最终分数
    save_results(df, top_n)
    archive_run(df, concept_map, {"top_n": top_n, "tech_limit": tech_limit})

    # 飞书+钉钉推送（双通道，任一成功即可）
    if enable_feishu and (feishu_webhook or dingtalk_webhook):
        push_to_feishu(feishu_webhook, df, {"top_n": top_n}, concept_map,
                       dingtalk_webhook=dingtalk_webhook)

    # 最终统计
    run_elapsed = (get_now() - run_start).total_seconds()
    log("")
    log(f"📊 统计:")
    log(f"  扫描: {len(df)} 只")
    if len(df) > 0:
        log(f"  最高分: {df['综合得分'].iloc[0]:.1f}")
        log(f"  最低分: {df['综合得分'].iloc[-1]:.1f}")
    else:
        log(f"  最高分: N/A")
        log(f"  最低分: N/A")
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
                "sentiment": ["景气_行业", "情绪_市场", "情绪_板块", "LLM_信号"],
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

            # 新增因子默认保留
            for default_factor, default_w in [("情绪_市场", 0.02), ("情绪_板块", 0.02), ("LLM_信号", 0.02)]:
                if default_factor not in new_weights:
                    new_weights[default_factor] = default_w

            # 归一化
            total = sum(new_weights.values())
            if total > 0:
                new_weights = {k: round(v/total, 4) for k, v in new_weights.items()}
                FACTOR_WEIGHTS = new_weights

        # 加载热门概念
        json_concepts = cfg.get("hot_concepts", [])
        if json_concepts:
            HOT_CONCEPTS = json_concepts

        # 优先从 credentials 读取 feishu_webhook，兼容旧配置顶层字段
        webhook = cfg.get("feishu_webhook", "")
        if not webhook:
            webhook = cfg.get("credentials", {}).get("feishu_webhook", "")
        return cfg, webhook
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
        return any(HISTORY_DIR.glob(f"snapshot_{today_compact}_*.csv")) if HISTORY_DIR.exists() else False
    elif phase == "盘后回测迭代":
        reports_dir = BASE_DIR / "reports"
        return any(reports_dir.glob(f"回测报告_{today_compact}_*.json")) if reports_dir.exists() else False
    elif phase == "盘中模拟交易":
        return any(HISTORY_DIR.glob(f"snapshot_{today_compact}_*.csv")) if HISTORY_DIR.exists() else False
    return False


def daemon_sleep():
    """
    计算下一次运行前的休眠秒数。
    四阶段调度:
      盘后回测 (15:08-15:28) → 2分钟后
      等待晚间 (15:28-16:57) → 算到16:58
      盘后AI分析 (16:58+) → 10分钟
      竞价量化 (9:21-9:27) → 1分钟
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

    # 等待AI分析开始(15:28-16:57): 算到16:58
    if (h == 15 and m >= 28) or (h == 16 and m < 58):
        target = now.replace(hour=16, minute=58, second=0, microsecond=0)
        return max(60, int((target - now).total_seconds()))

    # 盘后AI分析 (16:58+): 10分钟
    if h >= 17 or (h == 16 and m >= 58):
        return 600

    # 竞价 (9:21-9:27): 1分钟
    if h == 9 and 21 <= m < 28:
        return 60

    # 盘前 (9:18-9:20): 1分钟
    if h == 9 and 18 <= m < 21:
        return 60

    # 其他: 1分钟
    return 60


def run_daemon(top_n, tech_limit, enable_feishu, webhook, config=None):
    """
    常驻守护主循环 — 四阶段调度:
      盘后回测 (15:08-15:28): 验证排名/模拟/AI + 迭代
      盘后AI分析 (16:58+): 尽早开始,凌晨完成即可
      竞价量化推送 (9:21-9:27): 量化打分(含昨日AI因子) + 推送Top3
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
            elif h == 15 and 10 <= m < 30:
                phase = "盘后回测迭代"
            elif h >= 17:
                phase = "盘后AI分析"
            elif h == 9 and 23 <= m < 30:
                phase = "竞价量化推送"
            elif in_session and not (h == 12 or (h == 11 and m > 30)):
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
                main(top_n, tech_limit, enable_feishu, webhook, config=_run_cfg)

            elif phase == "盘中模拟交易":
                # ── 盘中模拟交易：更新持仓 + 仅首次买入 ──
                try:
                    from paper_trader import open_positions, update_positions, get_trade_summary, _get_positions_file
                    # 1. 更新现有持仓（止损/止盈/到期检查）
                    update_positions()
                    
                    # 2. 检查今日是否已买入（避免重复买入）
                    today = get_now().strftime("%Y-%m-%d")
                    pos_file = _get_positions_file()
                    today_already_bought = False
                    today_bought_stocks = []
                    if pos_file.exists():
                        import pandas as pd
                        df_pos = pd.read_parquet(pos_file)
                        today_mask = df_pos["买入日期"] == today
                        today_already_bought = today_mask.any()
                        if today_already_bought:
                            today_bought_stocks = df_pos[today_mask][["代码", "名称", "买入价", "买入日期"]].to_dict("records")
                    
                    # 3. 仅首次买入，后续只更新持仓
                    if not today_already_bought:
                        today_snap = sorted(HISTORY_DIR.rglob(f"snapshot_{get_now().strftime('%Y%m%d')}_*.csv"), reverse=True)
                        if today_snap:
                            df_today = pd.read_csv(today_snap[0])
                            buy_count = open_positions(df_today.head(top_n))
                            if buy_count > 0:
                                # 获取今日买入的详细信息
                                df_pos_new = pd.read_parquet(pos_file) if pos_file.exists() else pd.DataFrame()
                                if len(df_pos_new) > 0:
                                    today_mask_new = df_pos_new["买入日期"].astype(str) == today
                                    today_bought_stocks = df_pos_new[today_mask_new][["代码", "名称", "买入价", "买入时间"]].to_dict("records") if today_mask_new.any() else []
                                else:
                                    today_bought_stocks = []
                                log(f"  [模拟] 首次买入: 买入 {buy_count} 只")
                                for stock in today_bought_stocks:
                                    log(f"    - {stock['名称']}({stock['代码']}) 买入价:{stock['买入价']:.2f} 买入时间:{stock['买入时间']}")
                            else:
                                log(f"  [模拟] 首次买入: 所有股票已持有，跳过")
                        else:
                            log(f"  [模拟] 今日无快照，跳过")
                    else:
                        # 已买入，显示今日买入的详细信息
                        log(f"  [模拟] 今日已买入 {len(today_bought_stocks)} 只:")
                        for stock in today_bought_stocks:
                            log(f"    - {stock['名称']}({stock['代码']}) 买入价:{stock['买入价']:.2f} 买入时间:{stock['买入时间']}")
                        # 输出持仓更新状态
                        summary = get_trade_summary()
                        if summary:
                            active = summary.get("当前持仓", 0)
                            pnl = summary.get("总浮动盈亏率", 0)
                            log(f"  [模拟] 持仓更新: {active}只，浮动盈亏 {pnl:+.2%}")
                except Exception as e:
                    print(f"  [模拟] 异常: {e}")

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
