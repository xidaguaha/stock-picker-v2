#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bug日志工具 — 兼容层，底层转发到 logger.py 统一日志系统
用法不变:
    from bug_log import setup_logger, log_error, log_info
    logger = setup_logger("盘后AI分析")
    log_info("开始执行")
"""
import sys
import traceback
from datetime import datetime
from pathlib import Path

def log(msg, level="INFO"):
    """简易日志函数（防止异常路径调用未定义的log导致NameError崩溃）"""
    from datetime import datetime as _dt
    print(f"[{_dt.now().strftime('%H:%M:%S')}] [{level}] {msg}")


# 尝试导入统一日志系统
try:
    from logger import get_logger, init_logger
    _HAS_LOGGER = True
except Exception:
    _HAS_LOGGER = False

# 日志目录（向后兼容）
def _get_log_dir():
    p = Path(__file__).parent.parent  # 指向项目根目录
    for _ in range(5):
        if (p / "config.json").exists():
            return p / "logs" / "bug"
        p = p.parent
    return Path(__file__).parent / "logs" / "bug"

LOG_DIR = _get_log_dir()
LOG_DIR.mkdir(parents=True, exist_ok=True)

# 向后兼容：模块级状态
_current_module = "unknown"
_log_file = None
_logger = None


def setup_logger(module_name: str):
    """设置当前模块名（兼容接口，底层使用 logger.py）"""
    global _current_module, _log_file, _logger
    _current_module = module_name
    today = datetime.now().strftime("%Y-%m-%d")
    safe_name = module_name.replace("/", "_").replace("\\", "_")
    _log_file = LOG_DIR / f"{safe_name}_{today}.log"

    if _HAS_LOGGER:
        _logger = init_logger(run_id=safe_name)
        log_info(f"=== {module_name} 启动 ===")
    else:
        # fallback: 直接写兼容日志
        _write_compat(f"=== {module_name} 启动 ===", "INFO")
    return _log_file


def _write_compat(msg: str, level: str = "INFO"):
    """向后兼容的直接写入（logger.py 不可用时）"""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] [{level}] [{_current_module}] {msg}\n"
    if _log_file:
        try:
            with open(_log_file, "a", encoding="utf-8") as f:
                f.write(line)
        except Exception as e:
            log(f"操作异常: {e}", "WARN")
    sys.stdout.write(line)
    sys.stdout.flush()


def log_info(msg: str):
    """记录普通信息"""
    if _logger:
        _logger.info(msg)
    else:
        _write_compat(msg, "INFO")


def log_error(e: Exception = None, msg: str = ""):
    """记录错误/异常，包含完整堆栈"""
    full_msg = f"{msg}: {e}" if msg else str(e)
    if _logger:
        _logger.error(full_msg, exception=e)
    else:
        _write_compat(full_msg, "ERROR")
        if e:
            stack = traceback.format_exc()
            if stack and stack.strip() != "NoneType: None\n":
                _write_compat(f"堆栈:\n{stack}", "ERROR")


def log_warn(msg: str):
    """记录警告"""
    if _logger:
        _logger.warn(msg)
    else:
        _write_compat(msg, "WARN")


def log_perf(msg: str, duration: float = None):
    """记录性能数据"""
    dur_str = f" 耗时={duration:.1f}s" if duration else ""
    full_msg = f"{msg}{dur_str}"
    if _logger:
        _logger.performance(msg, duration or 0.0)
    else:
        _write_compat(full_msg, "PERF")


def log_data(source: str, count: int, success: bool = True):
    """记录数据获取结果"""
    status = "OK" if success else "FAIL"
    full_msg = f"{source}: {count}条 [{status}]"
    if _logger:
        level = "INFO" if success else "WARN"
        _logger._write_log("data_source", level, full_msg,
                           data={"source": source, "count": count, "status": status})
    else:
        _write_compat(full_msg, "DATA")


def get_log_summary(module_name: str = None) -> str:
    """获取今日错误汇总（向后兼容）"""
    today = datetime.now().strftime("%Y-%m-%d")
    errors = []
    for f in LOG_DIR.glob(f"*_{today}.log"):
        if module_name and module_name not in f.name:
            continue
        try:
            for line in f.read_text(encoding="utf-8", errors="replace").splitlines():
                if "[ERROR]" in line or "[WARN]" in line:
                    errors.append(line.strip())
        except Exception as e:
            log(f"操作异常: {e}", "WARN")
    return "\n".join(errors[-50:])


def catch_errors(func):
    """装饰器：自动捕获异常并记录"""
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            log_error(e, f"{func.__name__} 执行失败")
            raise
    return wrapper
