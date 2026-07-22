#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
日志系统 v2.0
分层记录：运行日志 / 错误日志 / Bug 追踪 / 数据源日志
"""

import os
import sys
import json
import traceback
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any

# ===================== 日志目录结构 =====================
# logs/
#   ├── runs/           # 每次运行的完整日志（按日期+时间）
#   ├── errors/         # 所有错误和异常（按日期）
#   ├── bugs/           # Bug 追踪（按日期，包含完整堆栈）
#   ├── data_source/    # 数据源获取日志（成功/失败分开）
#   └── performance/    # 性能日志（执行时间、数据量）
# =====================================================

BASE_DIR = Path(__file__).parent.parent  # 指向项目根目录
LOGS_DIR = BASE_DIR / "logs"
RUNS_DIR = LOGS_DIR / "runs"
ERRORS_DIR = LOGS_DIR / "errors"
BUGS_DIR = LOGS_DIR / "bugs"
DATA_SOURCE_DIR = LOGS_DIR / "data_source"
PERFORMANCE_DIR = LOGS_DIR / "performance"

# 创建所有目录
for d in [LOGS_DIR, RUNS_DIR, ERRORS_DIR, BUGS_DIR, DATA_SOURCE_DIR, PERFORMANCE_DIR]:
    d.mkdir(parents=True, exist_ok=True)


class Logger:
    """结构化日志系统"""
    
    def __init__(self, run_id: Optional[str] = None):
        self.run_id = run_id or datetime.now().strftime("%Y%m%d_%H%M%S")
        self.start_time = datetime.now()
        
    def _timestamp(self) -> str:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    
    def _write_log(self, log_type: str, level: str, message: str, 
                   data: Optional[Dict] = None, exception: Optional[Exception] = None):
        """写入日志文件"""
        log_entry = {
            "timestamp": self._timestamp(),
            "run_id": self.run_id,
            "level": level,
            "message": message,
        }
        
        if data:
            log_entry["data"] = data
        
        if exception:
            log_entry["exception"] = {
                "type": type(exception).__name__,
                "message": str(exception),
                "traceback": traceback.format_exc(),
            }
        
        # 写入对应的日志文件
        if log_type == "run":
            log_file = RUNS_DIR / f"{self.run_id}.jsonl"
        elif log_type == "error":
            log_file = ERRORS_DIR / f"{datetime.now().strftime('%Y%m%d')}.jsonl"
        elif log_type == "bug":
            log_file = BUGS_DIR / f"{datetime.now().strftime('%Y%m%d')}.jsonl"
        elif log_type == "data_source":
            log_file = DATA_SOURCE_DIR / f"{datetime.now().strftime('%Y%m%d')}.jsonl"
        elif log_type == "performance":
            log_file = PERFORMANCE_DIR / f"{datetime.now().strftime('%Y%m%d')}.jsonl"
        else:
            log_file = LOGS_DIR / "unknown.jsonl"
        
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(log_entry, ensure_ascii=False) + "\n")
        
        # 同时输出到控制台（带颜色）
        self._console_output(level, message)
    
    def _console_output(self, level: str, message: str):
        """控制台带颜色输出"""
        colors = {
            "DEBUG": "\033[36m",    # 青色
            "INFO": "\033[32m",     # 绿色
            "WARN": "\033[33m",     # 黄色
            "ERROR": "\033[31m",    # 红色
            "FATAL": "\033[35m",    # 紫色
        }
        reset = "\033[0m"
        color = colors.get(level, "")
        print(f"{color}[{level}]{reset} {self._timestamp()} {message}")
    
    # ==================== 公开方法 ====================
    
    def debug(self, message: str, data: Optional[Dict] = None):
        """调试信息"""
        self._write_log("run", "DEBUG", message, data)
    
    def info(self, message: str, data: Optional[Dict] = None):
        """普通信息"""
        self._write_log("run", "INFO", message, data)
    
    def warn(self, message: str, data: Optional[Dict] = None):
        """警告"""
        self._write_log("run", "WARN", message, data)
        self._write_log("error", "WARN", message, data)
    
    def error(self, message: str, exception: Optional[Exception] = None, 
              data: Optional[Dict] = None):
        """错误（会记录到 errors/ 和 bugs/）"""
        self._write_log("run", "ERROR", message, data, exception)
        self._write_log("error", "ERROR", message, data, exception)
        
        if exception:
            # 异常类错误同时记录到 bugs/
            self._write_log("bug", "ERROR", message, data, exception)
    
    def fatal(self, message: str, exception: Optional[Exception] = None):
        """致命错误（程序终止）"""
        self._write_log("run", "FATAL", message, None, exception)
        self._write_log("error", "FATAL", message, None, exception)
        self._write_log("bug", "FATAL", message, None, exception)
    
    def data_source(self, source: str, status: str, message: str, 
                    data: Optional[Dict] = None):
        """
        数据源日志
        source: 数据源名称（如 "东方财富", "腾讯", "新浪"）
        status: "success" / "fail" / "fallback"
        """
        log_data = {"source": source, "status": status}
        if data:
            log_data.update(data)
        self._write_log("data_source", "INFO" if status == "success" else "WARN", 
                       f"[{source}] {message}", log_data)
    
    def performance(self, operation: str, duration: float, 
                    records: Optional[int] = None):
        """
        性能日志
        operation: 操作名称
        duration: 耗时（秒）
        records: 处理记录数
        """
        data = {"operation": operation, "duration_seconds": duration}
        if records:
            data["records"] = records
        self._write_log("performance", "INFO", 
                       f"{operation} 耗时 {duration:.2f}s" + 
                       (f" ({records} 条记录)" if records else ""), 
                       data)
    
    def summary(self):
        """运行结束，输出汇总"""
        end_time = datetime.now()
        duration = (end_time - self.start_time).total_seconds()
        
        summary_data = {
            "start_time": self.start_time.strftime("%Y-%m-%d %H:%M:%S"),
            "end_time": end_time.strftime("%Y-%m-%d %H:%M:%S"),
            "duration_seconds": duration,
        }
        
        self._write_log("run", "INFO", 
                       f"运行结束，耗时 {duration:.2f} 秒", 
                       summary_data)


# ===================== 全局日志实例 =====================

_logger_instance: Optional[Logger] = None

def get_logger(run_id: Optional[str] = None) -> Logger:
    """获取全局日志实例"""
    global _logger_instance
    if _logger_instance is None:
        _logger_instance = Logger(run_id)
    return _logger_instance


def init_logger(run_id: Optional[str] = None) -> Logger:
    """初始化新的日志实例"""
    global _logger_instance
    _logger_instance = Logger(run_id)
    return _logger_instance


# ===================== 异常捕获装饰器 =====================

def log_exceptions(logger: Optional[Logger] = None):
    """
    装饰器：自动捕获并记录异常
    用法：
        @log_exceptions()
        def my_function():
            ...
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            _logger = logger or get_logger()
            try:
                return func(*args, **kwargs)
            except Exception as e:
                _logger.error(
                    f"函数 {func.__name__} 执行失败",
                    exception=e,
                    data={"args": str(args), "kwargs": str(kwargs)}
                )
                raise  # 重新抛出异常
        return wrapper
    return decorator


# ===================== 测试 =====================

if __name__ == "__main__":
    # 测试日志系统
    log = init_logger()
    
    log.info("日志系统测试开始")
    log.debug("调试信息", {"test": "debug"})
    log.info("普通信息", {"test": "info"})
    log.warn("警告信息", {"test": "warn"})
    
    # 测试数据源日志
    log.data_source("东方财富", "success", "获取实时行情成功", {"records": 5365})
    log.data_source("腾讯", "fallback", "东方财富失败，降级到腾讯", {"reason": "timeout"})
    
    # 测试异常捕获
    try:
        raise ValueError("测试异常")
    except Exception as e:
        log.error("捕获到异常", exception=e, data={"test": "error"})
    
    # 测试性能日志
    import time
    start = time.time()
    time.sleep(0.1)
    log.performance("测试操作", time.time() - start, 100)
    
    log.summary()
    print("\n✅ 日志系统测试完成，请查看 logs/ 目录")
