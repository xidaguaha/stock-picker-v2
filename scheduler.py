#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
选股调度器 v2.0 — 常驻进程，按正确流程调度
============================================
流程：
  盘后回测迭代(T+1日 15:08) → 验证排名/模拟/AI + 迭代
  盘后AI分析(T日 16:58+) → 尽早开始，凌晨完成即可
  盘前数据检查(T+1日 9:18) → 验证数据源连通
  竞价量化推送(T+1日 9:21) → 量化打分(含昨日AI因子) + 推送Top3
  盘中数据补充(T+1日 10:28/13:58) → 抓取盘中才有数据(资金流/龙虎榜)
  盘中模拟交易(T+1日 9:30-15:00) → 按排名模拟买入
"""

import subprocess
import sys
import time
import json
import os
import signal
from datetime import datetime, timedelta
from pathlib import Path

BASE_DIR = Path(__file__).parent

# ===== Ctrl+C 退出标志 =====
_shutdown = False

# ===== 进程锁：基于PID文件，确保只有一个调度器实例 =====
LOCK_FILE = BASE_DIR / "logs" / "scheduler.pid"

def _acquire_lock() -> bool:
    """尝试获取进程锁。返回 True 表示获取成功，False 表示已有实例运行"""
    try:
        LOCK_FILE.parent.mkdir(parents=True, exist_ok=True)
        # 读取已有 PID
        if LOCK_FILE.exists():
            old_pid = int(LOCK_FILE.read_text().strip())
            # 检查该进程是否还活着
            try:
                import signal
                os.kill(old_pid, 0)  # 只检查，不发送信号
                # 进程还在，无法获取锁
                return False
            except (ProcessLookupError, OSError):
                # 进程已死，删除旧锁文件
                LOCK_FILE.unlink(missing_ok=True)
        # 写入当前 PID
        LOCK_FILE.write_text(str(os.getpid()))
        return True
    except Exception:
        return True  # 出错时不阻塞

if not _acquire_lock():
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [ABORT] Scheduler already running (PID in {LOCK_FILE})")
    sys.exit(0)
PYTHON = sys.executable

# 导入共用模块的交易日历（含中国节假日+调休）
_COMMON = BASE_DIR / "共用模块"
sys.path.insert(0, str(_COMMON))
from trading_calendar import is_trading_day as _is_td, next_trading_day as _next_td

# 每个任务的超时时间（秒）— 盘前预热是可选加速任务，30分钟足够
SCRIPT_TIMEOUTS = {
    "盘后回测迭代": 1800,
    "盘后AI分析": 3600,
    "盘前数据预热": 1800,   # 30分钟，超时直接跳过
    "盘前数据检查": 600,    # 10分钟
    "竞价量化推送": 1800,   # 30分钟
    "盘中数据补充_上午": 600,
    "盘中数据补充_下午": 600,
    "盘中模拟交易": 600,
}

# 不重试的任务（超时后直接跳过，不阻塞后续）
NO_RETRY = {"盘前数据预热", "盘前数据检查"}

SCRIPTS = {
    "盘后回测迭代": BASE_DIR / "盘后回测迭代" / "run.py",
    "盘后AI分析": BASE_DIR / "盘后AI分析" / "run.py",
    "盘前数据预热": BASE_DIR / "盘前预热.py",
    "盘前数据检查": BASE_DIR / "盘前数据检查" / "run.py",
    "竞价量化推送": BASE_DIR / "竞价量化推送" / "run.py",
    "盘中数据补充_上午": BASE_DIR / "盘中数据补充" / "run.py",
    "盘中数据补充_下午": BASE_DIR / "盘中数据补充" / "run.py",
    "盘中模拟交易": BASE_DIR / "盘中模拟交易" / "run.py",
}

LOG_DIR = BASE_DIR / "logs"


def _already_done_today(phase: str) -> bool:
    """检查某个时段今天是否已执行过，避免重启后重复运行"""
    today = datetime.now().strftime("%Y-%m-%d")
    today_compact = datetime.now().strftime("%Y%m%d")

    if phase == "盘后AI分析":
        ai_file = BASE_DIR / "agent_data" / "latest_ai_analysis.json"
        if ai_file.exists():
            try:
                data = json.loads(ai_file.read_text(encoding="utf-8"))
                return data.get("date", "") == today
            except Exception as e:
                log(f"操作异常: {e}", "WARN")
        return False

    elif phase == "竞价量化推送":
        history_dir = BASE_DIR / "history"
        return any(history_dir.rglob(f"snapshot_{today_compact}_*.csv")) if history_dir.exists() else False

    elif phase == "盘后回测迭代":
        reports_dir = BASE_DIR / "reports"
        return any(reports_dir.glob(f"回测报告_{today_compact}_*.json")) if reports_dir.exists() else False

    elif phase == "盘中模拟交易":
        history_dir = BASE_DIR / "history"
        return any(history_dir.rglob(f"snapshot_{today_compact}_*.csv")) if history_dir.exists() else False

    elif phase == "盘前数据预热":
        preheat_status = BASE_DIR / "cache" / "preheat_status.json"
        if preheat_status.exists():
            try:
                data = json.loads(preheat_status.read_text(encoding="utf-8"))
                return data.get("date", "") == today and data.get("completed", False)
            except Exception:
                pass
        return False

    elif phase == "盘前数据检查":
        log_file = LOG_DIR / "bug" / f"盘前数据检查_{today}.log"
        return log_file.exists()

    elif phase.startswith("盘中数据补充"):
        intraday = BASE_DIR / "agent_data" / "intraday_data.json"
        if intraday.exists():
            try:
                data = json.loads(intraday.read_text(encoding="utf-8"))
                ts = data.get("timestamp", "")
                if not ts.startswith(today):
                    return False
                # 区分上午/下午
                hour = datetime.now().hour
                ts_hour = int(ts.split()[1].split(":")[0]) if " " in ts else 0
                if hour < 12:
                    return 10 <= ts_hour < 12
                else:
                    return ts_hour >= 14
            except Exception as e:
                log(f"操作异常: {e}", "WARN")
        return False

    return False
LOG_DIR.mkdir(parents=True, exist_ok=True)


def log(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    try:
        with open(LOG_DIR / "scheduler.log", "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass  # 写日志失败不应影响主流程


# ===== Ctrl+C 信号处理（放在 log 定义之后） =====
def _signal_handler(signum, frame):
    """SIGINT(Ctrl+C) / SIGTERM 处理器：设置退出标志，让主循环优雅退出"""
    global _shutdown
    _shutdown = True
    log("[Ctrl+C] 收到退出信号，正在停止...")

signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)


def is_trading_day(dt=None) -> bool:
    """使用共用模块的交易日历（含中国节假日+调休），回退到周末检查"""
    dt = dt or datetime.now()
    try:
        return _is_td(dt)
    except Exception:
        return dt.weekday() < 5


def _next_trading_datetime(from_dt: datetime, hour: int, minute: int) -> datetime:
    """找到从 from_dt 开始的下一个交易日的指定时分"""
    d = from_dt.date()
    for _ in range(30):  # 最多查找30天
        d = _next_td(d) if hasattr(_next_td, '__call__') else (d + timedelta(days=1))
        try:
            if _is_td(d):
                return datetime(d.year, d.month, d.day, hour, minute, 0)
        except Exception:
            if d.weekday() < 5:
                return datetime(d.year, d.month, d.day, hour, minute, 0)
    # 回退：7天后
    target = (from_dt + timedelta(days=7)).replace(hour=hour, minute=minute, second=0, microsecond=0)
    return target


def get_current_phase(dt=None) -> str | None:
    dt = dt or datetime.now()
    if not is_trading_day(dt):
        return None

    t = dt.time()
    h, m = t.hour, t.minute

    # 盘后回测迭代: 15:08-15:28（等收盘数据稳定，提前2分钟）
    if h == 15 and 8 <= m < 28:
        return "盘后回测迭代"

    # 盘后AI分析: 16:58+（尽早开始，凌晨完成即可，不影响盘前）
    if h >= 17 or (h == 16 and m >= 58):
        return "盘后AI分析"

    # 盘前数据预热: 8:00-8:10（提前缓存非实时数据，缩短竞价阶段耗时）
    if h == 8 and 0 <= m < 10:
        return "盘前数据预热"

    # 盘前数据检查: 9:18-9:20（竞价前检查，提前2分钟）
    if h == 9 and 18 <= m < 21:
        return "盘前数据检查"

    # 竞价量化推送: 9:21-9:27（竞价数据仅此时段可用）
    if h == 9 and 21 <= m < 28:
        return "竞价量化推送"

    # 盘中数据补充: 10:28 和 13:58（资金流/龙虎榜等盘中数据）
    if (h == 10 and m >= 28) or (h == 14 and m < 8):
        suffix = "上午" if h == 10 else "下午"
        return f"盘中数据补充_{suffix}"

    # 盘中模拟交易: 9:30-15:00（其余盘中时段）
    if (h == 9 and m >= 30) or (10 <= h <= 14) or (h == 15 and m == 0):
        return "盘中模拟交易"

    return None


def _clear_pycache(*dirs):
    """清理指定目录下的 __pycache__，防止 .pyc 陈旧缓存导致旧代码运行"""
    import shutil
    for d in dirs:
        if not d or not Path(d).exists():
            continue
        for pyc in Path(d).rglob("__pycache__"):
            try:
                shutil.rmtree(str(pyc))
            except Exception:
                pass


def run_subprocess(name: str, script: Path) -> bool:
    if not script.exists():
        log(f"  [ERROR] Script not found: {script}")
        return False

    # 清理 __pycache__：防止 .pyc 陈旧缓存导致旧代码运行（2026-07-14 推送事故根因）
    _common_dir = BASE_DIR / "共用模块"
    _clear_pycache(script.parent, _common_dir, BASE_DIR)

    timeout = SCRIPT_TIMEOUTS.get(name, 3600)
    log(f"  [START] {name} (超时{timeout//60}分钟)")
    try:
        # 用 Popen + 轮询，Ctrl+C 时能终止子进程并退出
        proc = subprocess.Popen(
            [PYTHON, str(script)],
            cwd=str(script.parent),
        )
        elapsed = 0
        while elapsed < timeout:
            ret = proc.poll()
            if ret is not None:
                break
            if _shutdown:
                log(f"  [STOP] {name} 收到Ctrl+C，终止子进程...")
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except:
                    proc.kill()
                return False
            time.sleep(1)
            elapsed += 1
        else:
            proc.kill()
            log(f"  [TIMEOUT] {name} 超时({timeout//60}分钟)")
            return False

        ret = proc.returncode
        if ret == 0:
            log(f"  [DONE] {name}")
        else:
            log(f"  [FAIL] {name} exit code={ret}")
        return ret == 0
    except Exception as e:
        log(f"  [ERROR] {name}: {e}")
        return False


def calc_sleep_seconds() -> int:
    now = datetime.now()

    if not is_trading_day(now):
        # 使用 trading_calendar 找下一个交易日，跳过中国节假日
        target = _next_trading_datetime(now, 8, 0)
        return max(60, int((target - now).total_seconds()))

    t = now.time()
    h, m = t.hour, t.minute

    # 已过盘后AI分析 → 等明天8:00盘前预热（跳过中国节假日）
    if h >= 17 and m >= 5:
        target = _next_trading_datetime(now, 8, 0)
        return max(60, int((target - now).total_seconds()))

    # 盘后回测(15:08-15:28) → 2分钟后
    if h == 15 and 8 <= m < 28:
        return 120

    # 等AI分析开始(15:28-16:57) → 算到16:58
    if (h == 15 and m >= 28) or (h == 16 and m < 58):
        target_ai = now.replace(hour=16, minute=58, second=0, microsecond=0)
        return max(60, int((target_ai - now).total_seconds()))

    # 盘后AI分析(16:58+) → 10分钟检查（长时间运行）
    if h >= 17 or (h == 16 and m >= 58):
        return 600

    # 盘前预热(8:00-8:10) → 1分钟后
    if h == 8 and 0 <= m < 10:
        return 60

    # 预热后到盘前检查前(8:10-9:17) → 休眠到9:18
    if h == 8 or (h == 9 and m < 18):
        target = now.replace(hour=9, minute=18, second=0, microsecond=0)
        return max(60, int((target - now).total_seconds()))

    # 竞价(9:21-9:27) → 1分钟后
    if h == 9 and 21 <= m < 28:
        return 60

    # 盘前(9:18-9:20) → 1分钟后
    if h == 9 and 18 <= m < 21:
        return 60

    # 盘中(9:30-15:00) → 5分钟
    if (h == 9 and m >= 30) or (10 <= h <= 14) or (h == 15 and m == 0):
        return 300

    # 其他 → 1分钟
    return 60


def main():
    log("=" * 60)
    log("  选股调度器 v2.0 启动")
    log("  流程: 盘后回测(15:08) → 盘后AI(16:58+) → 盘前预热(8:00) → 盘前检查(9:18) → 竞价推送(9:21) → 盘中补充/模拟")
    log("  Ctrl+C 退出")
    log("=" * 60)

    last_phase = None
    failed_tasks = {}  # {phase: retry_count}
    HEARTBEAT = 600  # 10分钟心跳

    while True:
        try:
            now = datetime.now()
            phase = get_current_phase(now)

            if phase and phase != last_phase:
                # 检查是否今天已执行过
                if _already_done_today(phase):
                    log(f"[跳过] {phase} 今天已执行过")
                    last_phase = phase
                else:
                    log(f"[切换] {phase}")
                    script = SCRIPTS.get(phase)
                    if script:
                        success = run_subprocess(phase, script)
                        # run_subprocess 可能执行很久，重新获取当前时间/phase
                        now = datetime.now()
                        current_phase = get_current_phase(now)
                        if not success:
                            if phase in NO_RETRY:
                                log(f"[跳过重试] {phase} 超时，不重试(不阻塞后续任务)")
                            elif current_phase != phase:
                                log(f"[跳过重试] {phase} 时段已过(当前: {current_phase or '空闲'})")
                            else:
                                failed_tasks[phase] = failed_tasks.get(phase, 0) + 1
                                log(f"[失败] {phase} 第{failed_tasks[phase]}次失败，将在同时段重试")
                        else:
                            if phase in failed_tasks:
                                log(f"[恢复] {phase} 重试成功")
                                del failed_tasks[phase]
                    last_phase = phase

            elif phase is None:
                last_phase = None

            # 重试失败的任务（同时段内最多重试2次，间隔5分钟）
            # 重试前重新检查当前 phase，避免阻塞后续任务
            now = datetime.now()
            current_phase = get_current_phase(now)
            if current_phase and current_phase in failed_tasks and failed_tasks[current_phase] <= 2:
                if current_phase in NO_RETRY:
                    log(f"[跳过重试] {current_phase} 属于不重试任务")
                    del failed_tasks[current_phase]
                    sleep_sec = calc_sleep_seconds()
                elif _already_done_today(current_phase):
                    del failed_tasks[current_phase]
                    sleep_sec = calc_sleep_seconds()
                else:
                    log(f"[重试] {current_phase} 第{failed_tasks[current_phase]+1}次尝试...")
                    script = SCRIPTS.get(current_phase)
                    if script:
                        success = run_subprocess(current_phase, script)
                        if success:
                            log(f"[恢复] {current_phase} 重试成功")
                            del failed_tasks[current_phase]
                        else:
                            failed_tasks[current_phase] += 1
                            if failed_tasks[current_phase] > 2:
                                log(f"[放弃] {current_phase} 重试3次仍失败，等待次日")
                    sleep_sec = 300  # 5分钟后重试
            else:
                sleep_sec = calc_sleep_seconds()

            # 心跳等待：最多等 HEARTBEAT 秒就重新检查 phase
            # 这样即使系统休眠后恢复，也不会错过整个任务时段
            actual_sleep = min(sleep_sec, HEARTBEAT)

            if sleep_sec > HEARTBEAT:
                next_check = datetime.now() + timedelta(seconds=actual_sleep)
                log(f"[休眠] 下次检查 {next_check.strftime('%H:%M')} (每{HEARTBEAT//60}分钟心跳, 目标任务还需{sleep_sec//60}分钟)")
            elif sleep_sec > 60:
                log(f"[等待] {sleep_sec}秒后检查...")
            else:
                log(f"[等待] {sleep_sec}秒...")

            # 可中断的 sleep：每秒检查 _shutdown 标志，Ctrl+C 立即退出
            _slept = 0
            while _slept < actual_sleep and not _shutdown:
                time.sleep(1)
                _slept += 1

            if _shutdown:
                break

        except KeyboardInterrupt:
            log("[退出] 调度器已停止 (KeyboardInterrupt)")
            break
        except Exception as e:
            log(f"[异常] {e}")
            if _shutdown:
                break
            time.sleep(60)

    # 清理 pid 文件
    try:
        LOCK_FILE.unlink(missing_ok=True)
    except:
        pass
    log("[退出] 调度器已停止")


def safe_sleep(total_seconds: int):
    """安全睡眠：每10分钟心跳一次，防止系统休眠/时间跳变导致错过任务"""
    HEARTBEAT = 600  # 10分钟
    deadline = time.time() + total_seconds
    last_heartbeat = time.time()
    while True:
        remaining = deadline - time.time()
        if remaining <= 0:
            break
        chunk = min(remaining, HEARTBEAT)
        time.sleep(chunk)
        # 每10分钟打印心跳
        if time.time() - last_heartbeat >= HEARTBEAT:
            now = datetime.now()
            remaining_min = int((deadline - time.time()) / 60)
            log(f"[心跳] {now.strftime('%Y-%m-%d %H:%M:%S')} 剩余 {remaining_min} 分钟")
            last_heartbeat = time.time()


if __name__ == "__main__":
    main()
