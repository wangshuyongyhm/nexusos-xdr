#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
task_state.py — nexusos-xdr 单任务状态管理

状态机: PENDING → RUNNING → DONE | FAILED | TIMEOUT | CANCELLED
落盘:   $NEXUS_HOME/xdr/tasks/<task_id>.json（1960 迁自 ~/.hermes/xdr-tasks）

复用 nexus_mem_utils.atomic_write_json / safe_read_json 保证原子写。

作者: ZCode Agent
日期: 2026-07-13
"""

import sys
import os
import re
import time
import fcntl
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, List, Optional

# 复用 nexus_mem_utils（在 自动化脚本/ 目录）
_SCRIPT_DIR = Path(__file__).resolve().parent
_AUTO_DIR = _SCRIPT_DIR.parent  # 自动化脚本/
# Task 282 P2-3: sys.path 初始化改用 nexus.paths_setup 单源（原 2 行 hack 删）
from nexusos_xdr import _vault_env as _ve  # Task668: 包内自足（干净环境 no-op）
_ve.setup_sys_path()

from nexusos_xdr._mem_compat import MEM_MISSING_MSG  # Task1597: mem=[mem] extra 容错
try:
    from nexusos_mem.nexus_mem_utils import atomic_write_json, safe_read_json  # Task668 ②: 包路径（原平铺名 vault 级外泄）
except ImportError:
    # mem 缺席（干净环境单装 xdr）：import 不炸，读写任务台账时才报安装指引
    def atomic_write_json(*_a, **_k):
        raise RuntimeError(MEM_MISSING_MSG)

    def safe_read_json(*_a, **_k):
        raise RuntimeError(MEM_MISSING_MSG)

# === 常量 ===
# 1960：默认值迁 .nexus/xdr/tasks（design §2.1 行2，经 nexusos_common 锚点）
STATE_DIR = Path(os.environ.get("NEXUS_XDR_TASK_DIR") or _ve.subpath("xdr/tasks"))

# 合法状态（状态机终态用集合做快速检查）
TERMINAL_STATES = {"DONE", "FAILED", "TIMEOUT", "CANCELLED"}
# S3 (Task315): 终态原因类型化——纯增量字段，status 枚举一律不动
# （哨兵 should_fire.py 硬编码 status ∈ TERMINAL_STATES，动枚举=全线炸）。
# completed=正常完成 / error=小弟自报失败 / aborted=用户取消 /
# timeout=墙钟超时 / cap_exceeded=step/cost cap 强杀 /
# interrupted=执行体死亡合成（Harness 式：下一个活着的读者合成） /
# unknown=历史数据或未传参 /
# done_after_timeout=Task576 例③: TIMEOUT 终态后真 done 迟到合并
# （终态不迁移，只升级原因+done_late 标志，见 merge_late_result）
END_REASONS = {
    "completed", "error", "aborted", "timeout",
    "cap_exceeded", "interrupted", "unknown", "done_after_timeout",
}
# Task594: 终态默认映射（update_status 漏传 reason 时的兜底，对齐主流传参统计）
_DEFAULT_END_REASONS = {
    "DONE": "completed",
    "FAILED": "error",
    "TIMEOUT": "timeout",
    "CANCELLED": "aborted",
}
# P1-5 修正: PENDING 也允许直接 FAILED/TIMEOUT（dispatchd 异常可能发生在 RUNNING 之前）
VALID_TRANSITIONS = {
    "PENDING": {"RUNNING", "CANCELLED", "FAILED", "TIMEOUT"},
    "RUNNING": {"DONE", "FAILED", "TIMEOUT", "CANCELLED"},
    # 1401-d2: TIMEOUT→DONE 单向矫正——done 回调迟到时 status 修正为 DONE。
    # Task576 的 merge_late_result 保留 result 但 status 仍 TIMEOUT → 51% 误报
    # （消费方读 status 看到 TIMEOUT 以为失败，实际已完成）。仅此一个方向：
    # DONE 不可回退；FAILED/CANCELLED 不矫正（语义明确，迟到 done 走 merge）。
    "TIMEOUT": {"DONE"},
}

# P2-4: task_id 格式校验（防路径遍历）
_TASK_ID_RE = re.compile(r"^[a-zA-Z0-9_\-]+$")

# 任务默认超时（秒）
DEFAULT_TIMEOUT = 3600  # 60 分钟（T674：1800 误伤 6/25 单，实测任务时长 40-90min 区间）


def now_iso() -> str:
    """UTC ISO 时间戳。"""
    import datetime
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def _validate_task_id(task_id: str) -> None:
    """校验 task_id 格式（防路径遍历）。P2-4。"""
    if not _TASK_ID_RE.match(task_id):
        raise ValueError(f"非法 task_id（只允许字母数字下划线连字符）: {task_id}")


def task_path(task_id: str) -> Path:
    """任务状态文件路径。"""
    _validate_task_id(task_id)
    return STATE_DIR / f"{task_id}.json"


def ensure_dir() -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)


# task 275 P0-2: 跨进程/跨线程排他锁，解决 update_status/append_callback/
# append_warning/save_checkpoint 的 RMW（读-改-写）三方竞争。
# 用 fcntl.flock 锁一个 sidecar `<task_id>.lock` 文件（不污染 state JSON）。
@contextmanager
def _state_lock(task_id: str):
    """排他锁同一任务的状态文件。"""
    _validate_task_id(task_id)
    ensure_dir()
    lock_path = STATE_DIR / f"{task_id}.lock"
    f = open(lock_path, "a+")
    try:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(f.fileno(), fcntl.LOCK_UN)
        f.close()


def create_task(
    task_id: str,
    caller: str,
    callee: str,
    task_path_file: str,
    timeout: int = DEFAULT_TIMEOUT,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """创建新任务状态（PENDING）。

    Args:
        task_id: 唯一任务 ID
        caller: 调用方 Agent 名（审批归属）
        callee: 目标 Agent 名
        task_path_file: 任务书文件绝对路径
        timeout: 超时秒数
        extra: 额外元数据（写到 state 里）
    Returns:
        初始化的 state dict
    """
    ensure_dir()
    state: Dict[str, Any] = {
        "task_id": task_id,
        "end_reason": None,       # S3 (Task315): 终态原因（非终态为 None，终态写入）
        "caller": caller,
        "callee": callee,
        "task_path": task_path_file,
        "status": "PENDING",
        "created": now_iso(),
        "started": None,
        "finished": None,
        "duration_sec": None,
        "timeout_sec": timeout,
        "result": None,          # done 后填: {summary, files, output_path}
        "error": None,           # failed/timeout 填错误描述
        "checkpoint": None,      # cancel 前的 tmux 快照
        "warnings": [],          # guards 警告等
        "callbacks": [],         # 收到的 /report 回调记录
        "notify_url": "",        # 缺陷5: webhook 回调（服务重启恢复用）
        # Task 297 P0-1/P0-2: 执行 loop 安全控制统计列
        # （JSON 文件存储——"加列"= 顶层字段 + 老文件 .get() fallback）
        "step_count": 0,         # patrol 累计的工具调用差量计数
        "cost_so_far": 0.0,      # patrol 累计的成本（USD，fail-open 解析）
        "credential_id": None,   # Task392 P0: dispatch 签发的凭据 ID（callback 校验用）
        "meta": extra or {},
    }
    save_state(state)
    return state


def save_state(state: Dict[str, Any]) -> None:
    """原子写状态文件。"""
    tid = state.get("task_id")
    if not tid:
        raise ValueError("state 缺少 task_id")
    atomic_write_json(task_path(tid), state)


def load_state(task_id: str) -> Optional[Dict[str, Any]]:
    """读取状态文件，不存在返回 None。"""
    return safe_read_json(task_path(task_id), default=None, log_warnings=False)


def record_poll(task_id: str) -> None:
    """Task514: 记录主控轮询感知（poll_count + last_polled_at 持久化到单一真源）。

    GET /task/{id} 与 MCP check_task_status 查询成功时调本函数。
    把"轮询感知 done"从易失 access log 变成 task_state 持久证据。
    复用 _state_lock 防与 patrol/callback 并发写冲突。

    Args:
        task_id: 任务 ID（不存在静默返回 None——查不存在的 task 是常态）
    """
    try:
        with _state_lock(task_id):
            state = load_state(task_id)
            if state is None:
                return None
            state["poll_count"] = state.get("poll_count", 0) + 1
            state["last_polled_at"] = now_iso()
            save_state(state)
    except Exception:
        # 计数是附属动作，失败不应影响查询主业——静默吞
        pass


def get_loop_counters(task_id: str) -> Dict[str, Any]:
    """Task 297 P0-1/P0-2: 读取执行 loop 安全控制统计字段。

    向后兼容：老 state 文件（Task 297 之前创建）没有 step_count/cost_so_far 顶层字段，
    这里统一用 .get(default) 兜底，调用方无需判空。

    Returns:
        {"step_count": int, "cost_so_far": float, "step_count_source": str}
        step_count_source ∈ {"state"(顶层) / "meta"(老 fallback) / "default"}
    """
    state = load_state(task_id)
    if not state:
        return {"step_count": 0, "cost_so_far": 0.0, "step_count_source": "default"}
    if "step_count" in state:
        return {
            "step_count": int(state.get("step_count") or 0),
            "cost_so_far": float(state.get("cost_so_far") or 0.0),
            "step_count_source": "state",
        }
    # Task 295 patrol 把 step_count/cost_so_far 写到 meta 里——兼容这个路径
    meta = state.get("meta") or {}
    return {
        "step_count": int(meta.get("step_count") or 0),
        "cost_so_far": float(meta.get("cost_so_far") or 0.0),
        "step_count_source": "meta" if "step_count" in meta else "default",
    }


def migrate_legacy_state_files(dry_run: bool = False) -> Dict[str, int]:
    """Task 297 P0-1/P0-2: migration 脚本——给老 state 文件补 step_count/cost_so_far 字段。

    本体系 task_state 是 **JSON 文件存储**（非 PG 表），所以"加列 migration"=
    遍历 $NEXUS_HOME/xdr/tasks/*.json，对缺字段的文件写入默认值。

    Args:
        dry_run: True=只报告不修改
    Returns:
        {"scanned": N, "migrated": M, "skipped": K}
    """
    stats = {"scanned": 0, "migrated": 0, "skipped": 0}
    if not STATE_DIR.exists():
        return stats
    import json as _json
    for p in STATE_DIR.glob("*.json"):
        stats["scanned"] += 1
        try:
            raw = p.read_text(encoding="utf-8")
            data = _json.loads(raw)
        except Exception:
            stats["skipped"] += 1
            continue
        if "step_count" in data and "cost_so_far" in data:
            stats["skipped"] += 1
            continue
        data["step_count"] = int(data.get("step_count") or 0)
        data["cost_so_far"] = float(data.get("cost_so_far") or 0.0)
        if dry_run:
            stats["migrated"] += 1
            continue
        try:
            atomic_write_json(p, data)
            stats["migrated"] += 1
        except Exception:
            stats["skipped"] += 1
    return stats


def update_status(
    task_id: str,
    new_status: str,
    result: Optional[Dict[str, Any]] = None,
    error: Optional[str] = None,
    end_reason: Optional[str] = None,
) -> Dict[str, Any]:
    """更新任务状态（带状态机校验）。

    Args:
        task_id: 任务 ID
        new_status: 新状态（PENDING/RUNNING/DONE/FAILED/TIMEOUT/CANCELLED）
        result: DONE 时填产出（summary/files/output_path）
        error: FAILED/TIMEOUT 时填错误描述
        end_reason: S3 (Task315) 终态原因（END_REASONS 之一；仅终态写入，非法值拒绝）
    Returns:
        更新后的 state
    Raises:
        ValueError: 状态文件不存在 / 非法状态迁移 / 非法 end_reason
    """
    # task 275 P0-2: 持锁做 RMW，防 dispatchd 主线程 / patrol / cancel 三方竞争
    with _state_lock(task_id):
        state = load_state(task_id)
        if state is None:
            raise ValueError(f"任务 {task_id} 状态文件不存在")

        old = state["status"]

        # 终态不可迁移——1401-d2 唯一例外：TIMEOUT→DONE 单向矫正
        # （迟到 done 到达时 status 修正为 DONE，51% 误报根治）
        if old in TERMINAL_STATES and not (old == "TIMEOUT" and new_status == "DONE"):
            raise ValueError(f"任务 {task_id} 已是终态 {old}，不能迁移到 {new_status}")

        # 校验迁移合法性
        allowed = VALID_TRANSITIONS.get(old, set())
        if new_status not in allowed:
            raise ValueError(f"非法状态迁移: {old} → {new_status} (允许: {allowed})")

        state["status"] = new_status

        # 1401-d2: TIMEOUT→DONE 矫正时强制 end_reason=done_after_timeout
        # （覆盖调用方传的 "completed"——可观测性：消费方能区分正常完成 vs 迟到矫正）
        if old == "TIMEOUT" and new_status == "DONE":
            end_reason = "done_after_timeout"

        # Task594: 终态默认映射兜底（漏宝#34）——调用方漏传 reason 不再存 None。
        # 值对齐 15 调用方主流传参（grep 统计见 Task594 report）：DONE→completed /
        # FAILED→error / TIMEOUT→timeout / CANCELLED→aborted（注意 "cancelled"
        # 不在 END_REASONS 枚举，CANCELLED 现有传参是 aborted）。
        # 显式传参优先不变；is None 判空（非 or——空串语义另行校验拦截）。
        if end_reason is None and new_status in TERMINAL_STATES:
            end_reason = _DEFAULT_END_REASONS.get(new_status)

        # S3 (Task315): 终态原因——终态时写入（未传参保留原值），非终态忽略
        if end_reason is not None:
            if end_reason not in END_REASONS:
                raise ValueError(f"非法 end_reason: {end_reason}（合法: {sorted(END_REASONS)}）")
            if new_status in TERMINAL_STATES:
                state["end_reason"] = end_reason

        # 时间戳维护
        if new_status == "RUNNING" and state.get("started") is None:
            state["started"] = now_iso()
        if new_status in TERMINAL_STATES:
            state["finished"] = now_iso()
            # 计算 duration
            if state.get("started"):
                try:
                    import datetime as _dt
                    s = _dt.datetime.fromisoformat(state["started"])
                    f = _dt.datetime.fromisoformat(state["finished"])
                    # 加固: 归一化 tz(naive 补 UTC),防 started/finished 混用致 TypeError
                    if s.tzinfo is None: s = s.replace(tzinfo=_dt.timezone.utc)
                    if f.tzinfo is None: f = f.replace(tzinfo=_dt.timezone.utc)
                    state["duration_sec"] = round((f - s).total_seconds(), 1)
                except Exception:
                    pass

        if result is not None:
            state["result"] = result
        if error is not None:
            state["error"] = error

        save_state(state)
        return state


def merge_late_result(
    task_id: str,
    result: Optional[Dict[str, Any]] = None,
    error: Optional[str] = None,
) -> Dict[str, Any]:
    """Task576 例③: 终态后迟到的真 done 合并（不走迁移，不破坏终态语义）。

    背景（Task562 实锤）: TIMEOUT 终态化后小弟真 done 到达，update_status 抛
    ValueError 被 callback 吞成 WARNING——DONE 事实只进 callbacks[]，轮询方
    （wait_callback / GET /task）永远看不到 result。

    本函数是"合并"路径（571 底稿裁决，非"迁移"）:
    - status 不变（TERMINAL_STATES 不可迁移的约束保持）
    - result 合并写入（原 None → 填充真产出）
    - end_reason: timeout → done_after_timeout（其余终态保留原值，只设标志）
    - done_late=True 顶层标志（callbacks[] 之外的感知通道）

    Args:
        task_id: 任务 ID
        result: 迟到的 done 产出（summary/files 等）
        error: 可选错误信息
    Returns:
        合并后的 state
    Raises:
        ValueError: 任务不存在 / 非终态（应走 update_status）/ 已合并过（幂等拒绝）
    """
    with _state_lock(task_id):
        state = load_state(task_id)
        if state is None:
            raise ValueError(f"任务 {task_id} 状态文件不存在")
        old = state["status"]
        if old not in TERMINAL_STATES:
            raise ValueError(
                f"任务 {task_id} 非终态 {old}，迟到合并仅适用于终态（常规更新走 update_status）"
            )
        if state.get("done_late"):
            raise ValueError(f"任务 {task_id} 已合并过迟到结果（幂等拒绝）")
        if result is not None:
            if old == "DONE" and isinstance(state.get("result"), dict):
                # T1556 B8: 重复 done（curl/客户端超时重试）非迟到——原整体覆盖
                # 在第二次 body 不全时丢第一次的 files/experiences。改字段级合并：
                # 旧值优先（第一次 done 是权威产出），新值只补空缺；files 取并集
                # 去重保序。duplicate_done 区分重复与真迟到（去 done_late 语义污染）。
                _old_r = state["result"]
                _merged_r = dict(result)
                for _k, _v in _old_r.items():
                    if _v is not None and _v != "" and _v != [] and _v != {}:
                        _merged_r[_k] = _v
                _seen_files = set()
                _merged_r["files"] = [
                    _f for _f in (list(_old_r.get("files") or []) + list(_merged_r.get("files") or []))
                    if not (_f in _seen_files or _seen_files.add(_f))
                ]
                state["result"] = _merged_r
                state["duplicate_done"] = True
            else:
                state["result"] = result
        if error is not None:
            state["error"] = error
        if state.get("end_reason") == "timeout":
            state["end_reason"] = "done_after_timeout"
        state["done_late"] = True
        state["done_late_ts"] = now_iso()
        state["done_late_from_status"] = old
        save_state(state)
        return state


def update_field(task_id: str, field: str, value: Any) -> None:
    """更新单个字段（不改 status，不走状态机校验）。

    Task 280 P1-19: 替代 dispatchd 里直接改 state dict 绕过状态机的写法。
    只用于改 notify_url / dispatched_path 等非状态字段——
    改 status 必须走 update_status（有 VALID_TRANSITIONS 校验）。
    """
    with _state_lock(task_id):
        state = load_state(task_id)
        if state is None:
            raise ValueError(f"任务不存在: {task_id}")
        state[field] = value
        save_state(state)


class CallbackLimitError(Exception):
    """T1556 B3: callbacks[] 上限锁内复核触发（治 check-then-append 并发击穿）。"""


def append_callback(
    task_id: str,
    callback: Dict[str, Any],
    max_callbacks: Optional[int] = None,
) -> None:
    """记录收到的 /report 回调。

    T1556 B3: max_callbacks 提供时在锁内原子复核（读长→判定→append 单临界区），
    封死调用方旧快照判定与 append 之间的并发击穿窗口（两并发同读 199 双过→201）。
    超限抛 CallbackLimitError，由调用方转 429。
    """
    # task 275 P0-2: 持锁 RMW
    with _state_lock(task_id):
        state = load_state(task_id)
        if state is None:
            return
        if (
            max_callbacks is not None
            and len(state.get("callbacks") or []) >= max_callbacks
        ):
            raise CallbackLimitError(
                f"callbacks 已达上限 {max_callbacks}（锁内复核，T1556 B3）"
            )
        state.setdefault("callbacks", []).append(
            {"ts": now_iso(), **callback}
        )
        save_state(state)


def append_warning(task_id: str, warning: str) -> None:
    """追加 guards 警告等。"""
    # task 275 P0-2: 持锁 RMW
    with _state_lock(task_id):
        state = load_state(task_id)
        if state is None:
            return
        state.setdefault("warnings", []).append(warning)
        save_state(state)


def once_flag(task_id: str, key: str) -> bool:
    """T657 修1: 幂等一次性标记——同 key 只返回 True 一次。

    治 648 实证的 bg-gate 重扫放大：同任务同结果重复发 gate 事件/警告
    （同秒爆发 38 条，唯一任务仅 17-18 个）。调用方首次拿到 True 执行动作，
    之后 False 跳过。结果变化用不同 key 表达（如 fail→pass 是新 key 自然重发）。
    无状态文件（合成测试/极老任务）保守放行 True——宁可重复不可丢信号。
    """
    with _state_lock(task_id):
        state = load_state(task_id)
        if state is None:
            return True
        flags = state.setdefault("once_flags", {})
        if flags.get(key):
            return False
        flags[key] = now_iso()
        save_state(state)
        return True


def save_checkpoint(task_id: str, pane_text: str) -> None:
    """cancel 前保存 tmux pane 快照（用于事后审查）。"""
    # task 275 P0-2: 持锁 RMW
    with _state_lock(task_id):
        state = load_state(task_id)
        if state is None:
            return
        state["checkpoint"] = {
            "ts": now_iso(),
            "pane_tail": pane_text[-2000:] if pane_text else "",  # 截尾 2000 字符
        }
        save_state(state)


def list_active() -> List[Dict[str, Any]]:
    """列出所有非终态任务（RUNNING/PENDING）。"""
    ensure_dir()
    active = []
    for p in STATE_DIR.glob("*.json"):
        state = safe_read_json(p, default=None, log_warnings=False)
        if state and state.get("status") not in TERMINAL_STATES:
            active.append(state)
    return active


def list_recent(limit: int = 20) -> List[Dict[str, Any]]:
    """列出最近的任务（按修改时间倒序）。"""
    ensure_dir()
    files = sorted(STATE_DIR.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    out = []
    for p in files[:limit]:
        state = safe_read_json(p, default=None, log_warnings=False)
        if state:
            out.append(state)
    return out

def list_unacked(hours: float = 24.0) -> List[Dict[str, Any]]:
    """列出窗口内 DONE 且未 ack 的任务（Task 1384 done 总结强制消费）。

    ack 语义：主控复述小弟总结 key_points 后 POST /task/{id}/ack 回写
    acked=true。窗口（默认 24h）外的历史任务自然豁免——机制上线无需回填。
    消费方：dispatch gate D9（REJECT 拦派单）+ persist gate P17（WARN 提醒）。
    """
    from datetime import datetime, timedelta, timezone  # 局部导入：仅本函数用

    ensure_dir()
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    out: List[Dict[str, Any]] = []
    for p in STATE_DIR.glob("*.json"):
        state = safe_read_json(p, default=None, log_warnings=False)
        if not state or state.get("status") != "DONE" or state.get("acked"):
            continue
        fin = str(state.get("finished") or "")
        try:
            ft = datetime.fromisoformat(fin.replace("Z", "+00:00"))
            if ft.tzinfo is None:
                ft = ft.replace(tzinfo=timezone.utc)
        except ValueError:
            continue  # finished 缺失/无效：不计入（保守不拦）
        if ft >= cutoff:
            out.append(state)
    out.sort(key=lambda s: str(s.get("finished") or ""), reverse=True)
    return out


def is_terminal(task_id: str) -> bool:
    """任务是否已终态。"""
    s = load_state(task_id)
    return s is not None and s.get("status") in TERMINAL_STATES


# === 自测 ===
def _self_test() -> None:
    print("=== task_state.py 自测 ===")
    import tempfile
    # 临时切 STATE_DIR
    global STATE_DIR
    orig = STATE_DIR
    STATE_DIR = Path(tempfile.mkdtemp())
    try:
        # 1. create
        s = create_task("test-1", "agent-a", "agent-b", "/tmp/task.md", timeout=60)
        assert s["status"] == "PENDING"
        print("  ✓ create_task → PENDING")

        # 2. PENDING → RUNNING
        s = update_status("test-1", "RUNNING")
        assert s["status"] == "RUNNING"
        assert s["started"] is not None
        print("  ✓ PENDING → RUNNING (started 记录)")

        # 3. 非法迁移（PENDING 不能直接到 DONE——DONE 必须经过 RUNNING）
        s2 = create_task("test-2", "agent-a", "agent-c", "/tmp/t.md")
        try:
            update_status("test-2", "DONE")
            print("  ✗ FAIL: 非法迁移没拦住")
            raise SystemExit(1)
        except ValueError:
            print("  ✓ 非法迁移 PENDING→DONE 被拦")

        # 3b. P1-5: PENDING → FAILED 合法（dispatchd 异常路径）
        s4 = create_task("test-4", "agent-a", "agent-d", "/tmp/t.md")
        s4 = update_status("test-4", "FAILED", error="ensure_no_popup 抛异常")
        assert s4["status"] == "FAILED"
        assert s4["error"] == "ensure_no_popup 抛异常"
        print("  ✓ PENDING → FAILED 合法（P1-5）")

        # 3c. P2-4: 非法 task_id（路径遍历）
        try:
            task_path("../../tmp/evil")
            print("  ✗ FAIL: 路径遍历没拦住")
            raise SystemExit(1)
        except ValueError:
            print("  ✓ 非法 task_id 被拦（P2-4）")

        # 4. RUNNING → DONE + duration
        s = update_status("test-1", "DONE", result={"summary": "ok"})
        assert s["status"] == "DONE"
        assert s["result"] == {"summary": "ok"}
        assert s["finished"] is not None
        assert s["duration_sec"] is not None
        print(f"  ✓ RUNNING → DONE (duration={s['duration_sec']}s)")

        # 5. 终态不可迁移
        try:
            update_status("test-1", "RUNNING")
            print("  ✗ FAIL: 终态迁移没拦住")
            raise SystemExit(1)
        except ValueError:
            print("  ✓ 终态 DONE→RUNNING 被拦")

        # 6. checkpoint + callback
        s3 = create_task("test-3", "agent-a", "agent-d", "/tmp/t.md")
        update_status("test-3", "RUNNING")
        save_checkpoint("test-3", "pane text here")
        append_callback("test-3", {"status": "progress", "msg": "step1"})
        append_warning("test-3", "[WARN] > 覆盖")
        s3 = load_state("test-3")
        assert s3["checkpoint"]["pane_tail"] == "pane text here"
        assert len(s3["callbacks"]) == 1
        assert len(s3["warnings"]) == 1
        print("  ✓ checkpoint + callback + warning")

        # 7. list_active（test-2 PENDING + test-3 RUNNING = 2 个活跃）
        active = list_active()
        assert len(active) == 2, f"期望 2 个活跃，实际 {len(active)}"
        print(f"  ✓ list_active → {len(active)} 个活跃任务")

        print("\n结果: 全部通过")
    finally:
        import shutil
        shutil.rmtree(STATE_DIR, ignore_errors=True)
        STATE_DIR = orig


if __name__ == "__main__":
    _self_test()
