#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
session_state.py — NEXUS-PRESENCE Agent 在场感知与事件流

存储（G8 双轨，Task1951；1960 迁 .nexus）:
  $NEXUS_HOME/xdr/sessions/sessions.json       — 在场表（覆盖写，当前所有活跃 Agent）
  $NEXUS_HOME/logs/xdr-events-archive/YYYY-MM-DD.jsonl — 事件流（日切归档，追加写）
  $NEXUS_HOME/xdr/sessions/events.jsonl        — legacy（启动时 _migrate_legacy_events 迁移入归档）

设计:
  - sessions.json: 覆盖写（当前快照），atomic_write_json + flock 保证安全
  - events 归档:   按日追加写（历史流），天然防并发写冲突（每次 append 一行）
  - 心跳守护线程定期清理 stale 会话（5 分钟无心跳 → 标记 offline）

复用 task_state.py 的模式:
  - import nexus.paths_setup 做 sys.path 初始化
  - atomic_write_json / safe_read_json 保证原子写
  - fcntl.flock sidecar 锁保证并发安全

作者: ZCode Agent  日期: 2026-07-22
"""

import sys
import os
import json
import fcntl
import logging
import hashlib
import threading
import re as _re_mod
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone, timedelta

# === 路径设置（同 task_state.py 模式）===
_SCRIPT_DIR = Path(__file__).resolve().parent
_AUTO_DIR = _SCRIPT_DIR.parent
from nexusos_xdr import _vault_env as _ve  # Task668: 包内自足（干净环境 no-op）
_ve.setup_sys_path()

from nexusos_xdr._mem_compat import MEM_MISSING_MSG  # Task1597: mem=[mem] extra 容错
try:
    from nexusos_mem.nexus_mem_utils import atomic_write_json, safe_read_json  # Task668 ②: 包路径（原平铺名 vault 级外泄）
except ImportError:
    # mem 缺席（干净环境单装 xdr）：import 不炸，读写在场台账时才报安装指引
    def atomic_write_json(*_a, **_k):
        raise RuntimeError(MEM_MISSING_MSG)

    def safe_read_json(*_a, **_k):
        raise RuntimeError(MEM_MISSING_MSG)

logger = logging.getLogger("nexusos-xdr.session")

# === 常量 ===
# 锚点单源 nexusos_common（1957/1960）：env > .nexus 默认；禁写死 ~/.hermes
SESSION_DIR = Path(os.environ.get("NEXUS_XDR_SESSION_DIR") or _ve.subpath("xdr/sessions"))
SESSIONS_FILE = SESSION_DIR / "sessions.json"
EVENTS_FILE = SESSION_DIR / "events.jsonl"  # legacy(已迁移到日切归档,保留常量兼容)
# Task1951 G8: events 日切归档——写入按天分文件,查询加载当日+昨日(滚动窗口,
# API 语义不变)。vault 侧不动(避免写入频率)。
# 1960: 默认值迁 .nexus/logs/xdr-events-archive（design §2.1 行8 家族）
EVENTS_ARCHIVE_DIR = Path(os.environ.get("NEXUS_XDR_EVENTS_ARCHIVE") or
                          _ve.subpath("logs/xdr-events-archive"))


def _archive_path(day: str) -> Path:
    return EVENTS_ARCHIVE_DIR / (day + ".jsonl")


def _today_str() -> str:
    return now_iso()[:10]


def archive_path_today() -> Path:
    """当日归档文件路径（mcp_server gate-rollup 等外部读者用）。"""
    return _archive_path(_today_str())


def _same_file(a: Path, b: Path) -> bool:
    """同实体判定：优先 samefile（st_dev/st_ino，可识 hardlink），
    fail 时退 realpath 字符串比对（可识 symlink 且不要求双方存在）。"""
    try:
        return os.path.samefile(a, b)
    except OSError:
        try:
            return os.path.realpath(a) == os.path.realpath(b)
        except OSError:
            return False


def _migrate_legacy_events() -> None:
    """启动迁移：旧 events.jsonl 按行 ts 日期分拣进归档，然后 rename .migrated。

    防自引用迁移环（Task1958/261028 事故）：legacy 若被重建为指向归档的
    symlink/hardlink，读端写端同实体会无限自复制（本机实测 7.8GB/3274 万行）。
    迁移前判定：legacy 解析进归档目录、或与归档现有文件同实体（samefile）→
    跳过迁移 + 告警日志。
    """
    if not EVENTS_FILE.exists():
        return
    try:
        src_real = os.path.realpath(EVENTS_FILE)
        arch_real = os.path.realpath(EVENTS_ARCHIVE_DIR)
    except OSError:
        return
    if os.path.dirname(src_real) == arch_real:
        logger.error(
            "自引用迁移环防护：legacy events.jsonl(%s) 解析进归档目录(%s)，跳过迁移",
            EVENTS_FILE, EVENTS_ARCHIVE_DIR)
        return
    try:
        EVENTS_ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
        for existing in EVENTS_ARCHIVE_DIR.glob("*.jsonl"):
            if _same_file(existing, EVENTS_FILE):
                logger.error(
                    "自引用迁移环防护：legacy events.jsonl 与归档文件同实体(%s)，跳过迁移",
                    existing)
                return
        with open(EVENTS_FILE, encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                day = None
                try:
                    day = json.loads(line).get("ts", "")[:10]
                except json.JSONDecodeError:
                    day = None
                if not day or len(day) != 10:
                    day = _today_str()
                with open(_archive_path(day), "a", encoding="utf-8") as af:
                    af.write(line + "\n")
        EVENTS_FILE.rename(EVENTS_FILE.with_suffix(".jsonl.migrated"))
    except OSError:
        return


_migrate_legacy_events()

# 心跳超时（秒）——超过这个时间没心跳的 Agent 标记为 stale
HEARTBEAT_TIMEOUT = 300  # 5 分钟
# stale 清理阈值 ——超过这个时间移除出在场景
STALE_REMOVE_THRESHOLD = 900  # 15 分钟

# === 身份校验（复用 AGENT_REGISTRY，含 external 外部 Agent）===
# 内部施工队（oc/omp/kimi/ds/ds2/ac）和外部主控（zcode/hermes/kimiapp）
# 都在 AGENT_REGISTRY 注册，external 状态的不被 dispatch 但可用于身份校验。


def validate_agent(agent: str) -> tuple:
    """校验 agent 身份是否合法（必须在 AGENT_REGISTRY 注册过）。

    Returns:
        (valid: bool, reason: str)
    """
    if not agent or not isinstance(agent, str):
        return False, "agent 名为空"
    if len(agent) > 30:
        return False, "agent 名过长（>30字符）"
    # 只允许字母数字下划线连字符
    import re
    if not re.match(r'^[a-zA-Z0-9_-]+$', agent):
        return False, "agent 名含非法字符（只允许字母数字_-）"
    # 必须在 AGENT_REGISTRY 注册（含 external）
    try:
        from nexusos_xdr.agents import AGENT_REGISTRY, ensure_launchd_registered
        ensure_launchd_registered()  # lazy init: 首次校验时触发 launchd 自动注册
        if agent not in AGENT_REGISTRY:
            known = sorted(AGENT_REGISTRY.keys())
            return False, f"未注册 agent '{agent}'（已注册: {known}）"
    except Exception:
        pass  # import 失败时降级（只做格式校验）
    return True, "ok"


def now_iso() -> str:
    """UTC ISO 时间戳。"""
    return datetime.now(timezone.utc).isoformat()


def now_local_str() -> str:
    """本地时间字符串（给人类读的）。"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def ensure_dir() -> None:
    SESSION_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================================
# sessions.json — 在场表（覆盖写）
# ============================================================================

@contextmanager
def _sessions_lock():
    """排他锁 sessions.json（sidecar .lock 文件，同 task_state 模式）。"""
    ensure_dir()
    lock_path = SESSION_DIR / "sessions.lock"
    f = open(lock_path, "a+")
    try:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(f.fileno(), fcntl.LOCK_UN)
        f.close()


def load_sessions() -> Dict[str, Any]:
    """读取在场表。返回 {agent_name: session_dict}。"""
    return safe_read_json(SESSIONS_FILE, default={}, log_warnings=False) or {}


def save_sessions(sessions: Dict[str, Any]) -> None:
    """原子写在场景。"""
    ensure_dir()
    atomic_write_json(SESSIONS_FILE, sessions)


def register_agent(
    agent: str,
    role: str = "executor",
    session_id: str = "",
    current_task: str = "",
    status: str = "init",
) -> Dict[str, Any]:
    """Agent 注册到在场景（init 时调用）。

    Args:
        agent: Agent 名（zcode/kimi/oc/omp/ds/ds2/ac/hermes/kimiapp）
        role: 角色（orchestrator=主控 / executor=执行）
        session_id: 会话 ID（如 zcode 的 sess_xxx）
        current_task: 当前任务描述
        status: 状态（init/working/idle/blocking/done）
    Returns:
        更新后的完整在场景
    Raises:
        ValueError: agent 身份校验失败
    """
    valid, reason = validate_agent(agent)
    if not valid:
        raise ValueError(reason)
    with _sessions_lock():
        sessions = load_sessions()
        sessions[agent] = {
            "agent": agent,
            "role": role,
            "session_id": session_id,
            "started": sessions.get(agent, {}).get("started", now_iso()),
            "last_heartbeat": now_iso(),
            "status": status,
            "current_task": current_task,
        }
        save_sessions(sessions)
    logger.info(f"[presence] register: {agent} role={role} task={current_task}")
    return sessions


def heartbeat_agent(
    agent: str,
    status: str = "working",
    current_task: str = "",
    context_pct: Optional[int] = None,
    session_id: str = "",
) -> Dict[str, Any]:
    """Agent 心跳更新（每 60s 调用）。

    Args:
        agent: Agent 名
        status: 当前状态
        current_task: 当前任务描述
        context_pct: context 窗口占用率（可选）
        session_id: 会话标识（Task532 P-d2: 可选——归属维度，register 外也带）
    Returns:
        更新后的完整在场景
    """
    # Task538 修复3: validate_agent（原无校验——未注册/恶意名直接 auto-register）
    valid, reason = validate_agent(agent)
    if not valid:
        raise ValueError(reason)
    with _sessions_lock():
        sessions = load_sessions()
        if agent not in sessions:
            # 未注册的心跳自动注册
            sessions[agent] = {
                "agent": agent,
                "role": "executor",
                "session_id": "",
                "started": now_iso(),
            }
        entry = sessions[agent]
        entry["last_heartbeat"] = now_iso()
        entry["status"] = status
        if current_task:
            entry["current_task"] = current_task
        if context_pct is not None:
            entry["context_pct"] = context_pct
        save_sessions(sessions)
    return sessions


def unregister_agent(agent: str) -> Dict[str, Any]:
    """Agent 退出在场景（done/收工时调用）。"""
    with _sessions_lock():
        sessions = load_sessions()
        if agent in sessions:
            del sessions[agent]
            save_sessions(sessions)
            logger.info(f"[presence] unregister: {agent}")
    return sessions


def get_active_sessions() -> Dict[str, Any]:
    """获取所有活跃 Agent（已清理 stale）。"""
    sessions = load_sessions()
    cleaned = {}
    now = datetime.now(timezone.utc)
    for name, entry in sessions.items():
        hb = entry.get("last_heartbeat", "")
        if hb:
            try:
                hb_dt = datetime.fromisoformat(hb.replace("Z", "+00:00"))
                elapsed = (now - hb_dt).total_seconds()
                if elapsed > STALE_REMOVE_THRESHOLD:
                    continue  # 超过 15 分钟，移除
                elif elapsed > HEARTBEAT_TIMEOUT:
                    entry["status"] = "stale"
            except Exception:
                pass
        cleaned[name] = entry
    return cleaned


# ============================================================================
# events.jsonl — 事件流（追加写）
# ============================================================================

def append_event(
    agent: str,
    event_type: str,
    detail: str = "",
    target: str = "",
    extra: Optional[Dict] = None,
) -> Dict[str, Any]:
    """追加一条事件到事件流。

    Args:
        agent: 来源 Agent
        event_type: 事件类型（init/op/done/block/alert/broadcast/need_coord）
        detail: 事件描述
        target: 操作目标（如文件路径，用于冲突检测）
        extra: 额外元数据
    Returns:
        写入的事件 dict
    Raises:
        ValueError: agent 身份校验失败
    """
    valid, reason = validate_agent(agent)
    if not valid:
        raise ValueError(reason)
    ensure_dir()
    event = {
        "ts": now_iso(),
        "ts_local": now_local_str(),
        "agent": agent,
        "type": event_type,
        "detail": detail,
        "target": target,
    }
    if extra:
        event.update(extra)

    # 追加写（天然防并发——每行独立 append）；Task1951 G8: 写当日归档文件
    EVENTS_ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    with open(_archive_path(_today_str()), "a", encoding="utf-8") as f:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        try:
            f.write(json.dumps(event, ensure_ascii=False) + "\n")
        finally:
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)

    return event


def get_events(since: str = "", limit: int = 100, agent_filter: str = "") -> List[Dict[str, Any]]:
    """读取事件流（增量轮询）。

    Args:
        since: ISO 时间戳，只返回此时间之后的事件（空=全部）
        limit: 最多返回条数
        agent_filter: 只看某 Agent 的事件（空=全部）
    Returns:
        事件列表（按时间正序）
    """
    # Task1951 G8: 读当日+昨日归档（滚动窗口）。旧 legacy 已在启动时分拣进归档。
    from datetime import timedelta as _td
    day_now = now_iso()[:10]
    try:
        _day_dt = datetime.fromisoformat(day_now)
        day_yesterday = (_day_dt - _td(days=1)).strftime("%Y-%m-%d")
    except ValueError:
        day_yesterday = day_now
    read_files = [p for p in (_archive_path(day_now), _archive_path(day_yesterday))
                  if p.exists()]
    if not read_files:
        return []

    events = []
    for _rf in read_files:
        with open(_rf, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    evt = json.loads(line)
                except json.JSONDecodeError:
                    continue
                # 时间过滤
                if since:
                    # Task1568 (1564 池建议③): since 解析失败显式报错（strict）——
                    # 原 except:pass 静默全量放行，过滤失效只靠调用方兜底（1564 urlencode
                    # 踩坑同族防御深化）。坏 since 是调用方 bug，静默=假象增量。
                    try:
                        since_dt = datetime.fromisoformat(since.replace("Z", "+00:00"))
                    except (ValueError, AttributeError) as e:
                        raise ValueError(
                            f"since 解析失败(期望 ISO 8601): {since[:60]!r} — {e}")
                    # 单条事件 ts 坏行跳过（保持旧语义——事件流逐行容错）
                    try:
                        evt_dt = datetime.fromisoformat(evt["ts"].replace("Z", "+00:00"))
                        if evt_dt <= since_dt:
                            continue
                    except Exception:
                        pass
                # Agent 过滤
                if agent_filter and evt.get("agent") != agent_filter:
                    continue
                events.append(evt)

    # Task1951 G8: 两日文件拼接后需全局 ts 正序（原单文件天然有序，拼接破坏）
    events.sort(key=lambda e: str(e.get("ts", "")))

    # 限制条数（取最后 N 条）
    if len(events) > limit:
        events = events[-limit:]
    return events


# ============================================================================
# Task593: invariant 配对断言（漏宝#33 裁剪版——旁路只读，fail-open）
# ============================================================================

# 配对语义表（单一真源：assert_ledger_pairs 与 monitor check_I14 共用，
# 经 PAIR_SEMANTICS_JSON 路径给 I14 shell 侧 import，别两处定义）。
# 每条: begin_type → {"end_type": ..., "key": 配对键提取器, "window_s": 合理在途窗}
# - key 从事件 dict 提取配对键（handler_id / agent 名）
# - window_s: begin 后超过此窗仍无 end = 「有始无终」孤儿告警
#   （在途任务数小时正常——窗放宽；hook 执行 15s 超时——窗收紧）
PAIR_SEMANTICS = [
    # broadcast_router 台账（broadcast_router.py:141 handler_id 天然唯一）
    {"begin": "hook_invoked", "end": "hook_result", "key": "handler_id", "window_s": 60},
    # presence 生命周期（register → unregister；900s 清理是内部行为不写 unregister，窗必须 > 900s×N）
    {"begin": "init", "end": "cleanup", "key": "agent", "window_s": 7 * 86400},
]
PAIR_SEMANTICS_JSON = str(Path(__file__).parent / "pair_semantics.json")  # I14 侧单源导入路径


def _pair_semantics_export() -> None:
    """把 PAIR_SEMANTICS 落成 JSON（I14 shell 侧消费的单一真源物化）。

    模块加载时执行一次（幂等覆盖）；失败静默（I14 有 python -c 内联 fallback）。
    """
    try:
        Path(PAIR_SEMANTICS_JSON).write_text(
            json.dumps(PAIR_SEMANTICS, ensure_ascii=False, indent=1), encoding="utf-8")
    except Exception:
        pass


_pair_semantics_export()


def assert_ledger_pairs(events: List[Dict[str, Any]], now_iso: str = "") -> List[Dict[str, Any]]:
    """Task593: 按配对语义扫描事件列表，返回断裂清单（纯函数，不写不读文件）。

    断裂两类：
      orphan    有始无终——begin 后超 window_s 仍无同 key 的 end
      dangling  有终无始——end 出现但之前无同 key 的 begin

    Args:
        events: 事件列表（ts 正序；字段同 events.jsonl 行）
        now_iso: 评估"现在"（默认墙钟——孤儿年龄按真实时间老化，事件流停写不影响判定；
                 单测注入固定值保证确定性）
    Returns:
        [{"kind": "orphan"|"dangling", "semantic": begin>end 类型,
          "key": 配对键, "begin_ts"/"end_ts": 相关时间, "age_s": 孤儿年龄}]
    """
    if not events:
        return []
    # 解析时间轴
    def _ts(evt):
        try:
            return datetime.fromisoformat(str(evt.get("ts", "")).replace("Z", "+00:00"))
        except Exception:
            return None

    now = None
    if now_iso:
        try:
            now = datetime.fromisoformat(now_iso.replace("Z", "+00:00"))
        except Exception:
            now = None
    if now is None:
        # Task593 修正: 默认墙钟（首版取最后事件 ts——流停写时孤儿年龄会算出负数不告警）
        now = datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)

    breaks: List[Dict[str, Any]] = []
    for sem in PAIR_SEMANTICS:
        begin_t, end_t, keyf, window = sem["begin"], sem["end"], sem["key"], sem["window_s"]
        begins: Dict[str, Dict[str, Any]] = {}   # key → {"ts": dt, "evt": evt}
        seen_end_keys: set = set()
        for evt in events:
            if evt.get("type") != begin_t:
                continue
            key = str(evt.get(keyf, ""))
            if not key:
                continue
            ts = _ts(evt)
            if ts is None:
                continue
            if key not in begins:  # 重复 begin 只记首个（重连场景）
                begins[key] = {"ts": ts, "evt": evt}
        for evt in events:
            if evt.get("type") != end_t:
                continue
            key = str(evt.get(keyf, ""))
            if not key:
                continue
            seen_end_keys.add(key)
            if key not in begins:
                ts = _ts(evt)
                breaks.append({
                    "kind": "dangling", "semantic": f"{begin_t}>{end_t}", "key": key,
                    "end_ts": evt.get("ts"), "agent": evt.get("agent"),
                })
        # 有始无终：begin 超窗仍无 end
        for key, info in begins.items():
            if key in seen_end_keys:
                continue
            age_s = (now - info["ts"]).total_seconds()
            if age_s > window:
                breaks.append({
                    "kind": "orphan", "semantic": f"{begin_t}>{end_t}", "key": key,
                    "begin_ts": info["evt"].get("ts"), "agent": info["evt"].get("agent"),
                    "age_s": round(age_s),
                })
    return breaks


# ============================================================================
# 冲突检测（op-log 声明意图）
# ============================================================================

# 操作目标最近声明记录（内存缓存，进程级）
# {target: {"agent": "zcode", "ts": "...", "op": "edit"}}
_recent_ops: Dict[str, Dict] = {}
OP_CONFLICT_WINDOW = 30  # 30 秒内同 target 两个 Agent 声明 = 冲突
# Task532 P-d5: declare_op 的 check-then-set 模块级锁（并发 declare 同 target 时
# 互相看不到对方声明的竞态修复）
_declare_op_lock = threading.Lock()


def get_recent_ops() -> Dict[str, Dict]:
    """公开读取接口（Task639 P1）——替代跨模块直访 _recent_ops 私有成员
    （T601 违例存量）。返回同对象引用（与 _active_patrols 同款同对象纪律：
    调用方只读；copy 会造双真源）。"""
    return _recent_ops

# Task541 修复2: _recent_ops 持久化——xdr 重启后 30s 冲突窗不丢
_RECENT_OPS_FILE = Path(SESSION_DIR).parent / "xdr-sessions" / "recent-ops.json"


def _load_recent_ops():
    """模块加载时从 recent-ops.json 恢复（过期条目查询时自然过滤）。"""
    global _recent_ops
    try:
        if _RECENT_OPS_FILE.exists():
            import json as _json_ro
            _recent_ops = _json_ro.loads(_RECENT_OPS_FILE.read_text(encoding="utf-8"))
    except Exception:
        pass  # 加载失败不阻塞——空 dict 等同原行为


def _save_recent_ops():
    """原子写 recent-ops.json（tmp+rename）。失败不阻塞 declare_op。"""
    try:
        import json as _json_wo
        tmp = str(_RECENT_OPS_FILE) + ".tmp"
        _RECENT_OPS_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(tmp, "w", encoding="utf-8") as f:
            _json_wo.dump(_recent_ops, f, ensure_ascii=False)
        os.replace(tmp, str(_RECENT_OPS_FILE))
    except Exception as e:
        logging.getLogger("nexusos_xdr.session_state").warning(
            f"[Task541] recent-ops.json 写入失败（不阻塞）: {e}")


# 模块加载时恢复
_load_recent_ops()


def _fingerprint_target(target: str) -> Optional[Dict[str, Any]]:
    """Task597 #54: 目标文件指纹（mtime+size+sha1 前 8 位）。

    存在时返回指纹 dict；不存在/不可读/超 10MB（sha1 跳过，只留 size+mtime）
    相应降级；完全拿不到 stat 时返回 None（不阻塞声明）。
    """
    try:
        st = os.stat(target)
    except OSError:
        return None
    fp: Dict[str, Any] = {"mtime": int(st.st_mtime), "size": st.st_size}
    if st.st_size <= 10 * 1024 * 1024:
        try:
            h = hashlib.sha1()
            with open(target, "rb") as f:
                for chunk in iter(lambda: f.read(1 << 16), b""):
                    h.update(chunk)
            fp["sha1"] = h.hexdigest()[:8]
        except OSError:
            pass  # 读失败只留 size+mtime
    return fp

# ── Task1704 件一（池#285）：target 归一化口径 ──────────────────────────────
# hook 查询（GET /session/op-log）带工具侧绝对路径；Agent 声明惯用相对路径/
# ~ 缩写/组合串（"a + b"）。原两侧纯字符串精确比对，三形态全部漏命中 →
# 误报。归一化（声明/查询同一函数）：组合串拆分 → expanduser → 相对路径
# 锚定 Vault 根（Agent 约定 vault 相对口径，B13 参数化铁律单源 _ve.VAULT_ROOT）。
_TARGET_SPLIT_RE = _re_mod.compile(r"\s*[+,，、;；]\s*")


def normalize_targets(raw: str) -> List[str]:
    """声明/查询 target → 规范绝对路径列表（组合串拆多目标，双侧同口径）。"""
    out: List[str] = []
    for part in _TARGET_SPLIT_RE.split(raw or ""):
        p = part.strip()
        if not p:
            continue
        p = os.path.expanduser(p)
        if not os.path.isabs(p):
            p = str(Path(_ve.VAULT_ROOT) / p)
        if p not in out:
            out.append(p)
    return out


def query_recent_ops(target: str = "", within: int = 120) -> List[Dict[str, Any]]:
    """按归一化口径查最近声明（Task1704 件一）。

    匹配=归一化精确命中 **或** basename 命中（兜底声明侧只写文件名的场景）。
    返回按 ts 倒序；time 解析失败的条目跳过（与原 GET 端点行为一致）。
    """
    now = datetime.now(timezone.utc)
    q_norm = normalize_targets(target) if target else []
    q_bases = {os.path.basename(p) for p in q_norm}
    ops: List[Dict[str, Any]] = []
    for tg, info in (_recent_ops or {}).items():
        if target:
            tg_norm = normalize_targets(tg)
            hit = any(p in q_norm for p in tg_norm) or os.path.basename(tg) in q_bases
            if not hit:
                continue
        ts_str = info.get("ts", "") or ""
        try:
            ts_dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        except Exception:
            continue
        if (now - ts_dt).total_seconds() > within:
            continue
        ops.append({
            "target": tg,
            "agent": info.get("agent", ""),
            "op": info.get("op", ""),
            "ts": ts_str,
            "detail": info.get("detail", ""),
        })
    ops.sort(key=lambda x: x.get("ts", ""), reverse=True)
    return ops


def declare_op(agent: str, op: str, target: str, detail: str = "", session_id: str = "") -> Dict[str, Any]:
    """声明操作意图（操作前调用，检测并发冲突）。

    多主控并存时，同一文件的并发修改靠此检测。
    Task532 P-d5: check-then-set 加模块级锁——原无锁下并发 declare 同一 target
    互相看不到对方的声明（check 时对方还没 set），冲突检测失效。
    Task532 P-d2: session_id 可选参数（事件归属会话维度，register 外也带）。

    Args:
        agent: 声明方 Agent
        op: 操作类型（edit/create/delete/dispatch）
        target: 操作目标（文件路径 / 任务 ID）
        detail: 操作描述
        session_id: 可选会话标识（写入事件流）
    Returns:
        {"declared": True, "conflict": bool, "other_agent": "...", "detail": "..."}
    Raises:
        ValueError: agent 身份校验失败
    """
    valid, reason = validate_agent(agent)
    if not valid:
        raise ValueError(reason)
    now = datetime.now(timezone.utc)
    conflict_info = {"declared": True, "conflict": False, "other_agent": "", "detail": ""}
    # Task1704 件一（池#285）：按归一化口径拆多目标（组合串"a + b"→两条声明），
    # 冲突/指纹/存储逐目标执行；归一化空结果（纯分隔符）兜底原值防丢声明。
    canonical = normalize_targets(target) or [target]
    marked_detail = detail
    any_changed = False
    with _declare_op_lock:
        for ct in canonical:
            # 检查最近是否有其他 Agent 声明同一目标
            if ct in _recent_ops:
                prev = _recent_ops[ct]
                prev_agent = prev.get("agent", "")
                if prev_agent and prev_agent != agent and not conflict_info["conflict"]:
                    try:
                        prev_dt = datetime.fromisoformat(prev["ts"].replace("Z", "+00:00"))
                        elapsed = (now - prev_dt).total_seconds()
                        if elapsed < OP_CONFLICT_WINDOW:
                            conflict_info["conflict"] = True
                            conflict_info["other_agent"] = prev_agent
                            conflict_info["detail"] = prev.get("detail", "")
                    except Exception:
                        pass

            # Task597 #54: 内容变化事后检测——同目标再声明时对比上次声明时的指纹，
            # hash/mtime/size 任一变化 → detail 附「内容已变化」标记（不拦截，事后可溯）
            new_fp = _fingerprint_target(ct)
            prev_fp = (_recent_ops.get(ct) or {}).get("fp") if ct in _recent_ops else None
            content_changed = bool(
                prev_fp and new_fp
                and (prev_fp.get("sha1") != new_fp.get("sha1")
                     or prev_fp.get("mtime") != new_fp.get("mtime")
                     or prev_fp.get("size") != new_fp.get("size"))
            )
            if content_changed:
                any_changed = True
                logger.info(
                    f"[Task597] op-log 内容变化: {ct} 上次声明后指纹改变 "
                    f"(prev={prev_fp} now={new_fp})"
                )

            # 记录本次声明（含指纹，供下次声明对比）；detail 染色变化标记
            # （记录/事件流/返回值三处同源，GET /session/op-log 可见）
            marked_detail = (detail + " [内容已变化:上次声明后指纹改变]") if content_changed else detail
            _recent_ops[ct] = {
                "agent": agent,
                "ts": now.isoformat(),
                "op": op,
                "detail": marked_detail,
                "fp": new_fp,
            }
        conflict_info["detail_changed"] = any_changed
        # Task541 修复2: 持久化到 recent-ops.json（重启后 30s 冲突窗不丢）
        _save_recent_ops()


    # 事件 target 留原声明串（人类可读）；归一化后的规范路径见 _recent_ops 键
    evt = append_event(agent, "op", detail=f"{op}: {marked_detail}", target=target,
                       extra={"session_id": session_id} if session_id else None)

    if conflict_info["conflict"]:
        logger.warning(
            f"[presence] CONFLICT: {agent} 声明 {op} {target}，"
            f"但 {conflict_info['other_agent']} {conflict_info['detail']} 在 {OP_CONFLICT_WINDOW}s 内也操作过"
        )

    return conflict_info


def broadcast_message(
    sender: str,
    message: str,
    msg_type: str = "info",
    route_mentions: bool = True,
) -> Dict[str, Any]:
    """广播消息给所有在线 Agent（写事件流 + 可选中央 @mention 路由）。

    Args:
        sender: 发送方 Agent
        message: 消息内容
        msg_type: info/warning/alert/need_coord
        route_mentions: True=写入后立即 fan-out 到 AGENT_REGISTRY.on_mention
    Returns:
        {"event": 写入的事件 dict, "routed": [...]} 或仅 event（route_mentions=False 时兼容旧行为）
    """
    event = append_event(
        sender,
        "broadcast",
        detail=message,
        extra={"msg_type": msg_type},
    )
    if not route_mentions:
        return event

    routed: List[Dict[str, Any]] = []
    try:
        from nexusos_xdr.broadcast_router import route_broadcast_event
        routed = route_broadcast_event(event)
        if routed:
            logger.info(
                "[presence] broadcast routed @mentions: %s",
                [r.get("agent") for r in routed],
            )
    except Exception as exc:
        logger.warning("[presence] broadcast @mention route failed: %s", exc)

    return {"event": event, "routed": routed}


# ============================================================================
# 心跳守护线程（清理 stale 会话）
# ============================================================================

def cleanup_stale_sessions() -> int:
    """清理 stale 会话（超过 STALE_REMOVE_THRESHOLD 的移除）。

    Returns:
        清理的会话数
    """
    sessions = load_sessions()
    if not sessions:
        return 0

    now = datetime.now(timezone.utc)
    to_remove = []
    for name, entry in sessions.items():
        hb = entry.get("last_heartbeat", "")
        if not hb:
            continue
        try:
            hb_dt = datetime.fromisoformat(hb.replace("Z", "+00:00"))
            elapsed = (now - hb_dt).total_seconds()
            if elapsed > STALE_REMOVE_THRESHOLD:
                to_remove.append(name)
        except Exception:
            continue

    if to_remove:
        with _sessions_lock():
            sessions = load_sessions()
            for name in to_remove:
                sessions.pop(name, None)
            save_sessions(sessions)
        for name in to_remove:
            append_event(name, "cleanup", detail=f"stale 会话自动清理（{STALE_REMOVE_THRESHOLD}s 无心跳）")
        logger.info(f"[presence] 清理 {len(to_remove)} 个 stale 会话: {to_remove}")

    return len(to_remove)


def unregister_agent(
    agent: str,
    session_id: str = "",
    reason: str = "",
    force: bool = False,
) -> Dict[str, Any]:
    """主动注销会话（persist 登出 / 清理幽灵 presence，Task572 未决#29）。

    安全语义（防误杀活跃 Agent）：
      - 目标不在场 → removed=False（幂等，不报错）
      - session_id 与注册时一致 → 视为本人注销，直接移除
      - 其余情况：仅当目标已 stale（心跳 > STALE_REMOVE_THRESHOLD）才移除；
        目标仍活跃且未带 force=True → refused=True（HTTP 层转 409）

    Returns:
        {"removed": bool, "refused": bool, "error": str}
    Raises:
        ValueError: agent 身份校验失败
    """
    valid, reason_msg = validate_agent(agent)
    if not valid:
        raise ValueError(reason_msg)

    now = datetime.now(timezone.utc)
    with _sessions_lock():
        sessions = load_sessions()
        entry = sessions.get(agent)
        if entry is None:
            return {"removed": False, "refused": False}
        # 本人判定：HTTP 无鉴权，session_id 与注册一致是唯一自证手段
        is_self = bool(session_id) and entry.get("session_id", "") == session_id
        if not is_self:
            hb = entry.get("last_heartbeat", "")
            elapsed = None
            if hb:
                try:
                    hb_dt = datetime.fromisoformat(hb.replace("Z", "+00:00"))
                    elapsed = (now - hb_dt).total_seconds()
                except (ValueError, AttributeError):
                    elapsed = None
            stale = elapsed is not None and elapsed > STALE_REMOVE_THRESHOLD
            if not stale and not force:
                return {
                    "removed": False,
                    "refused": True,
                    "error": (
                        f"会话 '{agent}' 仍在场（心跳 {int(elapsed or 0)}s 前 < "
                        f"{STALE_REMOVE_THRESHOLD}s），注销在场会话需 session_id 自证或 force=true"
                    ),
                }
        sessions.pop(agent, None)
        save_sessions(sessions)
    # 事件写在锁外（与 cleanup_stale_sessions 同模式）
    append_event(agent, "cleanup", detail=f"主动注销: {reason or '未注明原因'}")
    logger.info(f"[presence] {agent} 主动注销（reason={reason or '无'}）")
    return {"removed": True, "refused": False}
