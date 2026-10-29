#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
mcp_server.py — nexusos-xdr MCP Server + HTTP API 双协议

端口: 19998
- MCP:  /mcp（streamable-http，小弟原生接入）
- HTTP: /dispatch /task/<id> /cancel/<id> /health（kimi curl fallback）

6 核心工具（设计文档 §4.0）:
  dispatch / search_knowledge / write_knowledge
  request_approval / check_task_status / cancel_task

3 视觉预留工具（设计文档 §4.6，v0.4 降级文本启发式）:
  capture_screen / detect_popup_visual / analyze_ui_state

锁统一（v0.4 kimi 盲区2）:
  MCP 工具和 HTTP 端点都调同一个 dispatchd.dispatch() 函数。
  锁在 dispatch() 里，不在入口层。

作者: ZCode Agent  日期: 2026-07-13
"""

import sys
import os
import json
import logging
import threading
import time
import urllib.request
import urllib.error
import re
from pathlib import Path
import anyio
from typing import Optional

# === 路径设置（import nexus 包 + nexusos_xdr 包）===
_SCRIPT_DIR = Path(__file__).resolve().parent
_AUTO_DIR = _SCRIPT_DIR.parent  # 自动化脚本/
# Task 282 P2-3: sys.path 初始化改用 nexus.paths_setup 单源（原 for 循环 hack 删）
from nexusos_xdr import _vault_env as _ve  # Task668: 包内自足（干净环境 no-op）
_ve.setup_sys_path()

from fastmcp import FastMCP
from fastmcp.server.middleware import AuthMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

from nexusos_xdr import dispatchd
from nexusos_xdr import task_state
from nexusos_xdr import session_state
from nexusos_xdr.mcp_auth import nexus_mcp_auth  # 1417: MCP 层凭据中间件 auth check
from nexusos_xdr import spill  # Task311 S1: callback 大 summary 外溢（LOAD 期校验配置）
from nexusos_xdr.guards import execute_guard_global, DestructiveOperationError
from nexusos_xdr._vault_env import VAULT_ROOT  # Task668: 包内自足（干净环境可解析）

# NEXUS Gate engine(Task 365 升级:声明式门禁,2026-07-29)
# Task668 ②: 包路径 import（原裸名依赖 workdir sys.path——venv/干净环境不可达；
# nexusos_gate 是 xdr 的声明依赖，非 editable 安装下恒可达，降级只剩包真损场景）
try:
    from nexusos_gate.gate_engine import check as gate_engine_check
    _GATE_ENGINE_AVAILABLE = True
except Exception as _ge_err:
    _GATE_ENGINE_AVAILABLE = False
    # Task524 P1-6: 降级分支不依赖 logger（此时 logger 尚未定义，引用会 NameError 炸启动）
    print(f"[gate] gate_engine 加载失败,降级到旧 _run_done_gate: {_ge_err}", file=sys.stderr)

# Task518: on_pass 钩子注册表——scenario→动作 映射，gate passed 后自动执行
# Task538 修复5: on_pass 下沉到 gate_engine（CLI/其他 caller 也受益）。
# mcp_server 仅保留注册回调（模块加载时注册到 engine，不依赖 MCP 调用）。
try:
    from nexusos_gate.gate_engine import register_on_pass as _register_on_pass_engine  # Task668 ②
    _ON_PASS_ENGINE_AVAILABLE = True
except Exception:
    _ON_PASS_ENGINE_AVAILABLE = False

try:
    # T674 ②: 经 nexusos_xdr._vault_deps 解析——flip 后 venv 无 vault sys.path 时
    # 自动定位 vault 单源；真干净环境降级（与原 try/except 语义一致）。
    from nexusos_xdr._vault_deps import write_checkpoint, recover, CHECKPOINT_READY as _CHECKPOINT_AVAILABLE
except Exception as _cp_err:
    _CHECKPOINT_AVAILABLE = False
    write_checkpoint = recover = None
    # Task524 P1-6: 降级分支不依赖 logger（此时 logger 尚未定义，引用会 NameError 炸启动）
    print(f"[checkpoint] nexus.checkpoint 加载失败，state_transition 钩子降级: {_cp_err}", file=sys.stderr)


def _on_state_transition_pass(scenario: str, context: dict, result: dict) -> None:
    """state_transition gate passed → 写 LOOP checkpoint

    context 除 required 的 from_state/to_state/session_id 外，
    主控可塞 goal/queue/dispatch_index/recovery_guide 业务字段（gate_engine 不校验未声明字段）。
    cost 暂不传（数据源待定）。
    Task1682-d13: 增 inflight_summary/pending_events 透传（MSAF 在途消息+待决事件两层）。
    """
    if not _CHECKPOINT_AVAILABLE:
        return
    # queue 可能是 list 或逗号分隔字符串，兼容处理
    queue = context.get("queue", [])
    if isinstance(queue, str) and queue:
        queue = [q.strip() for q in queue.split(",") if q.strip()]
    # pending_events 同 queue 兼容（list 或逗号分隔字符串）
    pending_events = context.get("pending_events", [])
    if isinstance(pending_events, str) and pending_events:
        pending_events = [e.strip() for e in pending_events.split(",") if e.strip()]
    try:
        write_checkpoint(
            state=context.get("to_state", ""),
            session_id=context.get("session_id", ""),
            goal=context.get("goal", ""),
            queue=queue or None,  # 空则传 None 让 write_checkpoint 默认 []
            # Task532 P-c2: cost 补传（原漏传永空——LOOP 运行时可从 context 塞）
            cost=context.get("cost") or None,
            dispatch_index=context.get("dispatch_index", 0),
            recovery_guide=context.get("recovery_guide", ""),
            inflight_summary=context.get("inflight_summary", ""),
            pending_events=pending_events or None,
        )
    except Exception as _cpw:
        # T1556 B5: cp 写失败自保护显式告警——gate_engine on_pass 对钩子异常
        # fail-open 只留一行无上下文 warning，checkpoint 静默丢失 = LOOP 恢复面
        # 断层（1546 种子2/3）。此处带 session/state 定位补结构化告警，不抛出
        # （gate 结果语义不变）。
        logger.warning(
            f"[on_pass] state_transition checkpoint 写入失败"
            f" session={context.get('session_id', '')}"
            f" state={context.get('to_state', '')}:"
            f" {type(_cpw).__name__}: {_cpw}"
        )


if _CHECKPOINT_AVAILABLE and _ON_PASS_ENGINE_AVAILABLE:
    _register_on_pass_engine("state_transition", _on_state_transition_pass)


def _on_loop_start_pass(scenario: str, context: dict, result: dict) -> None:
    """loop_start gate passed → 若存在上次崩溃 checkpoint，把恢复指引塞进 result。

    Task522: recover 读最新 checkpoint。Task531 B8: 按 session_id 筛（多 LOOP 并跑
    防串会话，Task525 的 read_latest 已支持）——原无参 recover() 读任意最新。
    首次 LOOP 无崩溃历史时 recover 返"无 checkpoint 可恢复"，不塞字段。
    """
    if not _CHECKPOINT_AVAILABLE:
        return
    try:
        recovery = recover(context.get("session_id", ""))
        if recovery and recovery != "无 checkpoint 可恢复":
            result["loop_recovery"] = recovery  # 注入 gate 结果，主控读返回值即见
    except Exception as _e:
        logger.warning(f"[on_pass] loop_start recover 失败（不阻断 gate）: {_e}")


if _CHECKPOINT_AVAILABLE and _ON_PASS_ENGINE_AVAILABLE:
    _register_on_pass_engine("loop_start", _on_loop_start_pass)

# === 日志 ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    stream=sys.stderr,  # MCP 模式 stdout 被协议占用，日志走 stderr
)
logger = logging.getLogger("nexusos-xdr.mcp")
# 1970 启动更新提示（一次性日志行；feed off/同版零输出；缓存读+后台刷新零阻塞）
try:
    from nexusos_common.update_check import server_notify
    server_notify(logger.warning)
except Exception:
    pass

# === nexus.shared 知识库（启动时常驻 + 场景8: 线程安全锁）===
_kb = None
_kb_lock = threading.Lock()  # psycopg2 connection 非线程安全，MCP 并发调用需序列化

def get_kb():
    """懒加载 SharedKnowledge 实例（启动时构造，复用连接）。

    LOOP-3修复(2026-07-30): 原 if _kb is None 检查在锁外,
    并发调用会创建多个 SharedKnowledge 实例(各持PG连接)致泄漏。
    改用 double-checked locking: 锁外快查 + 锁内再查,只创建一次。
    """
    global _kb
    if _kb is None:
        with _kb_lock:
            if _kb is None:  # double-check: 拿锁后再查一次
                try:
                    from nexusos_mem.shared import SharedKnowledge
                    _kb = SharedKnowledge(agent_id="nexusos-xdr")
                    logger.info("SharedKnowledge 实例已创建")
                except Exception as e:
                    logger.error(f"SharedKnowledge 初始化失败: {e}")
    return _kb


# === FastMCP 实例 ===
# 1417（MCP L0+L1）：AuthMiddleware + mcp_auth.nexus_mcp_auth——工具集按凭据
# 分层可见（L0 匿名只读 / L1 凭据写 / L2 本批不开放），无效凭据 fail-closed。
mcp = FastMCP(
    "nexusos-xdr",
    middleware=[AuthMiddleware(auth=nexus_mcp_auth)],
)


# ============================================================================
# MCP 工具域 — Task620 (#57) 子包化迁出 → nexusos_xdr/tools/
# 物理实现（18 工具 + _forward_to_ocrx/_issue_search_cred）在
# nexusos_xdr/tools/registry.py；此处 shim re-export + 启动处显式注册。
# ============================================================================
from nexusos_xdr.tools import (  # noqa: E402  Task620 shim（显式列名）
    dispatch as _tool_dispatch,
    search_knowledge,
    apply_experience_outcome,
    write_knowledge,
    request_approval,
    check_task_status,
    cancel_task,
    capture_screen,
    detect_popup_visual,
    analyze_ui_state,
    verify_mcp_config,
    check_last_mile,
    presence_register,
    presence_active,
    presence_heartbeat,
    presence_events,
    presence_declare_op,
    gate_check,
    _issue_search_cred,
    _forward_to_ocrx,
    register_tools as _tools_register_routes,
)

# 工具名 dispatch 与本模块语义无冲突（HTTP 域用 http_dispatch）；shim 别名保外部
# `mcp_server.dispatch` 引用（MCP 客户端老配置/测试 patch 目标）不变。
dispatch = _tool_dispatch


# ============================================================================
# callback 域 — Task618 (#57 主线批) 子包化迁出 → nexusos_xdr/callback/
# 🔴 Task621 返工恢复：本 import 块曾因 T621 删 HTTP 域段时切割锚误吞
# （生产 xdr 重启崩溃循环 mcp_server:667 NameError——main() 不被测试覆盖，
# 323 绿未拦截。教训入 T621 report 附录）。
# ============================================================================
from nexusos_xdr.callback import (  # noqa: E402  Task618 shim（T621 恢复）
    http_callback,
    register_routes as _callback_register_routes,
)


# ============================================================================
# HTTP 任务 API 域 — Task621 (#57 收官批) 迁出 → nexusos_xdr/httpapi/
# 物理实现（/dispatch /task /task_state /cancel 四端点）在
# nexusos_xdr/httpapi/routes.py；此处 shim re-export + 启动处显式注册。
# ============================================================================
from nexusos_xdr.httpapi import (  # noqa: E402  Task621 shim
    http_dispatch,
    http_task_status,
    http_task_state_list,
    http_cancel,
    register_routes as _httpapi_register_routes,
)



@mcp.custom_route("/health", methods=["GET"])
async def http_health(request: Request) -> JSONResponse:
    """健康检查。"""
    def _health_sync():
        kb_ok = get_kb() is not None
        agents = dispatchd.list_supported_agents()
        active = task_state.list_active()
        router_loaded = False
        external_last_mile = {}
        try:
            from nexusos_xdr.broadcast_router import route_broadcast_event  # noqa: F401
            from nexusos_xdr.agents import audit_external_last_mile
            router_loaded = True
            external_last_mile = audit_external_last_mile()
        except Exception:
            pass
        return kb_ok, agents, active, router_loaded, external_last_mile
    kb_ok, agents, active, router_loaded, external_last_mile = await anyio.to_thread.run_sync(_health_sync)
    return JSONResponse({
        "status": "ok",
        "service": "nexusos-xdr",
        "version": "1.0.0",
        "port": 19998,
        "knowledge_base": "connected" if kb_ok else "disconnected",
        "agents": agents,
        "active_tasks": len(active),
        "features": {
            "broadcast_router": router_loaded,
        },
        "external_last_mile": external_last_mile,
    })


@mcp.custom_route("/system/dump", methods=["GET"])
async def http_system_dump(request: Request) -> JSONResponse:
    """体系配置全景单一视图（Task 305，仿 DeepSeek Harness dsh --dump-config）。

    只聚合进程内已有结构：AGENT_REGISTRY / session_state / task_state 活跃任务 /
    external_last_mile（组装方式同 /health）。禁磁盘重 IO（铁律#24，Task 488 教训）。
    fail-open：任一数据源异常 → 该段标 null 继续，整体不 500。
    回环只读，anonymous 豁免风格（无凭据逻辑）。
    """
    def _dump_sync():
        agents = None
        try:
            from nexusos_xdr.agents import AGENT_REGISTRY
            agents = [
                {
                    "name": name,
                    "status": entry.get("status", "unknown"),
                    "active_only": entry.get("status") == "active",  # 是否出现在 list_agents(active_only=True)
                    "dispatchable": entry.get("cls") is not None,    # 有 cls 才可被 dispatch
                    "wake_mode": entry.get("wake_mode"),
                }
                for name, entry in AGENT_REGISTRY.items()
            ]
        except Exception as e:
            logger.warning(f"/system/dump agents 段失败: {e}")

        sessions = None
        try:
            active_sessions = session_state.get_active_sessions()
            sessions = [
                {
                    "agent": name,
                    "session_id": info.get("session_id"),
                    "status": info.get("status"),
                    "role": info.get("role"),
                    "current_task": info.get("current_task"),
                }
                for name, info in (active_sessions or {}).items()
            ]
        except Exception as e:
            logger.warning(f"/system/dump sessions 段失败: {e}")

        active_tasks = None
        try:
            active_tasks = [
                {
                    "task_id": t.get("task_id"),
                    "assignee": t.get("callee") or t.get("assignee"),
                    "status": t.get("status"),
                    "created": t.get("created"),
                }
                for t in task_state.list_active()
            ]
        except Exception as e:
            logger.warning(f"/system/dump active_tasks 段失败: {e}")

        external_last_mile = None
        try:
            from nexusos_xdr.agents import audit_external_last_mile
            external_last_mile = audit_external_last_mile()
        except Exception as e:
            logger.warning(f"/system/dump external_last_mile 段失败: {e}")

        # Task316 G2: 运行态凭据脱敏清单（Harness CredentialInfo 等价物）——
        # 只输出类型/状态计数与到期分布，零 credential_id 零秘密值（fail-open 同 Task305 模式）
        credentials = None
        try:
            import psycopg2
            from nexusos_xdr.credential import CRED_DB
            _c = psycopg2.connect(f"dbname={CRED_DB}")
            _cur = _c.cursor()
            _cur.execute(
                "SELECT cred_type, status, count(*), min(expires_at)::text "
                "FROM credentials GROUP BY cred_type, status ORDER BY cred_type, status")
            _rows = _cur.fetchall()
            _cur.close(); _c.close()
            credentials = {
                "rows": [
                    {"cred_type": r[0], "status": r[1], "count": r[2],
                     "earliest_expiry": r[3]}
                    for r in _rows
                ],
                "hmac_secret_present": True,  # 存在性布尔，不读内容
            }
        except Exception as e:
            logger.warning(f"/system/dump credentials 段失败: {e}")

        return agents, sessions, active_tasks, external_last_mile, credentials

    agents, sessions, active_tasks, external_last_mile, credentials = await anyio.to_thread.run_sync(_dump_sync)
    return JSONResponse({
        "service": "nexusos-xdr",
        "version": "1.0.0",
        "port": 19998,
        "agents": agents,
        "sessions": sessions,
        "active_tasks": active_tasks,
        "external_last_mile": external_last_mile,
        "credentials": credentials,
    })


# ============================================================================
# NEXUS-PRESENCE: 在场感知端点 — Task604 (#57 批次③) 子包化迁出
# 物理实现已迁 nexusos_xdr/presence/（路由+函数体原样搬家）；此处保留：
#   ① shim re-export（外部引用/测试 patch 语义不变——同一函数对象）
#   ② 启动处显式注册（register_routes，见文件尾部调用点）
# ============================================================================
from nexusos_xdr.presence import (  # noqa: E402  Task604 shim
    http_session_register,
    http_session_unregister,
    http_session_heartbeat,
    http_session_active,
    http_session_event,
    http_session_events,
    http_session_broadcast,
    http_session_op_log,
    http_session_op_log_query,
    register_routes as _presence_register_routes,
)

# session 心跳守护线程（清理 stale 会话）
# ============================================================================

SESSION_PATROL_INTERVAL = 120  # 2 分钟巡检一次


def _session_heartbeat_patrol_loop(stop_event=None):
    """系统级 session 心跳守护（daemon 线程）。

    每 SESSION_PATROL_INTERVAL 秒清理 stale 会话（超时无心跳的移除）。
    """
    logger.info("[session-patrol] 在场感知守护线程启动（间隔 %ss）", SESSION_PATROL_INTERVAL)
    while True:
        try:
            cleaned = session_state.cleanup_stale_sessions()
            if cleaned > 0:
                logger.info("[session-patrol] 清理 %s 个 stale 会话", cleaned)
        except Exception as e:
            logger.error("[session-patrol] 巡检异常（不中断循环）: %s", e)
        if stop_event is not None:
            if stop_event.wait(SESSION_PATROL_INTERVAL):
                break
        else:
            import time as _time
            _time.sleep(SESSION_PATROL_INTERVAL)


def _start_session_heartbeat_patrol():
    """启动 session 心跳守护 daemon 线程。"""
    patrol_thread = threading.Thread(
        target=_session_heartbeat_patrol_loop,
        name="session-heartbeat-patrol",
        daemon=True,
    )
    patrol_thread.start()
    logger.info("[session-patrol] 守护线程已启动")
    return patrol_thread


def _gate_rollup_loop(stop_event=None):
    """T657 修1: gate 事件小时级聚合 rollup（与逐条去重互补）。

    648 实证事件流虚高主因是同任务重发（已由 task_state.once_flag 去重）；
    rollup 再给 Console(647)/监控一个低频聚合视图：每小时一条 gate_rollup
    事件，含 (agent × scenario × pass/fail) 计数，替代逐条重发的聚合需求。
    fail-open：任何异常只 log 不中断。
    """
    import time as _t657
    import json as _json657
    from collections import deque as _deque657
    logger.info("[gate-rollup] 聚合线程启动（间隔 3600s）")
    while True:
        try:
            from nexusos_xdr import session_state as _ss657
            from datetime import datetime as _dt657, timezone as _tz657, timedelta as _td657
            from collections import Counter as _Ctr657
            _f657 = _ss657.archive_path_today()  # Task1951 G8: events 日切归档后读当日文件
            if _f657.exists():
                _cutoff657 = _dt657.now(_tz657.utc) - _td657(hours=1)
                _agg657 = _Ctr657()
                with open(_f657, encoding="utf-8", errors="replace") as _fh657:
                    # Task1958: 禁 readlines 整读——事故期 7.8GB 归档整读释放 3000 万
                    # dict 霸占 GIL 致服务假死 20 分钟；deque(maxlen) 单遍流读 O(2000) 内存
                    for _line657 in _deque657(_fh657, maxlen=2000):
                        try:
                            _e657 = _json657.loads(_line657)
                        except Exception:
                            continue
                        if _e657.get("type") != "gate_check":
                            continue
                        try:
                            _dtv657 = _dt657.fromisoformat(
                                str(_e657.get("ts", "")).replace("Z", "+00:00"))
                            if _dtv657.tzinfo is None:
                                _dtv657 = _dtv657.replace(tzinfo=_tz657.utc)
                            if _dtv657 < _cutoff657:
                                continue
                        except Exception:
                            continue
                        _d657 = str(_e657.get("detail", ""))
                        _sc657 = _d657.split("scenario=")[1].split(" ")[0] \
                            if "scenario=" in _d657 else "?"
                        _agg657[(_e657.get("agent", "?"), _sc657,
                                 "passed=True" in _d657)] += 1
                if _agg657:
                    _parts657 = "; ".join(
                        f"{_a657}/{_s657}/{'pass' if _ok657 else 'fail'}={_n657}"
                        for (_a657, _s657, _ok657), _n657 in sorted(_agg657.items()))
                    _ss657.append_event(
                        agent="xdr", event_type="gate_rollup",
                        detail=f"1h聚合: {_parts657[:800]}", target="gate:rollup")
        except Exception as _e657:
            logger.warning("[gate-rollup] 异常（不中断）: %s", _e657)
        if stop_event is not None:
            if stop_event.wait(3600):
                break
        else:
            _t657.sleep(3600)


def _start_gate_rollup():
    """启动 gate rollup 聚合 daemon 线程。"""
    threading.Thread(
        target=_gate_rollup_loop,
        name="gate-rollup",
        daemon=True,
    ).start()


@mcp.custom_route("/", methods=["GET"])
async def http_root(request: Request) -> JSONResponse:
    """根路径信息。"""
    return JSONResponse({
        "service": "nexusos-xdr",
        "version": "1.0.0",
        "description": "所有 Agent 的通信协议层",
        "endpoints": {
            "mcp": "/mcp (streamable-http)",
            "dispatch": "POST /dispatch",
            "callback": "POST /callback",
            "task_status": "GET /task/{task_id}",
            "cancel": "POST /cancel/{task_id}",
            "session_register": "POST /session/register",
            "session_heartbeat": "POST /session/heartbeat",
            "session_active": "GET /session/active",
            "session_event": "POST /session/event",
            "session_events": "GET /session/events",
            "session_broadcast": "POST /session/broadcast",
            "session_op_log": "POST /session/op-log",
            "health": "GET /health",
        },
        "agents": dispatchd.list_supported_agents(),
    })


# ============================================================================
# 启动
# ============================================================================

# ============================================================================
# task 268: 系统级 embedding 健康守护巡逻（daemon 线程，复用 xdr 主进程）
# ============================================================================

EMBED_HEALTH_INTERVAL = 300  # 5 分钟巡检一次（系统级低频，区别于任务 patrol 的 30s）
EMBED_WORKER_URL = "http://127.0.0.1:11436"


def _check_embed_worker_health() -> bool:
    """探测 EMBED worker 是否 UP 且就绪。UP+ready 返回 True。

    判定依据：/health 端点返回 status=ok 且 ready=True。
    worker 重启窗口期 ready 可能 False（模型加载中），此时不补救（避免补出错误向量）。
    """
    try:
        import json as _json
        req = urllib.request.Request(f"{EMBED_WORKER_URL}/health", method="GET")
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = _json.loads(resp.read().decode("utf-8"))
        return data.get("status") == "ok" and data.get("ready") is True
    except Exception:
        return False


def _count_embedding_null(kb) -> int:
    """统计 embedding=NULL 且未废弃的经验条数。

    task 275 P0-3: 改用 SharedKnowledge 公开接口 count_embedding_null，
    不再调 kb._get_conn() 私有方法 + 裸 SQL（schema 变更会直接崩，
    且与 _kb_lock 保护的 add_knowledge 并发不安全）。
    """
    return kb.count_embedding_null()


def _reembed_nulls_safe(kb, limit: int = 50) -> tuple:
    """安全补齐 embedding=NULL（复用 reembed_nulls.embed_text）。

    task 275 P0-3: 改用 SharedKnowledge 公开接口（list_null_embedding_ids +
    reembed_knowledge），不再 kb._get_conn() + 裸 SQL UPDATE。

    Returns: (fixed_count, failed_count)
    """
    # 1636 活①收口: 原写法 insert 错误目录(切片后 parent.parent=src/ 非 vault
    # 自动化脚本/) + import 未切片的 reembed_nulls——干净/wheel 部署调用即炸。
    # 改用 nexusos_mem 公开接口 encode_query(worker+本地降级, 语义等价 embed_text)。
    from nexusos_mem.search import encode_query

    rows = kb.list_null_embedding_ids(limit=limit)
    fixed, failed = 0, 0
    for kid, title, content in rows:
        text = f"{title}\n{content or ''}"[:4000]
        emb = encode_query(text)
        if emb and len(emb) == 1024:
            if kb.reembed_knowledge(kid, emb):
                fixed += 1
            else:
                failed += 1
        else:
            failed += 1
    return fixed, failed


def _embed_health_patrol_loop(stop_event=None):
    """系统级 embedding 健康守护巡逻（daemon 线程主体）。

    每 EMBED_HEALTH_INTERVAL 秒：
    1. 探测 EMBED worker health（必须 ready=True，避免在模型加载期补错向量）
    2. worker UP → 统计 embedding=NULL → 有则补齐
    3. worker DOWN → 跳过本轮（等恢复），记日志
    4. worker UP 且 null=0 → 静默健康（不刷日志防噪音）

    异常不中断循环（单次巡检失败不能杀掉整个守护线程）。
    """
    logger.info("[embed-patrol] 系统级 embedding 健康守护线程启动（间隔 %ss）",
                EMBED_HEALTH_INTERVAL)
    while True:
        try:
            worker_up = _check_embed_worker_health()
            if not worker_up:
                logger.warning("[embed-patrol] EMBED worker DOWN，跳过本轮补救（等恢复）")
            else:
                kb = get_kb()
                if kb is None:
                    logger.warning("[embed-patrol] kb 未就绪，跳过本轮")
                else:
                    null_count = _count_embedding_null(kb)
                    if null_count > 0:
                        logger.info("[embed-patrol] 发现 %s 条 embedding=NULL，开始补齐", null_count)
                        fixed, failed = _reembed_nulls_safe(kb)
                        logger.info("[embed-patrol] 补齐完成: fixed=%s failed=%s", fixed, failed)
                    # worker UP 且 null=0 → 静默健康（不刷日志防噪音）
        except Exception as e:
            logger.error("[embed-patrol] 巡检异常（不中断循环）: %s", e)
        # sleep（支持 stop_event 优雅退出；无 stop_event 则纯 sleep）
        if stop_event is not None:
            if stop_event.wait(EMBED_HEALTH_INTERVAL):
                break
        else:
            time.sleep(EMBED_HEALTH_INTERVAL)


def _start_embed_health_patrol() -> "threading.Thread":
    """启动系统级 embedding 健康守护 daemon 线程（main() 调用）。"""
    patrol_thread = threading.Thread(
        target=_embed_health_patrol_loop,
        name="embed-health-patrol",
        daemon=True,  # 随主进程退出（不留孤儿线程）
    )
    patrol_thread.start()
    logger.info("[embed-patrol] 守护线程已启动")
    return patrol_thread


def _restore_active_tasks():
    """缺陷5修复: 服务重启后，给所有 RUNNING 任务重新挂 patrol 线程。

    没有这个的话，服务重启（launchd KeepAlive / 手动 reload）会导致
    所有 wait=False 的在跑任务变孤儿——patrol 死了，没人检测完成/弹窗。

    Task 280 P1-14: 重挂前检查 patrol registry（dispatchd.get_active_patrols()，T602 切公开接口），
      避免 reload（而非完全重启）时同一 task_id 两个 patrol 线程并发。
    Task 280 P1-15: timeout_left 按真实 elapsed 计算（原固定 -300 导致总监控时长翻倍）。
    """
    import threading
    import datetime as _dt
    from nexusos_xdr import dispatchd, task_state
    active = task_state.list_active()
    if not active:
        return
    logger.info(f"[restore] 发现 {len(active)} 个活跃任务，重新挂载 patrol 线程")
    for state in active:
        tid = state.get("task_id")
        callee = state.get("callee")
        if not tid or not callee:
            continue
        # Task 280 P1-14: 已有 patrol 注册则跳过（防 reload 时双 patrol 并发）
        if tid in dispatchd.get_active_patrols():
            logger.info(f"[restore] {tid} 已有 patrol 注册，跳过（避免双线程并发）")
            continue
        agent = dispatchd.get_agent(callee)
        if agent is None:
            logger.warning(f"[restore] {tid}: agent {callee} 不存在，跳过")
            continue
        if not agent.has_session():
            # S3 (Task315): 重启恢复合成点（Harness 式「下一个活着的读者」合成
            # interrupted）。守卫①tmux 服务可用才信 has-session 失败（服务挂=保守
            # 不合成）；守卫②有 done 回调不合成（矛盾态走 check_I10 告警）；
            # 守卫③间隔 2s 双检（等效 patrol 连续 2 轮确认，防启动竞态误判）。
            _s3_done_cb = any(
                str(c.get("status", "")).lower() in ("done", "completed", "success")
                for c in state.get("callbacks", [])
            )
            if dispatchd.tmux_server_alive() and not _s3_done_cb:
                import time as _time_s3
                _time_s3.sleep(2)
                _still_dead = not agent.has_session() and dispatchd.tmux_server_alive()
                if _still_dead:
                    logger.warning(f"[restore] {tid}: {callee} session 不存在（双检确认，无 done 回调），合成 interrupted")
                    try:
                        task_state.update_status(
                            tid, "FAILED",
                            error="服务重启时 session 不存在（双检确认，无 done 回调，合成 interrupted）",
                            end_reason="interrupted",
                        )
                    except Exception:
                        pass
                    continue
            logger.warning(f"[restore] {tid}: {callee} session 不存在，标记 FAILED（守卫未过，不合成 interrupted）")
            try:
                task_state.update_status(tid, "FAILED", error="服务重启时 session 不存在", end_reason="unknown")
            except Exception:
                pass
            continue
        # Task 280 P1-15: timeout_left 按真实 elapsed 计算（原 -300 固定值导致总监控时长翻倍）
        # elapsed = now - started（state["started"] 是 ISO 字符串）
        started_iso = state.get("started")
        elapsed = 0
        if started_iso:
            try:
                started = _dt.datetime.fromisoformat(started_iso)
                if started.tzinfo is None:
                    started = started.replace(tzinfo=_dt.timezone.utc)
                elapsed = (_dt.datetime.now(_dt.timezone.utc) - started).total_seconds()
            except Exception as e:
                logger.warning(f"[restore] {tid} 解析 started 失败（elapsed=0）: {e}")
        timeout_left = max(60, state.get("timeout_sec", 3600) - elapsed)
        # 重新挂 patrol
        stop_evt = threading.Event()
        dispatchd.start_patrol_thread(
            agent, tid, stop_evt,
            task_path=state.get("task_path", ""),
            timeout=timeout_left,
            notify_url=state.get("notify_url", ""),
        )
        logger.info(f"[restore] {tid} ({callee}) patrol 已重新挂载（elapsed={elapsed:.0f}s, timeout_left={timeout_left:.0f}s）")


def main():
    """启动 MCP server。"""
    import argparse
    parser = argparse.ArgumentParser(description="nexusos-xdr MCP Server")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=19998)
    args = parser.parse_args()

    logger.info(f"启动 nexusos-xdr MCP Server: {args.host}:{args.port}")
    logger.info(f"MCP 端点: http://{args.host}:{args.port}/mcp")
    logger.info(f"HTTP 端点: http://{args.host}:{args.port}/dispatch /task/<id> /cancel/<id> /health")

    # 预热知识库连接
    get_kb()

    # 缺陷5修复: 恢复重启前活跃任务的 patrol 线程
    _restore_active_tasks()

    # task 268: 启动系统级 embedding 健康守护线程（daemon，随主进程退出）
    _start_embed_health_patrol()

    # NEXUS-PRESENCE: 启动 session 心跳守护线程（清理 stale 会话）
    _start_session_heartbeat_patrol()
    _start_gate_rollup()  # T657 修1: gate 事件小时级聚合 rollup

    # Task604 (#57 批次③): presence 域路由显式注册（必须在 mcp.run 监听前——
    # FastMCP 装饰注册需发生在 uvicorn 启动前；幂等性：main() 只执行一次）
    _presence_register_routes(mcp, issue_search_cred=_issue_search_cred)

    # Task618 (#57 主线批): callback 域路由显式注册（mcp.run 前；非幂等——main 单次）
    _callback_register_routes(mcp)

    # Task620 (#57): MCP 工具域显式注册（18 工具；mcp.run 前，main 单次）
    _tools_register_routes(mcp, get_kb=get_kb, kb_lock=_kb_lock)

    # Task621 (#57 收官): HTTP 任务 API 域显式注册（四端点）
    _httpapi_register_routes(mcp)

    # streamable-http 传输
    mcp.run(
        transport="streamable-http",
        host=args.host,
        port=args.port,
        path="/mcp",
    )


if __name__ == "__main__":
    main()
