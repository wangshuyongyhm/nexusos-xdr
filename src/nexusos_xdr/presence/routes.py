#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""nexusos_xdr.presence.routes — NEXUS-PRESENCE HTTP 路由域（Task604 #57 批次③子包化试点）。

从 mcp_server.py 物理迁入（行为零变化——函数体原样搬家，只整容器不整形容）：
    /session/register /unregister /heartbeat /active /event /events
    /broadcast /op-log(POST+GET)

架构（试点模式，后续批次复用）：
    - 显式注册：本模块暴露 register_routes(mcp)，由 mcp_server 启动处调用一次。
      装饰器（@mcp.custom_route）在 register_routes 内执行——注册时机显式可见，
      禁止靠 import 副作用注册（顺序敏感=路由丢失风险）。
    - 依赖方向：mcp_server → presence 单向。本模块禁止 import mcp_server。
    - 交叉依赖注入：_issue_search_cred 是 mcp_server 域的凭据签发辅助（非
      presence 域），通过 register_routes(mcp, issue_search_cred=...) 注入——
      避免子包反向 import。
"""
import logging
from typing import Any, Callable, Dict, Optional

import anyio
from starlette.requests import Request
from starlette.responses import JSONResponse

from nexusos_xdr import session_state

logger = logging.getLogger("nexusos-xdr.presence")

# 注入位（register_routes 时绑定；Task639 T3: 参数已改必传——漏传在启动期
# TypeError fail-fast，而非运行期静默跳过签发。值仍可为 None——register 端点
# 内 None 守卫（Task639 新③）护显式传 None 场景）
_issue_search_cred: Optional[Callable[[str, str], Optional[str]]] = None


def register_routes(mcp: Any, issue_search_cred: Callable[[str, str], Optional[str]]) -> None:
    """在 mcp 实例上显式注册 presence 域全部 HTTP 路由（9 函数 10 路由）。

    跨模块契约:
        mcp_server 启动处（模块加载尾部）调用一次，且必须在 mcp 启动监听前——
        FastMCP 装饰注册需发生在 uvicorn run 之前。

    并发语义:
        仅注册期执行（进程启动单线程阶段）；注册后路由函数的并发语义与
        迁移前完全一致（anyio.to_thread 桥接 session_state 同步调用）。

    幂等性:
        🔴 非幂等——重复调用会对同 path 二次注册（FastMCP 行为=路由表重复项）。
        调用方（mcp_server）保证只调一次。

    Args:
        mcp: FastMCP 实例（宿主）
        issue_search_cred: 凭据签发辅助（mcp_server 域注入，register 端点用）
    """
    global _issue_search_cred
    _issue_search_cred = issue_search_cred

    mcp.custom_route("/session/register", methods=["POST"])(http_session_register)
    mcp.custom_route("/session/unregister", methods=["POST"])(http_session_unregister)
    mcp.custom_route("/session/heartbeat", methods=["POST"])(http_session_heartbeat)
    mcp.custom_route("/session/active", methods=["GET"])(http_session_active)
    mcp.custom_route("/session/event", methods=["POST"])(http_session_event)
    mcp.custom_route("/session/events", methods=["GET"])(http_session_events)
    mcp.custom_route("/session/broadcast", methods=["POST"])(http_session_broadcast)
    mcp.custom_route("/session/op-log", methods=["POST"])(http_session_op_log)
    mcp.custom_route("/session/op-log", methods=["GET"])(http_session_op_log_query)




async def http_session_register(request: Request) -> JSONResponse:
    """Agent 注册到在场景（init 时调用）。

    Body: {"agent":"zcode","role":"orchestrator","session_id":"...","current_task":"...","status":"init"}
    """
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "无效 JSON"}, status_code=400)
    agent = body.get("agent", "")
    if not agent:
        return JSONResponse({"error": "缺少 agent"}, status_code=400)
    def _register_sync():
        sessions = session_state.register_agent(
            agent=agent,
            role=body.get("role", "executor"),
            session_id=body.get("session_id", ""),
            current_task=body.get("current_task", ""),
            status=body.get("status", "init"),
        )
        # Task543: reg_kind=router 的重注册写专用 type（与 MCP 入口一致，观测器不误报）
        _evt_type_543 = "router_register" if body.get("reg_kind") == "router" else "init"
        # 1636 活④: 事件透传 session_id（extra 顶层合并）——e2esuite-* 等结构性豁免
        # 靠它识别（check_gate_grace 读 evt.session_id），此前只进 sessions 表不落事件流
        session_state.append_event(agent, _evt_type_543, detail=f"注册: role={body.get('role','executor')}, task={body.get('current_task','')}",
                                   extra={"session_id": body.get("session_id", "")})
        init_context = {}
        try:
            from nexusos_xdr.exp_injector import inject_for_init
            init_context = inject_for_init(agent, role=body.get("role", "executor"))
        except Exception as _ie:
            logger.debug(f"[exp_injector] init 注入失败（不阻塞 register）: {_ie}")
        # Task639 新③: 未注入时守卫（:29 注释声称 None 时跳过签发，原直调 TypeError→500）
        if _issue_search_cred is not None:
            _search_cred = _issue_search_cred(agent, body.get("role", "executor"))
        else:
            _search_cred = None
            logger.warning("[presence] _issue_search_cred 未注入，register 跳过 search_cred 签发")
        return sessions, init_context, _search_cred
    try:
        sessions, init_context, _search_cred = await anyio.to_thread.run_sync(_register_sync)
    except ValueError as e:
        # Task524 P1-1a: 未注册 agent 等身份校验失败——客户端错返 400（原裸 500）
        return JSONResponse({"error": str(e)}, status_code=400)
    except Exception as e:
        logger.exception("HTTP /session/register 异常")
        return JSONResponse({"error": str(e)}, status_code=500)
    return JSONResponse({
        "ok": True, "agent": agent, "active": list(sessions.keys()),
        "init_context": init_context,
        **({"search_credential_id": _search_cred} if _search_cred else {}),
    })


async def http_session_unregister(request: Request) -> JSONResponse:
    """主动注销会话（persist 登出 / 清理幽灵 presence，Task572 未决#29）。

    Body: {"agent":"kimiapp","session_id":"注册时的会话标识","reason":"收工","force":false}
    语义:
      - 不在场 → {"ok":true,"removed":false}（幂等，仍 200）
      - session_id 与注册一致 → 本人注销，直接移除
      - 已 stale（>900s 无心跳）→ 移除（幽灵 presence 场景）
      - 仍活跃的他人会话 → 需 force=true，否则 409
    """
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "无效 JSON"}, status_code=400)
    agent = body.get("agent", "")
    if not agent:
        return JSONResponse({"error": "缺少 agent"}, status_code=400)
    try:
        result = await anyio.to_thread.run_sync(lambda: session_state.unregister_agent(
            agent=agent,
            session_id=body.get("session_id", ""),
            reason=body.get("reason", ""),
            force=bool(body.get("force", False)),
        ))
    except ValueError as e:
        return JSONResponse({"error": str(e)}, status_code=400)
    except Exception as e:
        logger.exception("HTTP /session/unregister 异常")
        return JSONResponse({"error": str(e)}, status_code=500)
    if result.get("refused"):
        return JSONResponse(
            {"ok": False, "removed": False, "error": result.get("error", "")},
            status_code=409,
        )
    return JSONResponse({"ok": True, "agent": agent, "removed": result.get("removed", False)})


async def http_session_heartbeat(request: Request) -> JSONResponse:
    """Agent 心跳更新（每 60s 调用）。

    Body: {"agent":"zcode","status":"working","current_task":"...","context_pct":45}
    """
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "无效 JSON"}, status_code=400)
    agent = body.get("agent", "")
    if not agent:
        return JSONResponse({"error": "缺少 agent"}, status_code=400)
    def _hb_sync():
        sessions = session_state.heartbeat_agent(
            agent=agent,
            status=body.get("status", "working"),
            current_task=body.get("current_task", ""),
            context_pct=body.get("context_pct"),
        )
        return sessions, session_state.now_iso()
    try:
        sessions, hb_at = await anyio.to_thread.run_sync(_hb_sync)
    except ValueError as e:
        # Task538 修复3: validate_agent 失败——客户端错返 400（原裸 500）
        return JSONResponse({"error": str(e)}, status_code=400)
    except Exception as e:
        logger.exception("HTTP /session/heartbeat 异常")
        return JSONResponse({"error": str(e)}, status_code=500)
    return JSONResponse({"ok": True, "agent": agent, "heartbeat_at": hb_at})


async def http_session_active(request: Request) -> JSONResponse:
    """查询所有活跃 Agent（轮询端点，给日常感知用）。"""
    active = await anyio.to_thread.run_sync(session_state.get_active_sessions)
    return JSONResponse({
        "active_count": len(active),
        "agents": active,
    })


async def http_session_event(request: Request) -> JSONResponse:
    """推送关键事件到事件流（op/done/block/alert）。

    Body: {"agent":"zcode","type":"done","detail":"完成简历修改","target":""}
    """
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "无效 JSON"}, status_code=400)
    agent = body.get("agent", "")
    event_type = body.get("type", "")
    if not agent or not event_type:
        return JSONResponse({"error": "缺少 agent 或 type"}, status_code=400)
    def _event_sync():
        evt = session_state.append_event(
            agent=agent,
            event_type=event_type,
            detail=body.get("detail", ""),
            target=body.get("target", ""),
        )
        routed = []
        if event_type == "broadcast":
            try:
                from nexusos_xdr.broadcast_router import route_broadcast_event
                routed = route_broadcast_event(evt)
            except Exception as exc:
                logger.warning("[session/event] broadcast route 失败: %s", exc)
        return evt, routed
    try:
        evt, routed = await anyio.to_thread.run_sync(_event_sync)
    except ValueError as e:
        # Task524 P1-1b: 身份校验失败——客户端错返 400（原裸 500）
        return JSONResponse({"error": str(e)}, status_code=400)
    except Exception as e:
        logger.exception("HTTP /session/event 异常")
        return JSONResponse({"error": str(e)}, status_code=500)
    return JSONResponse({"ok": True, "event": evt, "routed": routed})


async def http_session_events(request: Request) -> JSONResponse:
    """拉取事件流（增量轮询）。

    Query: ?since=ISO时间戳&limit=100&agent=zcode&assert_pairs=1
    Task593: assert_pairs=1 时附带配对断言结果（断裂清单）；默认不带（零开销）。
    """
    qp = request.query_params
    since = qp.get("since", "")
    limit = int(qp.get("limit", "100"))
    agent_filter = qp.get("agent", "")
    events = await anyio.to_thread.run_sync(lambda: session_state.get_events(since=since, limit=limit, agent_filter=agent_filter))
    resp = {
        "count": len(events),
        "events": events,
    }
    # Task593: 配对断言旁路（可选；断言异常 fail-open 不影响原响应）
    if qp.get("assert_pairs") == "1":
        # Task639 P2: assert_ledger_pairs 是 O(n²)——events 量由调用方 limit
        # 控制（默认 100），超大 limit（如全量拉取）时拒绝扫描，防管理端点慢查询
        if len(events) > 1000:
            resp["pair_assertion"] = {
                "error": f"events={len(events)} > 1000，O(n²) 配对扫描拒绝执行，请用 since/limit 收窄",
            }
        else:
            try:
                breaks = session_state.assert_ledger_pairs(events)
                resp["pair_assertion"] = {
                    "checked": len(events),
                    "breaks": breaks,
                    "break_count": len(breaks),
                }
            except Exception as e:
                logger.warning("[Task593] assert_pairs 异常（fail-open）: %s", e)
                resp["pair_assertion"] = {"error": str(e)}
    return JSONResponse(resp)


async def http_session_broadcast(request: Request) -> JSONResponse:
    """广播消息给所有在线 Agent。

    Body: {"sender":"zcode","message":"我开始改init.md了","msg_type":"alert"}
    """
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "无效 JSON"}, status_code=400)
    sender = body.get("sender", body.get("agent", ""))
    message = body.get("message", "")
    msg_type = body.get("msg_type", "info")
    if not sender or not message:
        return JSONResponse({"error": "缺少 sender 或 message"}, status_code=400)
    try:
        result = await anyio.to_thread.run_sync(lambda: session_state.broadcast_message(sender, message, msg_type))
    except ValueError as e:
        # Task524 P1-1c: 身份校验失败——客户端错返 400（原裸 500）
        return JSONResponse({"error": str(e)}, status_code=400)
    except Exception as e:
        logger.exception("HTTP /session/broadcast 异常")
        return JSONResponse({"error": str(e)}, status_code=500)
    if isinstance(result, dict) and "event" in result:
        return JSONResponse({
            "ok": True,
            "event": result["event"],
            "routed": result.get("routed", []),
        })
    return JSONResponse({"ok": True, "event": result, "routed": []})


async def http_session_op_log(request: Request) -> JSONResponse:
    """声明操作意图（操作前调用，检测并发冲突）。

    Body: {"agent":"zcode","op":"edit","target":"/path/to/file","detail":"改排版"}
    Task1704 件一（池#285）：支持 "targets": ["/a", "/b", "相对/c"] 多目标
    （与 target 单串二选一或并存）；每目标逐条声明并归一化（相对/~/组合串
    拆分见 session_state.normalize_targets），冲突取并集。
    Returns: {"declared":true,"conflict":bool,"other_agent":"..."}
    """
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "无效 JSON"}, status_code=400)
    agent = body.get("agent", "")
    op = body.get("op", "")
    target = body.get("target", "")
    targets_raw = body.get("targets") or []
    if not isinstance(targets_raw, list):
        return JSONResponse({"error": "targets 须为数组"}, status_code=400)
    all_targets = [t for t in ([target, *targets_raw] if target else targets_raw) if t]
    if not agent or not op or not all_targets:
        return JSONResponse({"error": "缺少 agent/op/target(s)"}, status_code=400)
    try:
        merged: dict = {"declared": True, "conflict": False, "other_agent": "", "detail": ""}
        for t in all_targets:
            result = await anyio.to_thread.run_sync(
                lambda _t=t: session_state.declare_op(agent, op, _t, body.get("detail", "")))
            if result.get("conflict"):
                merged["conflict"] = True
                merged["other_agent"] = result.get("other_agent", "")
                merged["detail"] = result.get("detail", "")
            if result.get("detail_changed"):
                merged["detail_changed"] = True
        return JSONResponse(merged)
    except ValueError as e:
        # Task517: 未注册 agent 等身份校验失败——客户端错返 400（原裸 500）
        return JSONResponse({"error": str(e)}, status_code=400)
    except Exception as e:
        logger.exception("HTTP /session/op-log 异常")
        return JSONResponse({"error": str(e)}, status_code=500)



async def http_session_op_log_query(request: Request) -> JSONResponse:
    """查询最近操作声明（只读，与 POST /session/op-log 同 URL，按 method 区分）。

    Query:
        ?target=<文件路径>  可选，过滤指定 target
        ?within=<秒>        可选，默认 120，只返回 N 秒内的声明
    Returns:
        {"ops": [{"target","agent","op","ts","detail"}, ...], "count": N}
    Task1704 件一（池#285）：匹配改归一化口径（session_state.query_recent_ops，
    exact-归一化或 basename 命中）——治相对路径/~/组合串声明永不命中的误报。
    """
    qp = request.query_params
    target = qp.get("target", "")
    try:
        within = int(qp.get("within", "120"))
    except ValueError:
        within = 120

    try:
        ops = session_state.query_recent_ops(target=target, within=within)
    except Exception as exc:
        return JSONResponse({"ops": [], "count": 0, "error": str(exc)})
    return JSONResponse({"ops": ops, "count": len(ops)})


