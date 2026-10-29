#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""nexusos_xdr.httpapi.routes — HTTP 任务 API 域（Task621 #57 收官批，自 mcp_server.py 迁入）。

物理迁入（行为零变化）：/dispatch /task/{id} /task_state /cancel/{id} 四端点。
（/health /system/dump 与 main 守护域同留 mcp_server——启动自检性质。）

架构（T604 三要素）：register_routes(mcp) 显式注册；依赖方向
mcp_server → httpapi 单向（本模块 import dispatchd/task_state 正向）。
"""
import logging

import anyio
from starlette.requests import Request
from starlette.responses import JSONResponse

from nexusos_xdr import dispatchd
from nexusos_xdr import task_state

logger = logging.getLogger("nexusos-xdr.mcp")  # 保持宿主 logger 名（观测兼容）


def register_routes(mcp) -> None:
    """显式注册 HTTP 任务 API 五端点（mcp_server 启动处调用，mcp.run 前）。
    （Task 1384 +/task/{id}/ack：done 总结消费回执。）

    契约：调用一次；🔴 非幂等（重复注册双路由）。
    """
    mcp.custom_route("/dispatch", methods=["POST"])(http_dispatch)
    mcp.custom_route("/task/{task_id}", methods=["GET"])(http_task_status)
    mcp.custom_route("/task_state", methods=["GET"])(http_task_state_list)
    mcp.custom_route("/cancel/{task_id}", methods=["POST"])(http_cancel)
    mcp.custom_route("/task/{task_id}/ack", methods=["POST"])(http_task_ack)


async def http_task_ack(request: Request) -> JSONResponse:
    """POST /task/{task_id}/ack — done 总结消费回执（Task 1384）。

    body: {"agent": 主控名, "key_points": ["复述小弟总结要点", ...]}
    校验：agent 非空；key_points ≥1 条非空；任务存在且 DONE。
    写入：state.acked=true + state.ack={agent, key_points, ts}。
    """
    try:
        task_id = request.path_params["task_id"]
        body = await request.json()
        agent = str(body.get("agent") or "").strip()
        key_points = body.get("key_points")
        if not agent:
            return JSONResponse(
                {"error": "agent 必填（ack 的主控名）"}, status_code=400)
        if not isinstance(key_points, list) or not [
            k for k in key_points if str(k).strip()
        ]:
            return JSONResponse(
                {"error": "key_points 至少 1 条非空（复述小弟总结再 ack）"},
                status_code=400)
        state = task_state.load_state(task_id)
        if state is None:
            return JSONResponse(
                {"error": f"任务不存在: {task_id}"}, status_code=404)
        if state.get("status") != "DONE":
            return JSONResponse(
                {"error": f"任务状态 {state.get('status')} 非 DONE——ack 仅面向 done 总结"},
                status_code=400)
        # 1646: 高风险单 ack 必须携带审读凭证（dispatch-orchestrator §5 机器化，
        # 同 D9 拦未 ack 的机制位）。仅新注册且命中风险词表的任务有 high_risk 字段
        # （历史任务无字段=零回溯）。audit_mem: 0 + audit_note = 主控确认低风险的
        # 逃生口（机器留痕不静默）。
        audit_mem = body.get("audit_mem")
        audit_note = str(body.get("audit_note") or "").strip()
        if state.get("high_risk") and (
                not isinstance(audit_mem, int) or isinstance(audit_mem, bool) or audit_mem < 0):
            return JSONResponse({
                "error": "高风险单 ack 需携带审读凭证 audit_mem（dispatch-orchestrator "
                         "§5）——int MEM 编号（Explore 全文审读产出）；确认实际低风险"
                         "用 audit_mem: 0 + audit_note 说明"}, status_code=400)
        task_state.update_field(task_id, "ack", {
            "agent": agent,
            "key_points": [str(k) for k in key_points],
            **({"audit_mem": audit_mem} if state.get("high_risk") else {}),
            **({"audit_note": audit_note} if audit_note else {}),
            "ts": task_state.now_iso(),
        })
        task_state.update_field(task_id, "acked", True)
        return JSONResponse({
            "ok": True, "task_id": task_id, "acked": True, "agent": agent,
            "key_points_n": len(key_points)})
    except Exception as e:  # noqa: BLE001
        logger.exception("HTTP /task/{id}/ack 异常")
        return JSONResponse({"error": str(e)}, status_code=500)



async def http_dispatch(request: Request) -> JSONResponse:
    """HTTP dispatch 端点（kimi 用 curl 调用）。

    Body: {"to": "kimi", "caller": "zcode", "task_path": "...",
           "timeout": 600 或 "timeout_sec": 600, "wait": false}
    timeout/timeout_sec 双键等价（timeout 优先）——池 #179（1656 根修）：
    调用方历史传键 timeout_sec（1460/1461/1649/1651/1656 五单台账 3600 实证），
    原实现只认 timeout → 永远默认 3600。非法值（非正整数）400 拒绝。
    """
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "无效 JSON"}, status_code=400)

    to = body.get("to")
    task_path = body.get("task_path")
    if not to or not task_path:
        return JSONResponse({"error": "缺少 to 或 task_path"}, status_code=400)

    raw_timeout = body.get("timeout")
    if raw_timeout is None:
        raw_timeout = body.get("timeout_sec", 3600)
    try:
        timeout = int(raw_timeout)
        if timeout <= 0:
            raise ValueError("非正数")
    except (TypeError, ValueError):
        return JSONResponse(
            {"error": f"timeout 非法（需正整数秒）: {raw_timeout!r}"},
            status_code=400,
        )

    try:
        result = await anyio.to_thread.run_sync(lambda: dispatchd.dispatch(
            to=to,
            task_path=task_path,
            caller=body.get("caller", "user"),
            timeout=timeout,
            wait=body.get("wait", False),
            notify_url=body.get("notify_url", ""),
            force=bool(body.get("force", False)),  # Task991: 忙碌硬检查越过出口
            suspend_until=body.get("suspend_until"),  # Task1543 #217: 挂起窗口（epoch 秒/ISO）
        ))
        return JSONResponse(result)
    except dispatchd.DispatchError as e:
        return JSONResponse({"error": e.message}, status_code=e.status_code)
    except Exception as e:
        logger.exception("HTTP dispatch 异常")
        return JSONResponse({"error": str(e)}, status_code=500)


async def http_task_status(request: Request) -> JSONResponse:
    """HTTP 查任务状态。"""
    task_id = request.path_params["task_id"]
    try:
        result = await anyio.to_thread.run_sync(lambda: dispatchd.check_status(task_id))
        # Task514: 轮询证据持久化（附属动作，失败不阻断查询）
        try:
            task_state.record_poll(task_id)
        except Exception as _pe:
            logger.warning(f"[Task514] record_poll 失败（不阻断查询）: {_pe}")
        return JSONResponse(result)
    except dispatchd.DispatchError as e:
        return JSONResponse({"error": e.message}, status_code=e.status_code)

async def http_task_state_list(request: Request) -> JSONResponse:
    """HTTP 任务台账列表（kimi curl fallback，补全 task_state 单一真相源的 HTTP 查询缺口）。

    返回 active（非终态）+ recent（最近 20，含终态）+ count。
    """
    try:
        def _ts_list():
            active = task_state.list_active()
            recent = task_state.list_recent(limit=20)
            return active, recent
        active, recent = await anyio.to_thread.run_sync(_ts_list)
        return JSONResponse({
            "active": active,
            "recent": recent,
            "count": {"active": len(active), "recent": len(recent)},
        })
    except Exception as e:
        logger.exception("HTTP /task_state 异常")
        return JSONResponse({"error": str(e)}, status_code=500)


async def http_cancel(request: Request) -> JSONResponse:
    """HTTP 取消任务。
    Task395 P3: cancel ≈ kill task, 需 user_consent（B7 代码化）。
    P3 fail-open: 无 user_consent 只 warn（P4 改 fail-closed）。
    """
    task_id = request.path_params["task_id"]
    try:
        body = await request.json()
    except Exception:
        body = {}
    if not body.get("user_consent"):
        logger.warning(f"[Task395] cancel {task_id} 无 user_consent（P3 fail-open, B7 代码化）")
    try:
        result = await anyio.to_thread.run_sync(lambda: dispatchd.cancel_task(task_id))
        return JSONResponse(result)
    except dispatchd.DispatchError as e:
        return JSONResponse({"error": e.message}, status_code=e.status_code)



# callback 域 — Task618 (#57 主线批) 子包化迁出 → nexusos_xdr/callback/
# 物理实现（http_callback + _bg_gate_check 线程 + 清算钩子）在
# nexusos_xdr/callback/handler.py；此处保留 shim re-export + 启动处显式注册。
from nexusos_xdr.callback import (  # noqa: E402  Task618 shim
    http_callback,
    register_routes as _callback_register_routes,
)

