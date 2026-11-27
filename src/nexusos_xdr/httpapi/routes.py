from nexusos_xdr.proc_origin import wrap as _origin_wrap
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
    mcp.custom_route("/dispatch", methods=["POST"])(
        _origin_wrap(http_dispatch, "dispatch", 19998))
    # 2611239006：服务端自查来历的只读状态（只记录阶段）
    mcp.custom_route("/origin_verified_stats", methods=["GET"])(http_origin_verified_stats)
    mcp.custom_route("/task/{task_id}", methods=["GET"])(http_task_status)
    mcp.custom_route("/task_state", methods=["GET"])(http_task_state_list)
    mcp.custom_route("/cancel/{task_id}", methods=["POST"])(
        _origin_wrap(http_cancel, "cancel", 19998))
    # P0-1 ACK 链（task 2611111762）：投递回执（执行者第一动作）+ steer 登记
    mcp.custom_route("/ack", methods=["POST"])(
        _origin_wrap(http_ack, "ack_delivery", 19998))
    mcp.custom_route("/steer", methods=["POST"])(http_steer_register)
    mcp.custom_route("/task/{task_id}/ack", methods=["POST"])(
        _origin_wrap(http_task_ack, "ack", 19998))
    # 2611239003：签收/延后两个开关的只读状态
    mcp.custom_route("/ack_policy_stats", methods=["GET"])(http_ack_policy_stats)


async def http_ack_policy_stats(request: Request) -> JSONResponse:
    """GET /ack_policy_stats — 签收归属与延后判定的只读状态（2611239003）。

    返回：两个开关的当前值 + 最近 24h 的「已核实签收 / 未核实签收 / 被拒 / 延后判定」
    计数与「因延后而待结的记录数」+ 「等签收的任务数」。只读，不改任何东西。
    """
    try:
        from nexusos_xdr import ack_policy as _ap
        hours = 24
        try:
            _qp = getattr(request, "query_params", None)
            _raw = _qp.get("hours") if _qp is not None else None
            if _raw:
                hours = max(1, min(int(_raw), 168))
        except Exception:  # noqa: BLE001 — 参数坏了按默认窗口
            hours = 24
        return JSONResponse(_ap.stats(hours=hours))
    except Exception as e:  # noqa: BLE001
        logger.warning(f"[2611239003] 状态接口异常: {type(e).__name__}: {e}")
        return JSONResponse({"ok": False, "error": f"状态接口异常: {type(e).__name__}"},
                            status_code=500)


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
        # === 2611239003：签收必须由派单的人（第二层）===
        _ack_via9003 = {}
        # 出示派单方自己的检索凭据（X-Credential）→ 持有者必须等于台账 caller 且 caller 在主控名单；
        # 派单方不是主控的任务（系统派单方）允许主控代签并留痕。自报名字只做展示。
        # 开关 ack_identity：record（默认，接受但记数）/ enforce（未核实 403）。每次请求读一个小文件。
        try:
            from nexusos_xdr import ack_policy as _ap9003
            _pol9003 = _ap9003.read_policy()
            _presented9003 = (request.headers.get("X-Credential") or "").strip() \
                or str(body.get("credential") or "").strip()
            _caller9003 = str(state.get("caller") or "").strip()
            _v9003, _d9003 = _ap9003.ack_verdict(_presented9003, _caller9003)
            _reject9003 = _ap9003.should_reject(_v9003, _pol9003["ack_identity"])
            _kind9003 = ("acked_verified" if _v9003 in ("verified_caller", "verified_delegate")
                         else ("acked_rejected" if _reject9003 else "acked_unverified"))
            _ap9003.record(_kind9003, task_id, _caller9003, f"{_v9003}: {_d9003}",
                           extra={"verdict": _v9003})
            _ack_via9003 = {"verdict": _v9003, "detail": _d9003}
            if _reject9003:
                return JSONResponse({
                    "ok": False, "rejected": True,
                    "reason": f"签收必须是派单方本人（{_v9003}: {_d9003}）",
                    "mode": _pol9003["ack_identity"],
                    "msg": "请求头带 X-Credential: <派单方自己的检索凭据>；派单方不是主控的任务可由主控代签",
                }, status_code=403)
        except Exception as _ap_e9003:  # noqa: BLE001
            # 2611239003：异常默认值落在「不放过」的一侧——能读到开关且是 enforce 就拒。
            _mode_fb9003 = "record"
            try:
                from nexusos_xdr import ack_policy as _ap_fb
                _mode_fb9003 = _ap_fb.read_policy()["ack_identity"]
            except Exception:  # noqa: BLE001 — 连开关都读不到：只能按 record（运维路径）
                _mode_fb9003 = "record"
            logger.warning(f"[2611239003] 签收归属闸异常（mode={_mode_fb9003}）: "
                           f"{type(_ap_e9003).__name__}: {_ap_e9003}")
            if _mode_fb9003 == "enforce":
                return JSONResponse({
                    "ok": False, "rejected": True,
                    "reason": f"签收归属闸异常（强制模式拒绝）: {type(_ap_e9003).__name__}",
                    "mode": _mode_fb9003,
                }, status_code=403)
            _ack_via9003 = {"verdict": "gate_error", "detail": str(_ap_e9003)[:120]}
        # 2611230868: ack 返回带「已采用、待给结果」曝光编号（最多 10 个）
        try:
            import psycopg2 as _pg868
            _c868 = _pg868.connect("host=127.0.0.1 port=5432 dbname=nexus_mem user=wang")
            _cur868 = _c868.cursor()
            from nexusos_xdr.task_aliases import task_id_aliases as _alias_868
            _aliases_868 = _alias_868(task_id, (state or {}).get("task_path", ""))
            _cur868.execute(
                "SELECT target_id FROM agent_activity "
                "WHERE action='apply_attempt' AND task_id = ANY(%s) "
                "AND detail->>'auto_judged' = 'adopted' "
                "AND (outcome IS NULL OR outcome='pending') LIMIT 10", (_aliases_868,))
            _pending_adopted = [r[0] for r in _cur868.fetchall()]
            _c868.close()
        except Exception:
            _pending_adopted = []
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
        # 2611239907：签收写 signoff 字段（原写 ack 字段把投递回执时刻覆盖掉——
        # 回执时刻已挪 delivery_receipt 字段（ack.py handle_ack），互不覆盖）。
        # acked 布尔含义不变（完成且已签收）；ack 字段纯做投递状态机。
        task_state.update_field(task_id, "signoff", {
            "agent": agent,
            # 2611239003：签收归属判定痕迹（verdict=verified_caller/verified_delegate/…）
            "ack_identity": _ack_via9003,
            "key_points": [str(k) for k in key_points],
            **({"audit_mem": audit_mem} if state.get("high_risk") else {}),
            **({"audit_note": audit_note} if audit_note else {}),
            "ts": task_state.now_iso(),
        })
        task_state.update_field(task_id, "acked", True)
        # === 2611239003：派单方是主控的任务，自动判定挪到签收后；一个任务只判一次 ===
        try:
            from nexusos_xdr import ack_policy as _ap2_9003
            _st2_9003 = task_state.load_state(task_id) or {}
            _caller2_9003 = str(_st2_9003.get("caller") or "").strip()
            if _ap2_9003.is_orchestrator(_caller2_9003):
                if _st2_9003.get("auto_judged_at"):
                    logger.info(f"[2611239003] {task_id} 自动判定已做过（{_st2_9003.get('auto_judged_at')}），签收不重复判")
                else:
                    from nexusos_xdr.callback.handler import auto_judge_task_exposures as _aj9003
                    _res9003 = await anyio.to_thread.run_sync(
                        lambda: _aj9003(task_id, _st2_9003.get("callee", "") or "", ""))
                    task_state.update_field(task_id, "auto_judged_at", task_state.now_iso())
                    _ap2_9003.record("judge_at_ack", task_id, _caller2_9003, "签收后自动判定",
                                     extra={"judged": int((_res9003 or {}).get("not_used", 0) or 0)})
                    logger.info(f"[2611239003] {task_id} 签收后自动判定完成: {_res9003}")
        except Exception as _aj_e9003:  # noqa: BLE001 — 判定失败不阻断签收（口径同回调侧）
            logger.warning(f"[2611239003] 签收后自动判定失败（不阻断签收）: "
                           f"{type(_aj_e9003).__name__}: {_aj_e9003}")
        # === 2611192130 J1: ack_consumed —— 主控消费回执落流（MEM#118115 空白③：
        # ack 此前只是 task_state 字段，消费动作在事件流无痕——24h 消费窗对账缺段）。
        # fire-and-forget：事件失败不影响 ack 主流程。
        try:
            from nexusos_xdr import session_state as _ss_j1
            _ss_j1.emit_system_event(
                agent, "ack_consumed",
                detail=f"{task_id} done 总结已被 {agent} 消费（key_points={len(key_points)}）",
                target=f"task:{task_id}",
            )
        except Exception:
            pass
        return JSONResponse({
            "ok": True, "task_id": task_id, "acked": True, "agent": agent,
            "key_points_n": len(key_points),
            "pending_adopted": _pending_adopted})
    except Exception as e:  # noqa: BLE001
        logger.exception("HTTP /task/{id}/ack 异常")
        return JSONResponse({"error": str(e)}, status_code=500)


async def http_ack(request: Request) -> JSONResponse:
    """POST /ack — 投递回执（P0-1 ACK 链，task 2611111762）。

    body: {"task_id": "...", "key": "ack-xxx", "agent": "omp", "kind": "dispatch"}
    kind=steer 时不需 task_id（key 即身份）。幂等：同键重复回执 ok+duplicate。
    与 /task/{id}/ack（Task1384 done 总结消费回执）是两个协议——本端点是
    投递确认（收到任务书即发），那个是完成验收（读完总结才发）。
    """
    try:
        try:
            body = await request.json()
        except Exception:
            return JSONResponse({"error": "无效 JSON"}, status_code=400)
        agent = str(body.get("agent") or "").strip()
        kind = str(body.get("kind") or "dispatch").strip()
        key = str(body.get("key") or "").strip()
        if not agent:
            return JSONResponse({"error": "agent 必填（回执的执行者名）"}, status_code=400)
        from nexusos_xdr import ack as ack_mod
        if kind == "steer":
            if not key:
                return JSONResponse({"error": "kind=steer 时 key 必填"}, status_code=400)
            return JSONResponse(ack_mod.handle_steer_ack(key, agent))
        task_id = str(body.get("task_id") or "").strip()
        if not task_id:
            return JSONResponse({"error": "task_id 必填（kind=dispatch）"}, status_code=400)
        return JSONResponse(ack_mod.handle_ack(task_id, key, agent, kind))
    except FileNotFoundError as e:
        return JSONResponse({"error": str(e)}, status_code=404)
    except PermissionError as e:
        return JSONResponse({"error": str(e)}, status_code=400)
    except ValueError as e:
        return JSONResponse({"error": str(e)}, status_code=400)
    except Exception as e:  # noqa: BLE001
        logger.exception("HTTP /ack 异常")
        return JSONResponse({"error": str(e)}, status_code=500)


async def http_steer_register(request: Request) -> JSONResponse:
    """POST /steer — steer 投递登记（协议层，task 2611111762）。

    body: {"agent": "kimi", "note": "纠偏 xxx"}→ {steer_key, ack_deadline, ack_cmd}。
    词法层（消费验证）在 steer.sh（task/2611111761 修），本端点供其/主控登记
    幂等键并获取回执命令——steer 链的可考古落账（#615/2010 教训）。
    """
    try:
        try:
            body = await request.json()
        except Exception:
            return JSONResponse({"error": "无效 JSON"}, status_code=400)
        agent = str(body.get("agent") or "").strip()
        note = str(body.get("note") or "").strip()
        if not agent:
            return JSONResponse(
                {"error": "agent 必填（steer 目标 session 名）"}, status_code=400)
        from nexusos_xdr import ack as ack_mod
        return JSONResponse(ack_mod.register_steer(agent, note))
    except Exception as e:  # noqa: BLE001
        logger.exception("HTTP /steer 异常")
        return JSONResponse({"error": str(e)}, status_code=500)




async def http_origin_verified_stats(request: Request) -> JSONResponse:
    """GET /origin_verified_stats — 服务端自查来历的只读状态（2611239006，只记录阶段）。

    返回：进程启动以来的计数（按入口 × 来历类别 × 是否对得上）+ 最近 20 条对不上的样例。
    只读；不含任何凭据号（样例里只有入口、自报名字、来历类别/会话、原因、端口与持有者进程号）。
    """
    try:
        from nexusos_xdr import proc_origin as _po
        return JSONResponse(_po.stats())
    except Exception as e:  # noqa: BLE001
        logger.warning(f"[2611239006] 状态接口异常: {type(e).__name__}: {e}")
        return JSONResponse({"ok": False, "error": f"状态接口异常: {type(e).__name__}"},
                            status_code=500)


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
        # 2611172060 M4 数据面：派发拒绝（409 锁冲突/忙碌等）落带时间戳应用日志
        # （uvicorn 访问行无时间戳，窗口计数不可用）——nexus-monitor C3 消费此行。
        logger.warning(f"[dispatch-reject] HTTP {e.status_code}: {e.message[:160]}")
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


def _blocked_summary() -> dict:
    """Task 2611230853 件三：被硬拦（end_reason=gate_blocked）的总数 / 超 48h 数 / 最老小时数。

    不改状态枚举：被拦任务的状态字段仍是 DONE，靠 end_reason 显形。
    数据源 = list_recent 的大窗口扫描（单次落盘为主控手触发，量级可接受）。
    """
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    rows = task_state.list_recent(limit=5000) or []
    blocked = [r for r in rows if str(r.get("end_reason") or "") == "gate_blocked"]
    over, oldest = 0, 0.0
    for r in blocked:
        ts = r.get("finished") or r.get("created") or ""
        try:
            dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            hours = (now - dt).total_seconds() / 3600.0
        except Exception:
            continue
        oldest = max(oldest, hours)
        if hours >= 48:
            over += 1
    return {"total": len(blocked), "over_48h": over, "oldest_hours": round(oldest, 1)}

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
        try:
            _blocked = await anyio.to_thread.run_sync(_blocked_summary)
        except Exception:
            _blocked = {"total": 0, "over_48h": 0, "oldest_hours": 0}
        return JSONResponse({
            "active": active,
            "recent": recent,
            "count": {"active": len(active), "recent": len(recent)},
            # Task 2611230853 件三：硬拦显形（不改状态枚举，只加汇总字段）
            "blocked_total": _blocked.get("total", 0),
            "blocked_over_48h": _blocked.get("over_48h", 0),
            "blocked_oldest_hours": _blocked.get("oldest_hours", 0),
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

