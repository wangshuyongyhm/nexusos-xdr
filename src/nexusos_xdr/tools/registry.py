#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""nexusos_xdr.tools.registry — MCP 工具域（Task620 #57 自 mcp_server.py 迁入）。

物理迁入（行为零变化，T609 纪律）：18 个 @mcp.tool 工具 + 域内辅助
（_forward_to_ocrx / _issue_search_cred）。

架构（T604 三要素）：
    - register_tools(mcp, get_kb) 显式注册（mcp_server 启动处调用，mcp.run 前）。
    - 依赖注入：get_kb（知识库单例工厂，mcp_server 域留守）经参数注入——
      不反向 import mcp_server（依赖方向 mcp_server → tools 单向）。
    - 陷阱2 合规：工具 docstring（MCP schema 来源）逐字保留。

1417（MCP L0+L1）：注册处包 _mcp_error_wrapper——业务函数返回 {"error":...}
字典（HTTP 语义）在 MCP 层抛 ToolError（isError=true 标准错误结构）。
分层可见性由 mcp_auth.nexus_mcp_auth（AuthMiddleware）负责，本文件只管
错误语义归一与注册。

logger 名保持 nexusos-xdr.mcp（观测兼容优先，同 T618 裁决）。
"""
import functools
import inspect
import json
import logging
import os
import re
import sys
import threading
import urllib.request
from typing import Optional

from nexusos_common import subpath  # 1960：锚点单源（design §2.1 行9b）

from fastmcp.exceptions import ToolError

from nexusos_xdr import dispatchd
from nexusos_xdr import session_state
from nexusos_xdr import task_state
from nexusos_xdr._vault_env import VAULT_ROOT  # Task668: 包内自足（干净环境可解析）

# Task639 X1: 包路径 import（经 nexusos_xdr/__init__ 的 __path__ append 恒可达），
# 不再依赖"mcp_server 先 import"的 sys.path 副作用——裸名 import 在子包被单独
# 首载时走降级分支、gate_check 工具返回 error（fail-open）。包真损时 ImportError
# 响亮失败（fail-closed）。
from nexusos_gate.gate_engine import check as gate_engine_check

logger = logging.getLogger("nexusos-xdr.mcp")  # 保持宿主 logger 名（M8 观测兼容）


def _mcp_error_wrapper(fn):
    """MCP 层错误语义归一（1417 gap①）：业务函数返回 {"error": ...} 字典
    （HTTP 语义）→ 抛 ToolError，FastMCP 转 isError=true 的标准 MCP 错误结构，
    外部客户端可编程判定失败。HTTP 侧不经此 wrapper（HTTP 路由直接调域模块），
    零回归。__signature__/__doc__ 保留——FastMCP 从签名+docstring 派生 schema。
    """
    @functools.wraps(fn)
    def wrapped(*args, **kwargs):
        result = fn(*args, **kwargs)
        if isinstance(result, dict) and result.get("error"):
            raise ToolError(str(result["error"]))
        return result
    wrapped.__signature__ = inspect.signature(fn)  # schema 派生需要原签名
    return wrapped


def _mcp_dispatch_wrapper(fn):
    """dispatch 专用 wrapper（1420 L2）：wait=False 硬编码——MCP 客户端调用路径
    禁同步等待：wait=True 会同步阻塞最长 3600s 占死 xdr 线程池槽（1411 report
    自承风险），MCP 调用一律异步立返 task_id，进度经 check_task_status 拉。
    错误归一同 _mcp_error_wrapper。
    """
    @functools.wraps(fn)
    def wrapped(*args, **kwargs):
        kwargs["wait"] = False  # 硬编码：MCP 调用禁同步等待（外部客户端无阻塞语义）
        result = fn(*args, **kwargs)
        if isinstance(result, dict) and result.get("error"):
            raise ToolError(str(result["error"]))
        return result
    wrapped.__signature__ = inspect.signature(fn)  # schema 派生需要原签名
    return wrapped

# 注入位（register_tools 时绑定；get_kb/_kb_lock 为 mcp_server 域知识库
# 单例工厂与线程安全锁——psycopg2 连接非线程安全，MCP 并发调用需同一把锁序列化）
_get_kb = None
_kb_lock = None


def register_tools(mcp, get_kb=None, kb_lock=None) -> None:
    """在 mcp 实例上显式注册全部 18 个 MCP 工具。

    契约：mcp_server 启动处调用一次（mcp.run 前）；🔴 非幂等（重复注册
    双份工具），调用方保证单次。并发语义：仅注册期执行（单线程阶段）。

    Args:
        mcp: FastMCP 实例（宿主）
        get_kb: 知识库单例工厂注入（mcp_server.get_kb——线程安全锁在其侧）
        kb_lock: 线程安全锁（🔴 T628 T1: 必传——忘传静默新锁会让 MCP 工具
            与 api_server 域两把锁并发写同一 PG 连接池，RMW 竞态。缺省
            兜底已删，未传即 TypeError 显式报错优于静默分叉）
    """
    global _get_kb, _kb_lock
    if kb_lock is None:
        raise TypeError(
            "register_tools(mcp, get_kb, kb_lock) —— kb_lock 必传（T628 T1："
            "静默兜底新锁=两把锁并发写 PG，显式报错优于静默分叉）"
        )
    _get_kb = get_kb
    _kb_lock = kb_lock

    mcp.tool()(_mcp_dispatch_wrapper(dispatch))
    mcp.tool()(_mcp_error_wrapper(search_knowledge))
    mcp.tool()(_mcp_error_wrapper(apply_experience_outcome))
    mcp.tool()(_mcp_error_wrapper(write_knowledge))
    mcp.tool()(_mcp_error_wrapper(request_approval))
    mcp.tool()(_mcp_error_wrapper(check_task_status))
    mcp.tool()(_mcp_error_wrapper(cancel_task))
    mcp.tool()(_mcp_error_wrapper(capture_screen))
    mcp.tool()(_mcp_error_wrapper(detect_popup_visual))
    mcp.tool()(_mcp_error_wrapper(analyze_ui_state))
    mcp.tool()(_mcp_error_wrapper(verify_mcp_config))
    mcp.tool()(_mcp_error_wrapper(check_last_mile))
    mcp.tool()(_mcp_error_wrapper(presence_register))
    mcp.tool()(_mcp_error_wrapper(presence_active))
    mcp.tool()(_mcp_error_wrapper(presence_heartbeat))
    mcp.tool()(_mcp_error_wrapper(presence_events))
    mcp.tool()(_mcp_error_wrapper(presence_declare_op))
    mcp.tool()(_mcp_error_wrapper(gate_check))

# ============================================================================
# 6 核心工具
# ============================================================================

def dispatch(
    to: str,
    task_path: str,
    caller: str = "user",
    timeout: int = 3600,
    wait: bool = False,
    notify_url: str = "",
) -> dict:
    """派任务给另一个 Agent。

    Args:
        to: 目标 Agent 名（oc/omp/kimi/ds/ds2/ac）
        task_path: 任务书文件绝对路径
        caller: 调用方 Agent 名（审批归属，默认 user）
        timeout: 超时秒数（默认 3600=60分钟，T674 从 1800 调大）
        wait: True=同步等待完成，False=异步立即返回 task_id（默认）。
            🔴 Task639 T2: wait=True 为普通 def 直调——FastMCP sync 工具在独立
            worker 线程跑（不冻结事件循环），但最长阻塞 timeout 秒（默认 3600s），
            期间占用线程池 1 槽。高并发 wait=True 有线程池耗尽风险；根治（anyio
            包装 async 化）在 backlog，当前用量（单主控串行派发）不触发。
        notify_url: 任务完成/异常时的 webhook 回调 URL（场景5）。patrol 会 POST {task_id,status,result}
    Returns:
        {"task_id": "...", "status": "...", "callee": "...", ...}
    """
    logger.info(f"MCP dispatch: {caller} → {to}, task={task_path}, notify={notify_url or '无'}")
    try:
        result = dispatchd.dispatch(to, task_path, caller, timeout, wait=wait, notify_url=notify_url)
        # Task543: dispatch 成功写专用事件 type（观测器精确匹配，替代关键词近似识别）
        if isinstance(result, dict) and result.get("task_id"):
            try:
                session_state.append_event(
                    caller, "dispatch_sent",
                    detail=f"task_id={result.get('task_id')} target={to} task={task_path}",
                    target=to,
                )
            except Exception as _ev:
                logger.warning(f"[Task543] dispatch_sent event 写入失败（不阻塞）: {_ev}")
        return result
    except dispatchd.DispatchError as e:
        return {"error": e.message, "status_code": e.status_code}
    except Exception as e:
        logger.exception("dispatch 异常")
        return {"error": str(e)}


def search_knowledge(query: str, top_k: int = 5, project: str = "", task_id: str = "", agent_id: str = "") -> dict:
    """搜索共享知识库（nexusos-mem）。

    Args:
        query: 搜索词
        top_k: 返回条数（默认5）
        project: 可选项目过滤（项目名，如 nexusos-mem/system）
        task_id: 可选任务 ID（Task 319: 关联本次 search 到具体任务，供 Phase 1 自动检测套用）
        agent_id: 归因 Agent 名（Task531 B3: 不传用 nexusos-xdr 单例——G5 按 callee
                  查不到；传真实名让 apply_attempt 归因正确）
    Returns:
        {"query": "...", "count": N, "results": [...]}
    """
    try:
        # Task531 B3: 归因正确性——agent_id 优先新实例（各持连接，短生命周期），
        # 否则 _get_kb() 单例归因 nexusos-xdr。search 是短操作，新实例成本可接受。
        from nexusos_mem.shared import SharedKnowledge
        with _kb_lock:
            _kb_b3 = SharedKnowledge(agent_id=agent_id or "nexusos-xdr")
            # Task526: task_id 非空时自动 annotate（对齐 HTTP /search 带 agent_id
            # 自动 annotate 的语义）——小弟执行 task 时的搜索=套用尝试记录，
            # apply_attempt 行带 task_id 供 done gate G10（Task528）按任务查回填。
            results = _kb_b3.search(
                query,
                top_k=top_k,
                project=project or None,
                task_id=task_id or None,
                annotate=bool(task_id),
            )
        return {
            "query": query,
            "count": len(results),
            "results": [
                {
                    "id": r.get("id"),
                    "title": r.get("title"),
                    "content": (r.get("content") or "")[:500],
                    "shadow_code": r.get("shadow_code"),
                    "score": r.get("final_score") or r.get("rank"),
                    "project": r.get("project"),
                    "knowledge_type": r.get("knowledge_type"),
                    "snippet": r.get("snippet"),
                    # Task531 B2: 回传 apply_attempt_id（shared.search annotate 时写入
                    # row——主控回填 outcome 要用，原响应构造丢弃了该字段）
                    "apply_attempt_id": r.get("apply_attempt_id"),
                    # Task532 P-m2: 校准相关字段透传（原白名单丢弃——主控判断经验可信度要用）
                    "outcome": r.get("outcome"),
                    "calibration_weight": r.get("calibration_weight"),
                    "confidence_label": r.get("confidence_label"),
                }
                for r in results
            ],
        }
    except Exception as e:
        logger.exception("search_knowledge 异常")
        return {"error": str(e)}


def apply_experience_outcome(
    apply_attempt_id: int,
    exp_id: int,
    result: str,
    evidence: str = "",
    agent_id: str = "",
) -> str:
    """主控套用经验后回填真实结果（认知诚实闭环关键，B+ 方案 Task527）。

    覆盖 callback 写的 pending 占位值。主控验收 task 后，对套用过的经验
    调此工具回填 success/partial/failed。

    Args:
        apply_attempt_id: 对应的 apply_attempt 记录 id（search annotate 时写的 agent_activity id）
        exp_id: 被套用的经验 knowledge.id
        result: success / partial / failed（真实结果）
        evidence: 结果证据（failed 时作为反例内容）
        agent_id: 回填 agent（默认从 session 推导）

    Returns:
        JSON {ok, result, exp_id, counterexample_created}
    """
    try:
        _src = agent_id or "mcp"
        # SharedKnowledge 顶层未 import（get_kb 模式是函数内局部 import）
        from nexusos_mem.shared import SharedKnowledge
        # 场景8: record_apply_outcome 是 DB 写路径，与 _kb_lock 保护的
        # add_knowledge 并发不安全——对齐 write_knowledge 的锁模式
        with _kb_lock:
            _kb = SharedKnowledge(agent_id=_src)
            _r = _kb.record_apply_outcome(
                apply_attempt_id=apply_attempt_id,
                exp_id=exp_id,
                result=result,
                evidence=evidence,
            )
        return json.dumps(_r, ensure_ascii=False)
    except ValueError as e:
        return json.dumps({"ok": False, "error": str(e)}, ensure_ascii=False)
    except Exception as e:
        logger.exception("apply_experience_outcome 异常")
        return json.dumps({"ok": False, "error": str(e)}, ensure_ascii=False)


def write_knowledge(
    title: str,
    content: str,
    knowledge_type: str = "experience",
    tags: Optional[list] = None,
    project: Optional[str] = None,
    verification_source: str = "tool_verification",
) -> dict:
    """写经验/发现到共享知识库。done 前必须调用。

    Args:
        title: 一句话标题
        content: 详细内容
        knowledge_type: experience/finding/decision/pitfall（默认 experience）
        tags: 关键词列表
        project: 项目名
        verification_source: tool_verification/cross_ref/user_asserted（默认 tool_verification）
        knowledge_type 合法值限定：experience/finding/decision/pitfall/verification/
            document（1421 QA 实测非法枚举被静默接受——补枚举校验，非法值返回 error）
    Returns:
        {"id": N, "shadow_code": "XX", "deduped": bool}
    """
    _VALID_TYPES = {"experience", "finding", "decision", "pitfall", "verification", "document"}
    if knowledge_type not in _VALID_TYPES:
        return {"error": f"knowledge_type 非法: {knowledge_type}（合法值: {sorted(_VALID_TYPES)}）"}
    kb = _get_kb()
    if kb is None:
        return {"error": "知识库不可用"}

    # Task 280 P1-18: 删除 execute_guard_global(content) 调用——
    # guards 用于校验 dispatch 的 shell 命令（防 rm -rf 等危险操作），
    # 不应校验知识库 content（content 里含"不要用 rm -rf"这种说明性文字会被误伤）。
    # 知识库 content 是数据不是命令，无需 guards 拦截。

    try:
        # 场景8: 加锁序列化 DB 操作
        with _kb_lock:
            kid = kb.add_knowledge(
                title=title,
                content=content,
                knowledge_type=knowledge_type,
                tags=tags,
                project=project,
                confidence=1.0 if verification_source == "tool_verification" else 0.7,
                verification_source=verification_source,
                embed=True,
            )
            # 取 shadow_code（add_knowledge 内部已自动补码，但要查一下）
            entry = kb.get_knowledge(kid)
        shadow = entry.get("shadow_code") if entry else None
        deduped = entry and entry.get("content_hash") is not None
        return {
            "id": kid,
            "shadow_code": shadow,
            "deduped": bool(deduped),
        }
    except Exception as e:
        logger.exception("write_knowledge 异常")
        return {"error": str(e)}


def request_approval(operation: str, reason: str, caller: str = "user") -> dict:
    """请求审批（由调用方 Agent 处理，不弹给用户）。

    Args:
        operation: 要执行的操作描述
        reason: 为什么需要这个操作
        caller: 调用方 Agent 名（审批归属）
    Returns:
        {"approved": true/false/null, "by": "...", "warnings": [...]}
    """
    return dispatchd.request_approval(operation, reason, caller)


def check_task_status(task_id: str) -> dict:
    """查任务状态。

    Args:
        task_id: 任务 ID
    Returns:
        {"task_id": "...", "status": "running/done/failed/timeout/cancelled", "result": {...}}
    """
    try:
        result = dispatchd.check_status(task_id)
        # Task514: 轮询证据持久化（附属动作，失败不阻断查询）
        try:
            task_state.record_poll(task_id)
        except Exception as _pe:
            logger.warning(f"[Task514] record_poll 失败（不阻断查询）: {_pe}")
        return result
    except dispatchd.DispatchError as e:
        return {"error": e.message, "status_code": e.status_code}


def cancel_task(task_id: str) -> dict:
    """取消任务。发送 Ctrl+C + 保存 checkpoint。

    v0.4: Ctrl+C 可能中断写操作致文件损坏，cancel 前写 checkpoint。

    Args:
        task_id: 任务 ID
    Returns:
        {"task_id": "...", "status": "CANCELLED", "message": "..."}
    """
    try:
        return dispatchd.cancel_task(task_id)
    except dispatchd.DispatchError as e:
        return {"error": e.message, "status_code": e.status_code}


# ============================================================================
# 3 视觉预留工具（v0.4 降级文本启发式，未来 nexus-vision 就绪后转发）
# ============================================================================

def capture_screen(agent: str) -> dict:
    """截取画面。

    agent="mumu" → 转发 nexus-ocrx 获取 MuMu Android 截图（真实视觉）
    其他 agent → tmux capture-pane 文本（降级）

    Args:
        agent: "mumu" 或 Agent 名（oc/kimi/ac...）
    Returns:
        MuMu: {"method": "ocrx", "screenshot_url": "...", "ocr_elements": [...]}
        TUI: {"agent": "...", "tmux_pane_text": "...", "method": "text_fallback"}
    """
    # MuMu 画面 → 转发 ocrx
    if agent.lower() in ("mumu", "emulator", "android"):
        return _forward_to_ocrx("/screenshot", method="GET")

    from nexusos_xdr.agents import get_agent
    a = get_agent(agent)
    if a is None:
        return {"error": f"未知 Agent: {agent}"}
    text = a.capture_pane(lines=50)
    return {
        "agent": agent,
        "tmux_pane_text": text,
        "method": "text_fallback",
    }


def _forward_to_ocrx(path: str, method: str = "POST", data: dict = None) -> dict:
    """转发请求到 nexus-ocrx (11438)。ocrx 不可用时降级。"""
    import urllib.request
    url = f"http://127.0.0.1:11438{path}"
    try:
        if method == "GET":
            req = urllib.request.Request(url)
        else:
            payload = json.dumps(data or {}, ensure_ascii=False).encode()
            req = urllib.request.Request(url, data=payload,
                                         headers={"Content-Type": "application/json"}, method=method)
        with urllib.request.urlopen(req, timeout=30) as resp:
            ct = resp.headers.get("Content-Type", "")
            if "json" in ct:
                return {"method": "ocrx", **json.loads(resp.read())}
            elif "image" in ct:
                # PNG 图片，返回大小+OCR
                png = resp.read()
                # 顺带做 OCR
                try:
                    ocr_req = urllib.request.Request(f"http://127.0.0.1:11438/ocr", method="POST")
                    ocr_req.add_header("Content-Type", "application/json")
                    with urllib.request.urlopen(ocr_req, timeout=30) as ocr_resp:
                        ocr = json.loads(ocr_resp.read())
                        return {"method": "ocrx", "screenshot_bytes": len(png), "ocr": ocr}
                except Exception:
                    return {"method": "ocrx", "screenshot_bytes": len(png)}
            return {"method": "ocrx", "raw": resp.read()[:200]}
    except urllib.error.URLError:
        return {"error": "nexus-ocrx 不可用（11438 未运行）", "fallback": "text_only"}
    except Exception as e:
        return {"error": f"ocrx 转发失败: {e}"}


def detect_popup_visual(agent: str) -> dict:
    """视觉检测弹窗。

    agent="mumu" → 转发 ocrx VLM 分析（真实视觉检测）
    其他 → 关键词检测（check_popup）

    Args:
        agent: "mumu" 或 Agent 名
    Returns:
        {"method": "ocrx_vlm"|"keyword_fallback", "detected": bool, ...}
    """
    if agent.lower() in ("mumu", "emulator", "android"):
        return _forward_to_ocrx("/analyze", data={
            "prompt": "这个画面是否有弹窗/对话框/确认框？如果有，描述弹窗内容和按钮位置。"
        })

    from nexusos_xdr.agents import get_agent
    a = get_agent(agent)
    if a is None:
        return {"error": f"未知 Agent: {agent}"}
    detected = a.check_popup()
    return {
        "agent": agent,
        "method": "keyword_fallback",
        "detected": detected,
    }


def analyze_ui_state(agent: str) -> dict:
    """分析 UI 状态。

    agent="mumu" → 转发 ocrx VLM 分析（画面理解）
    其他 → tmux 文本启发式

    Args:
        agent: "mumu" 或 Agent 名
    Returns:
        {"state": "idle|working|waiting_approval|stuck|crashed", ...}
    """
    if agent.lower() in ("mumu", "emulator", "android"):
        return _forward_to_ocrx("/analyze", data={
            "prompt": "分析这个游戏画面的状态：是登录界面/加载中/主城/战斗/弹窗？当前应该做什么操作？"
        })

    from nexusos_xdr.agents import get_agent
    a = get_agent(agent)
    if a is None:
        return {"error": f"未知 Agent: {agent}"}

    if not a.has_session():
        return {"agent": agent, "state": "crashed", "confidence": 0.9, "evidence": "tmux session 不存在"}

    tail = a.capture_pane(lines=5)
    is_idle = a.is_idle()

    if is_idle:
        state, confidence = "idle", 0.7
    elif a.check_popup():
        state, confidence = "waiting_approval", 0.8
    else:
        state, confidence = "working", 0.8

    return {
        "agent": agent,
        "state": state,
        "confidence": confidence,
        "evidence": tail[-200:] if tail else "(empty)",
        "method": "text_heuristic",  # 未来: "vlm"
    }


# ============================================================================
# 场景7: MCP 配置验证工具
# ============================================================================

def verify_mcp_config(agent: str = "") -> dict:
    """验证小弟的 nexusos-xdr MCP 配置是否生效。

    场景7: 小弟配了 MCP 但没重启时配置不生效。此工具供小弟启动后自检。

    Args:
        agent: 调用方 Agent 名（可空，用于标识谁在自检）
    Returns:
        {"connected": bool, "tools_visible": N, "self_test": "pass/fail"}
    """
    # 如果小弟能调到这个工具，说明 MCP 已连接——本身就是验证通过
    return {
        "connected": True,
        "tools_visible": 18,  # Task532 P-m1: 17+1(apply_experience_outcome Task527)。
        # 原 C2 修复注释: 6核心+3视觉+1verify+1check_last_mile+1gate_check+5PRESENCE=17
        "self_test": "pass",
        "agent": agent or "unknown",
        "message": "MCP 连接正常。如果之前配了 MCP 但工具不可见，需要重启 TUI 进程。",
    }


def check_last_mile(agent: str = "") -> dict:
    """检查 external/主控 Agent 最后一公里互感状态（init §2.7）。

    检查 AGENT_REGISTRY.on_mention、脚本存在性、xdr broadcast_router 是否加载。
    Cursor 额外检查 hooks.json 与 pending 文件。

    Args:
        agent: 你的 agent 名（cursor/zcode/kimiapp/空=全量 external 审计）
    """
    import json as _json
    from pathlib import Path as _Path

    result: dict = {"ok": True, "broadcast_router_loaded": False, "agents": {}}
    try:
        from nexusos_xdr.broadcast_router import route_broadcast_event  # noqa: F401
        result["broadcast_router_loaded"] = True
    except Exception as exc:
        result["broadcast_router_loaded"] = False
        result["router_error"] = str(exc)
        result["ok"] = False
        result["action"] = "launchctl kickstart -k gui/$(id -u)/ai.nexus.xdr"

    try:
        from nexusos_xdr.agents import AGENT_REGISTRY, audit_external_last_mile
        audit = audit_external_last_mile()
        if agent:
            audit = {agent: audit.get(agent, {"status": "unknown"})}
        result["agents"] = audit

        if agent == "cursor":
            # 1960 双拆（design §2.1 行9/9b）：hooks.json 是 Cursor 产品自身配置
            # 原位保留（参数化）；xdr 自有队列进 .nexus/xdr/agents/cursor/
            hooks = _Path(os.environ.get("NEXUS_XDR_CURSOR_CONFIG")
                          or _Path.home() / ".cursor" / "hooks.json")
            pending = _Path(os.environ.get("NEXUS_XDR_AGENT_DIR")
                            or subpath("xdr/agents")) / "cursor" / "broadcast-pending.jsonl"
            result["cursor_runtime"] = {
                "hooks_json": hooks.is_file(),
                "pending_file": pending.is_file(),
                "pending_lines": sum(
                    1 for ln in pending.read_text(encoding="utf-8").splitlines() if ln.strip()
                ) if pending.is_file() else 0,
            }
            if hooks.is_file():
                try:
                    hj = _json.loads(hooks.read_text(encoding="utf-8"))
                    result["cursor_runtime"]["hooks_session_start"] = bool(
                        hj.get("hooks", {}).get("sessionStart")
                    )
                    result["cursor_runtime"]["hooks_stop"] = bool(
                        hj.get("hooks", {}).get("stop")
                    )
                except Exception:
                    result["cursor_runtime"]["hooks_parse_error"] = True

        missing = [n for n, v in audit.items() if v.get("status") != "ready"]
        if missing:
            result["ok"] = False
            result["missing_or_partial"] = missing
            result["action"] = result.get(
                "action",
                "实现 on_mention + 边缘 Hook，见 XDR最后一公里-外部Agent互感适配.md",
            )
    except Exception as exc:
        result["ok"] = False
        result["audit_error"] = str(exc)

    return result


# ============================================================================
# NEXUS-PRESENCE: MCP 工具（MCP 客户端如 kimi.app/Cursor 可直接调）
# 与 HTTP /session/* 端点共用 session_state 底层，身份校验统一
# ============================================================================

def _issue_search_cred(agent: str, role: str) -> Optional[str]:
    """签发 search_cred（fail-open）。Task472。

    MEM 是所有 Agent 共用的能力，凭据机制验证事件完整性非访问限制。
    所有 Agent（主控+小弟）register 时都该拿到 search_cred。

    Task537 修复1: 签发后写入 05-System/Agent/{agent}/search_cred.txt，
    让 mem_first_guard.py:54 读到新鲜 cred（原 .txt 是手工死件，过期后 search 403）。
    """
    try:
        from nexusos_xdr.credential import issue_credential
        cred_id = issue_credential(
            cred_type="search_cred",
            bearer=agent,
            prerequisites={"role": role},
        )
        # Task537 修复1: 刷新 .txt 文件（每次 register 覆盖旧值）
        if cred_id:
            _cred_file = VAULT_ROOT / "05-System" / "Agent" / agent / "search_cred.txt"
            _cred_file.parent.mkdir(parents=True, exist_ok=True)
            _cred_file.write_text(cred_id, encoding="utf-8")
        return cred_id
    except Exception as _e:
        logger.debug(f"[Task472] search_cred 签发失败（fail-open）: {_e}")
        return None

def presence_register(
    agent: str,
    role: str = "executor",
    session_id: str = "",
    current_task: str = "",
    status: str = "init",
    reg_kind: str = "agent",
) -> dict:
    """注册到 NEXUS 在场感知（init 时调用）。

    让其他在线 Agent 知道你在干什么。多主控并存时靠此互感。

    Args:
        agent: 你的工具名（zcode/kimi/oc/omp/ds/ds2/ac/hermes/kimiapp）
        role: 角色（orchestrator=主控 / executor=执行）
        session_id: 会话标识（如 zcode-07291726），用于按会话追踪。C3 修复(2026-07-29): 原缺此参数致 MCP Agent session_id 永空
        current_task: 当前任务简述
        status: 状态（init/working/idle/blocking/done）
        reg_kind: 注册类别（Task543: "agent"=真注册→事件 type=init；
                  "router"=路由器心跳式重注册→事件 type=router_register，不要求配 init gate）
    Returns:
        {"ok": true, "agent": "...", "active": ["zcode", "kimi"]}
    """
    try:
        sessions = session_state.register_agent(agent, role=role, session_id=session_id, current_task=current_task, status=status)
        # Task543: reg_kind=router 的重注册写专用 type（观测器不再当 init 信号误报漏调）
        _evt_type_543 = "router_register" if reg_kind == "router" else "init"
        # 1636 活④: 事件透传 session_id（extra 顶层合并）——与 HTTP 入口对齐，结构性豁免依据
        session_state.append_event(agent, _evt_type_543, detail=f"MCP注册: role={role}, task={current_task}",
                                   extra={"session_id": session_id or ""})
        # Task 469: 和 HTTP register 对齐——注入 init_context（约束+坑+经验），让小弟也受益
        init_context = {}
        try:
            from nexusos_xdr.exp_injector import inject_for_init
            init_context = inject_for_init(agent, role=role)
        except Exception as _ie:
            logger.debug(f"[exp_injector] MCP init 注入失败（不阻塞）: {_ie}")
        _search_cred = _issue_search_cred(agent, role)
        return {
            "ok": True, "agent": agent, "active": list(sessions.keys()),
            "init_context": init_context,
            **({"search_credential_id": _search_cred} if _search_cred else {}),
        }
    except ValueError as e:
        return {"error": str(e)}


def presence_active() -> dict:
    """查询所有活跃 Agent（互感核心——看还有谁在线）。

    Returns:
        {"active_count": N, "agents": {name: {role, status, current_task, ...}}}
    """
    active = session_state.get_active_sessions()
    return {"active_count": len(active), "agents": active}


def presence_heartbeat(
    agent: str,
    status: str = "working",
    current_task: str = "",
    context_pct: int = 0,
) -> dict:
    """心跳更新（每 60s 调用，保持在线状态）。

    Args:
        agent: 你的工具名
        status: 当前状态（working/idle/blocking）
        current_task: 当前在干什么
        context_pct: 上下文占用百分比（Task532 P-m3: 对齐 HTTP 端点）
    Returns:
        {"ok": true, "heartbeat_at": "..."}
    """
    try:
        session_state.heartbeat_agent(agent, status=status, current_task=current_task, context_pct=context_pct)
        return {"ok": True, "agent": agent, "heartbeat_at": session_state.now_iso()}
    except ValueError as e:
        return {"error": str(e)}


def presence_events(since: str = "", limit: int = 50, agent_filter: str = "") -> dict:
    """拉取事件流（增量轮询，看全天所有 Agent 活动）。

    Args:
        since: ISO 时间戳，只返回此时间之后的事件（空=全部）
        limit: 最多返回条数（默认50；上限500——1421 QA 实测 999999 超大响应
            导致客户端流断，加帽防响应失控）
        agent_filter: 只看某 Agent 的事件（Task532 P-m4: 对齐 HTTP 端点）
    Returns:
        {"count": N, "events": [{ts, agent, type, detail}, ...]}
    """
    limit = max(1, min(limit, 500))  # 1421 QA：无上限会返回全量事件（大响应断流）
    events = session_state.get_events(since=since, limit=limit, agent_filter=agent_filter)
    return {"count": len(events), "events": events}


def presence_declare_op(
    agent: str,
    op: str,
    target: str,
    detail: str = "",
) -> dict:
    """声明操作意图（改文件前调用，检测并发冲突）。

    多主控并存时，同一文件的并发修改靠此检测。
    返回 conflict=true 时停下，广播 need_coord 等用户裁决。

    Args:
        agent: 你的工具名
        op: 操作类型（edit/create/delete/dispatch）
        target: 操作目标（文件路径 / 任务 ID）
        detail: 操作描述
    Returns:
        {"declared": true, "conflict": bool, "other_agent": "..."}
    """
    try:
        result = session_state.declare_op(agent, op, target, detail)
        return result
    except ValueError as e:
        return {"error": str(e)}


def gate_check(scenario: str, context: dict = {}) -> str:
    """过场景门禁(NEXUS Gate)。

    在执行动作前调用,检查该场景的门禁项是否满足。
    返回 JSON {passed, failures, hard_fails, warns}。

    Args:
        scenario: 场景名(done/init/dispatch/persist/code)
        context: 上下文字段(各场景 required_context 不同)
                 done: {task_id, files, summary, experiences,
                        source: Agent名(G5 mem_queried 按 callee 查 search 记录用),
                        task_path_report: report.md 绝对路径(G4 line_count 用)}
                 init: {agent, role, session_id}
            dispatch: {task_id, task_dir, target_agent, agent: 调用者名(Task545 required)}
             persist: {date, session_log_path, touched_dirs}
                code: {changed_files, commit_msg}

    Returns:
        JSON 字符串,含 passed(是否通过)和 failures(每项详情)
    """
    # Task530 A3 / Task545: agent 从 context 多级解析（G5 按 callee 查、事件归因都要真名）。
    # 链：agent > source > caller > unknown——dispatch 场景惯例传 caller（required 已加），
    # "mcp" 是未注册名会让 append_event 校验抛错被吞（Task545 根因），改为 unknown 占位
    _gate_agent = context.get("agent") or context.get("source") or context.get("caller") or "unknown"
    # T678 调用方契约：gate YAML 已全量 {vault_root} 模板化——本工具是唯一的 MCP 入口，
    # 注入 vault_root（context 显式 > env NEXUS_VAULT_ROOT > 包位置推导），解析失败
    # 显式报错（_resolve_template 缺 key 静默落字面量会让 cwd/路径非法——陷阱1）
    try:
        from nexusos_xdr._vault_env import resolve_gate_vault_root
        _vr_ok, _vr = resolve_gate_vault_root(context)
        if not _vr_ok:
            return json.dumps({"passed": False, "error": _vr,
                               "failures": [{"id": "VAULT_ROOT", "status": "fail",
                                             "severity": "REJECT", "detail": _vr}]},
                              ensure_ascii=False)
    except Exception as _vre:
        return json.dumps({"passed": False, "error": f"vault_root 契约防线异常: {_vre}"},
                          ensure_ascii=False)
    result = gate_engine_check(scenario, context or {}, agent=_gate_agent)
    # Task609 搭车②: gate 事件登记已内置 gate_engine.check()（防 import 直调无痕迹
    # 断层）——此处重复块删除防双写。detail 格式在 engine 层与本处历史格式一致（M8 兼容）。
    # Task538 修复5: on_pass 钩子已下沉到 gate_engine.check()（CLI/其他 caller 也触发）
    # 原 mcp_server 本地 trigger 块删除——engine 层统一触发，不重复
    return json.dumps(result, ensure_ascii=False)
