#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
broadcast_router.py — XDR 中央 @mention 广播路由

broadcast 写入 events.jsonl 后，解析 message 中的 @agent 点名，
按 AGENT_REGISTRY[agent].on_mention 同步 fan-out 到各 external Agent。
设计:
  - Layer A（中央）: XDR 监听 + 写 pending / 调 on_mention 脚本
  - Layer B/C（边缘）: 各 Agent 宿主 hook 唤醒 LLM turn（XDR 无法替代）
  - S2 hook 台账（Task308）: _invoke_on_mention 单一埋点，每次调用写
    hook_invoked/hook_result 成对事件到 XDR 事件流（零新存储，
    /session/events 现成查询口）。台账全程 fail-open——自身异常绝不
    影响 hook 调用主链路（#32422 教训：旁路逻辑不得污染主链路）。

作者: NEXUS 体系  日期: 2026-07-22  Task308: omp 增加 S2 hook 台账
"""

import json
import logging
import re
import subprocess
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Union


logger = logging.getLogger("nexusos-xdr.broadcast_router")

_SCRIPT_DIR = Path(__file__).resolve().parent
_AGENTS_DIR = _SCRIPT_DIR / "agents"

MENTION_RE = re.compile(r"@([a-zA-Z0-9_-]+)")

OnMentionHandler = Union[str, Callable[[Dict[str, Any]], Dict[str, Any]]]

# --- S2 hook 台账（Task308）----------------------------------------------
# 每次 on_mention 调用写 hook_invoked / hook_result 成对事件到 XDR 事件流
# （复用 /session/events 查询口，零新存储）。handler_id = "<event_ts>:<agent>"
# 天然唯一，查询侧按 handler_id 配对即得「调了谁/结果/耗时」。
# 台账事件来源 agent 用 "xdr"（launchd ai.nexus.xdr 自动注册名）；306 蓝图
# 原文的 "xdr-router" 未过 session_state.validate_agent 且注册表禁改。
_LEDGER_AGENT = "xdr"
_LEDGER_MAX_FIELD = 200  # 台账字符串字段封顶（防超大输出灌爆事件流）
_HOOK_TIMEOUT_S = 15


def _clip(value: Any, limit: int = _LEDGER_MAX_FIELD) -> Any:
    """台账字段封顶；非字符串原样返回。"""
    return value[:limit] if isinstance(value, str) else value


def _ledger_write(event_type: str, detail: str, extra: Optional[Dict[str, Any]] = None) -> None:
    """fail-open 写 hook 台账事件。

    台账自身异常（含 agent 未注册 / 事件流写失败）只 warning，
    绝不向 hook 调用主链路传播（#32422：旁路逻辑不得污染主链路）。
    """
    try:
        from nexusos_xdr import session_state  # 懒加载防环：session_state 反向懒加载本模块
        session_state.append_event(_LEDGER_AGENT, event_type, detail=detail, extra=extra)
    except Exception as exc:
        logger.warning("[hook_ledger] %s 写入失败(fail-open): %s", event_type, exc)


def parse_mentions(message: str) -> List[str]:
    """Extract @agent mentions from broadcast message (deduped, lowercased)."""
    if not message:
        return []
    seen: set = set()
    mentions: List[str] = []
    for raw in MENTION_RE.findall(message):
        key = raw.lower()
        if key not in seen:
            seen.add(key)
            mentions.append(key)
    return mentions


def resolve_mentions(event: Dict[str, Any]) -> List[str]:
    """Resolve target agents from broadcast event."""
    message = event.get("detail") or ""
    mentions = parse_mentions(message)
    target = (event.get("target") or "").strip().lower()
    if target and target not in mentions:
        mentions.append(target)
    sender = (event.get("agent") or "").strip().lower()
    return [m for m in mentions if m and m != sender]


def route_broadcast_event(event: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Fan-out broadcast event to mentioned agents with on_mention handlers.

    Returns list of per-agent routing results (for HTTP response / logging).
    """
    if event.get("type") != "broadcast":
        return []

    try:
        from nexusos_xdr.agents import AGENT_REGISTRY
    except Exception as exc:
        logger.warning("[broadcast_router] AGENT_REGISTRY import failed: %s", exc)
        return []

    results: List[Dict[str, Any]] = []
    for agent_name in resolve_mentions(event):
        entry = AGENT_REGISTRY.get(agent_name)
        if not entry:
            logger.debug("[broadcast_router] unknown mention @%s, skip", agent_name)
            continue
        handler = entry.get("on_mention")
        if not handler:
            logger.debug("[broadcast_router] @%s has no on_mention handler", agent_name)
            continue
        try:
            result = _invoke_on_mention(agent_name, handler, event)
        except Exception as exc:
            logger.error("[broadcast_router] @%s on_mention failed: %s", agent_name, exc)
            result = {"ok": False, "error": str(exc)}
        if isinstance(result, dict) and result.get("enqueued") == 0:
            logger.warning(
                "[broadcast_router] @%s enqueued=0 reason=%s ts=%s",
                agent_name, result.get("reason"), result.get("ts"),
            )
        results.append({"agent": agent_name, **result})
    return results


def _invoke_on_mention(
    agent_name: str,
    handler: OnMentionHandler,
    event: Dict[str, Any],
) -> Dict[str, Any]:
    """Invoke on_mention handler (script path or callable), with S2 ledger pairing.

    进入写 hook_invoked，每个出口（正常/失败/异常/超时）写同 handler_id
    的 hook_result。台账 fail-open；返回值与异常语义和无台账时完全一致
    （route 层的 except 兜底行为不变）。
    """
    event_ts = event.get("ts", "")
    handler_id = f"{event_ts}:{agent_name}"
    script_path = handler if isinstance(handler, str) else f"callable:{getattr(handler, '__name__', type(handler).__name__)}"
    _ledger_write(
        "hook_invoked",
        f"on_mention @{agent_name} invoked (handler_id={handler_id})",
        {
            "handler_id": handler_id,
            "hook_type": "on_mention",
            "target_agent": agent_name,
            "script_path": _clip(script_path),
            "event_ts": event_ts,
            "sender": event.get("agent", ""),
            "timeout_s": _HOOK_TIMEOUT_S,
        },
    )
    start = time.monotonic()
    try:
        result, meta = _run_on_mention_handler(agent_name, handler, event)
    except subprocess.TimeoutExpired:
        _ledger_write(
            "hook_result",
            f"on_mention @{agent_name} timeout after {_HOOK_TIMEOUT_S}s (handler_id={handler_id})",
            {
                "handler_id": handler_id,
                "target_agent": agent_name,
                "exit_code": None,
                "duration_ms": int((time.monotonic() - start) * 1000),
                "error": "timeout",
            },
        )
        raise  # 行为零变更：route 层 except 兜底，异常语义保持原样
    except Exception as exc:
        _ledger_write(
            "hook_result",
            f"on_mention @{agent_name} exception: {exc} (handler_id={handler_id})",
            {
                "handler_id": handler_id,
                "target_agent": agent_name,
                "exit_code": None,
                "duration_ms": int((time.monotonic() - start) * 1000),
                "error": _clip(str(exc)),
            },
        )
        raise
    ledger_result: Dict[str, Any] = {
        "handler_id": handler_id,
        "target_agent": agent_name,
        "exit_code": meta.get("exit_code"),
        "duration_ms": int((time.monotonic() - start) * 1000),
        "enqueued": meta.get("enqueued"),
        "dedup_reason": _clip(meta.get("dedup_reason")),
        "stdout_json_ok": meta.get("stdout_json_ok"),
    }
    if meta.get("error"):
        ledger_result["error"] = _clip(meta["error"])
    _ledger_write(
        "hook_result",
        f"on_mention @{agent_name} exit={meta.get('exit_code')} enqueued={meta.get('enqueued')} (handler_id={handler_id})",
        ledger_result,
    )
    return result


def _run_on_mention_handler(
    agent_name: str,
    handler: OnMentionHandler,
    event: Dict[str, Any],
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """执行 on_mention（原 _invoke_on_mention 主体）。返回 (result, ledger_meta)。"""
    if callable(handler):
        out = handler(event)
        if isinstance(out, dict):
            return out, {"handler_kind": "callable"}
        return {"ok": True, "result": out}, {"handler_kind": "callable"}

    if not isinstance(handler, str) or not handler.strip():
        err = "empty on_mention handler"
        return {"ok": False, "error": err}, {"error": err}

    script = Path(handler)
    if not script.is_absolute():
        script = _AGENTS_DIR / handler
    if not script.exists():
        err = f"on_mention script missing: {script}"
        return {"ok": False, "error": err}, {"error": err}
    payload = json.dumps(event, ensure_ascii=False)
    from nexusos_mem.env_scrub import scrubbed_env  # Task596 池#50: 子进程 env 净化（局部 import，standalone 运行兼容）
    proc = subprocess.run(
        ["/bin/bash", str(script)],
        input=payload,
        capture_output=True,
        text=True,
        timeout=_HOOK_TIMEOUT_S,
        check=False,
        env=scrubbed_env(),
    )
    if proc.returncode != 0:
        err = (proc.stderr or proc.stdout or "").strip()
        logger.warning(
            "[broadcast_router] @%s script exit %s: %s",
            agent_name,
            proc.returncode,
            err[:200],
        )
        return (
            {"ok": False, "exit_code": proc.returncode, "stderr": err[:500]},
            {"exit_code": proc.returncode, "error": err[:_LEDGER_MAX_FIELD]},
        )

    stdout = (proc.stdout or "").strip()
    if stdout:
        try:
            parsed = json.loads(stdout)
            if isinstance(parsed, dict):
                # Handler may echo full event JSON; do not overwrite routed agent name.
                parsed.pop("agent", None)
                return (
                    {"ok": True, **parsed},
                    {
                        "exit_code": 0,
                        "enqueued": parsed.get("enqueued"),
                        "dedup_reason": parsed.get("reason"),
                        "stdout_json_ok": True,
                    },
                )
        except json.JSONDecodeError:
            pass
    return (
        {"ok": True, "stdout": stdout[:500] if stdout else ""},
        {"exit_code": 0, "stdout_json_ok": True if stdout else None},
    )
