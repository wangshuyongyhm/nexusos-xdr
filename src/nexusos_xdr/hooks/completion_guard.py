#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""completion_guard.py — Task452 铁律2 hook：完成度闭环（Stop hook）。

触发：主控生成回复结束时（Stop event）。
机制：检测回复含"落盘/收工"等意图词 → 查 xdr task_state 有无 PENDING/RUNNING → 有则警告。
设计：stopHookEndBehavior=continue 请求 Agent 重新考虑（不强制 block）。

2026-08-06 固化：未完成提落盘 = 认知诚实体系断链。
本 hook 把"完成度闭环"从"靠自觉"升级到"代码强制提醒"。

局限（方向X 接受）：拿不到 ZCode 内部 todo（阶段 todo），只查 dispatch task_state。
"""
import json
import os
import sys
import urllib.request
from datetime import datetime
from pathlib import Path


# 落盘/收工意图词
CLOSURE_KEYWORDS = [
    "落盘", "收工", "今天先这样", "今天到此", "会话结束", "本次完成",
]


# ---- 运行痕迹：守卫真触发时往 ~/.zcode/nexus-hook-trace.log 追加一行，回答「它到底触发过没有」----
TRACE_FILE = Path.home() / ".zcode" / "nexus-hook-trace.log"


def _resolve_identity(stdin_data: dict) -> tuple:
    """返回 (窗口身份, 命中来源)。

    取法与部署位 broadcast_guard.py 的同名实现一致（环境变量 > 会话映射表 > 缺省 zcode）。
    不 import 它：broadcast_guard 在模块级读 stdin，import 会把本 hook 的输入吃掉。
    """
    injected = os.environ.get("NEXUS_ZCODE_AGENT_NAME")
    if injected:
        return injected, "env"
    try:
        rows = (Path.home() / ".zcode" / "nexus-session-map.tsv").read_text(
            encoding="utf-8").splitlines()
    except OSError:
        rows = []
    for session, source in (
        (str(stdin_data.get("session_id") or "").strip(), "stdin"),
        (os.environ.get("CLAUDE_SESSION_ID", "").strip(), "env_session"),
    ):
        if not session:
            continue
        for line in rows:
            sid, _, name = line.partition("\t")
            if sid == session and name.strip():
                return name.strip(), source
    return "zcode", "default"


def _write_trace(stdin_data: dict, guard: str, missing: list) -> None:
    """写一行触发痕迹。写不进去就算了——痕迹是观测件，不能反过来影响守卫本职。"""
    agent, source = _resolve_identity(stdin_data)
    try:
        with TRACE_FILE.open("a", encoding="utf-8") as f:
            f.write(
                f"{datetime.now().astimezone().isoformat(timespec='seconds')} "
                f"event={stdin_data.get('hook_event_name') or 'Stop'} "
                f"guard={guard} agent={agent} source={source} "
                f"result=reminded missing={';'.join(missing)}\n"
            )
    except OSError:
        pass


def _extract_reply_text(data: dict) -> str:
    """从 Stop hook stdin 提取回复文本（字段名实测多版本兼容）。"""
    # 2611230843：宿主 Stop 输入实际键是 responseText / last_assistant_message（痕迹文件实证），
    # 旧键保留在后以兼容旧宿主——原写法只认旧键，一个都对不上 → 拿不到文本即静默退出。
    for k in ("responseText", "last_assistant_message", "response", "reply", "output",
              "text", "assistant_response", "message"):
        v = data.get(k)
        if isinstance(v, str) and v:
            return v
        # 嵌套 dict 也试一层
        if isinstance(v, dict):
            for kk in ("content", "text", "value"):
                vv = v.get(kk)
                if isinstance(vv, str) and vv:
                    return vv
    return ""


def _query_active_tasks() -> list:
    """查 xdr :19998/task_state，返 PENDING/RUNNING 任务清单。失败返 []。"""
    try:
        # 2611192100 Q1a: 显式 with（原写法 resp 不 close，每 60s 泄漏一个连接）
        with urllib.request.urlopen(
            "http://127.0.0.1:19998/task_state", timeout=3
        ) as resp:
            data = json.loads(resp.read())
        # task_state 返回 {"active": [...], "recent": [...]} 或 [list]
        if isinstance(data, list):
            return [t for t in data if isinstance(t, dict)
                    and t.get("status") in ("PENDING", "RUNNING")]
        if isinstance(data, dict):
            active = data.get("active", []) or []
            return [t for t in active if isinstance(t, dict)
                    and t.get("status") in ("PENDING", "RUNNING")]
    except Exception:
        pass
    return []


def main():
    try:
        data = json.load(sys.stdin)
    except Exception:
        sys.exit(0)

    reply_text = _extract_reply_text(data)

    # 检测落盘/收工意图
    has_closure_intent = any(kw in reply_text for kw in CLOSURE_KEYWORDS)
    if not has_closure_intent:
        sys.exit(0)

    # 查 xdr task_state
    active = _query_active_tasks()

    if active:
        summary_lines = []
        for t in active[:3]:
            tid = t.get("task_id", "?")
            status = t.get("status", "?")
            result = t.get("result", {}) or {}
            summary = str(result.get("summary", ""))[:60] if isinstance(result, dict) else ""
            summary_lines.append(f"  - {tid}: {status} {summary}")
        suffix = f"\n  ...等 {len(active)} 个" if len(active) > 3 else ""

        output = {
            "hookSpecificOutput": {
                "hookEventName": "Stop",
                "stopHookEndBehavior": "continue",  # 请求继续（让 Agent 重新考虑）
            },
            "additionalContext": (
                f"🔴 完成度闭环警告（Task452 铁律2）：你说\"落盘/收工\"，"
                f"但 xdr 有 {len(active)} 个 PENDING/RUNNING 任务未完成：\n"
                + "\n".join(summary_lines) + suffix + "\n"
                "未 100% 完成不提落盘（认知诚实体系）。先完成或明确暂停原因。"
            ),
        }
        _write_trace(data, "completion", [f"{len(active)} 个 PENDING/RUNNING 任务未完成"])
        print(json.dumps(output, ensure_ascii=False))

    sys.exit(0)


if __name__ == "__main__":
    main()
