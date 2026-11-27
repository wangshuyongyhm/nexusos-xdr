#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""broadcast_guard.py — Task455 zcode 最后一公里修复（方案3）。

替代废弃的 project 级 bash hook 体系（nexus-before-submit-prompt.sh / nexus-stop-broadcast.sh）。
挂 user 级 ~/.zcode/cli/config.json 的 UserPromptSubmit + Stop（同 event 多 hook 共存）。

触发：
  - UserPromptSubmit：用户每次提交 prompt 时（与 interaction_guard 共存）
  - Stop：主控生成回复结束时（与 completion_guard + selfcheck_evidence_guard 共存）

机制：
  读 ~/.zcode/nexus-broadcast-pending.jsonl（xdr 中央路由 on_mention 写入的待处理 @zcode 广播），
  非空时输出 additionalContext 提醒 Agent 处理（含 WAKE_PROMPT 风格的 4 步执行指令）。
  空 pending 不输出（避免每轮空提醒）。

输出格式（strict schema 根治模板，MEM#40605）：
  UserPromptSubmit: {hookSpecificOutput: {hookEventName, additionalContext}}
  Stop: {hookSpecificOutput: {hookEventName, stopHookEndBehavior: continue, additionalContext}}

2026-08-06 固化：project 级 bash hook 被 ZCode 安全策略忽略（config.project_hooks.ignored），
                  + watcher 7-24 停（设计 FALLBACK）。本 hook 用 user 级 python 管道（已实证可用，
                  Task452 4 hook 同款）替代，单一真源（处女原则）。
"""
import json
import os
import sys
from datetime import datetime
from pathlib import Path

# ---- 运行痕迹：每次运行往 ~/.zcode/nexus-hook-trace.log 追加一行，回答「这次 hook 以什么身份、凭什么跑的」----
TRACE_FILE = Path.home() / ".zcode" / "nexus-hook-trace.log"


def _read_stdin() -> dict:
    """hook 的输入 JSON 只读一次并缓存（身份解析和 main 都要用）。

    人在终端里直接跑这个脚本时 stdin 是终端，就不读了，免得卡住等输入。
    """
    if sys.stdin is None or sys.stdin.isatty():
        return {}
    try:
        data = json.load(sys.stdin) or {}
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


_STDIN = _read_stdin()


def _lookup_session(session: str) -> str:
    """拿会话 ID 查会话映射表 ~/.zcode/nexus-session-map.tsv（每行 <会话 ID>\t<实例名>），查不到返回空串。"""
    if not session:
        return ""
    try:
        for line in (Path.home() / ".zcode" / "nexus-session-map.tsv").read_text(encoding="utf-8").splitlines():
            sid, _, name = line.partition("\t")
            if sid == session and name.strip():
                return name.strip()
    except OSError:
        pass
    return ""


def _resolve_identity() -> tuple:
    """返回 (实例名, 命中来源)。

    顺序与 bash 脚本 nexus-agent-identity.sh 一致：环境变量注入 > 会话映射表 > 缺省 zcode。
    查映射表有两个会话 ID 来源，先 hook 输入里的 session_id，再环境变量 CLAUDE_SESSION_ID——
    这两个 ZCode 都只在指南里声明过，本机没有实测，所以都试；痕迹里会记下各自有没有。
    用标准库内联而不去调 Vault 里的脚本：hook 进程用哪个 python3、能否 import 到 Vault 的包都不受控。
    """
    injected = os.environ.get("NEXUS_ZCODE_AGENT_NAME")
    if injected:
        return injected, "env"
    for session, source in (
        (str(_STDIN.get("session_id") or "").strip(), "stdin"),
        (os.environ.get("CLAUDE_SESSION_ID", "").strip(), "env_session"),
    ):
        name = _lookup_session(session)
        if name:
            return name, source
    return "zcode", "default"


_AGENT_NAME, _IDENTITY_SOURCE = _resolve_identity()


def _write_trace() -> None:
    """写一行运行痕迹。写不进去就算了——痕迹是观测件，不能反过来影响 hook 本职。"""
    try:
        event = _STDIN.get("hook_event_name") or _STDIN.get("event") or ""
        for arg in sys.argv[1:]:
            if arg.startswith("--event="):
                event = arg.split("=", 1)[1]
                break
        stop_behavior = "block" if event == "Stop" and _read_pending() else "-"
        with TRACE_FILE.open("a", encoding="utf-8") as f:
            f.write(
                f"{datetime.now().astimezone().isoformat(timespec='seconds')} "
                f"event={_STDIN.get('hook_event_name') or _STDIN.get('event') or ''} "
                f"agent={_AGENT_NAME} source={_IDENTITY_SOURCE} "
                f"has_stdin_sid={int(bool(str(_STDIN.get('session_id') or '').strip()))} "
                f"has_env_sid={int(bool(os.environ.get('CLAUDE_SESSION_ID', '').strip()))} "
                f"stdin_keys={','.join(sorted(_STDIN.keys()))} "
                f"stop_behavior={stop_behavior}\n"
            )
    except OSError:
        pass


_AGENT_SUFFIX = _AGENT_NAME.removeprefix("zcode")
PENDING_FILE = Path.home() / ".zcode" / f"nexus-broadcast-pending{_AGENT_SUFFIX}.jsonl"

# 处理指令（参考 nexus-broadcast-lib.sh 的 WAKE_PROMPT，简化为本 hook 直接生成；
# 署名按 env 派生——zcode2 窗口注入的回传指令必须以 zcode2 名义应答。
# 注意：保持普通字符串（非 f-string）——{agent_name} 与 {from} 等由 _format_wake
# 统一 .format() 填充，agent 名也走同一通道，避免双层花括号解析冲突）
WAKE_INSTRUCTIONS = """[NEXUS 广播唤醒 · B模式 · agent={agent_name}] 有 {count} 条待处理 @{agent_name} 广播：
{list}
对每条广播自动执行：
  1) 在聊天输出「📡 收到 {from} 广播：{detail}」
  2) 执行广播要求
  3) curl -sf -X POST http://127.0.0.1:19998/session/event -H "Content-Type: application/json" \\
       -d '{{"agent":"{agent_name}","type":"answer","target":"{from}","detail":"..."}}'  回传
  4) 运行 nexus-clear-pending.sh {ts}  清理 pending（路径见 05-System/自动化脚本/nexusos_xdr/agents/zcode/）
不要问用户，直接执行。"""


def _read_pending() -> list:
    """读 pending 文件，返 list[dict]。失败/空返 []。"""
    if not PENDING_FILE.exists():
        return []
    try:
        text = PENDING_FILE.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return []
    items = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            if isinstance(obj, dict):
                items.append(obj)
        except json.JSONDecodeError:
            continue
    return items


def _format_wake(pending_items: list) -> str:
    """格式化 WAKE_INSTRUCTIONS 提醒文本。"""
    count = len(pending_items)
    # 列出每条广播（最多 5 条，避免 detail 过长）
    list_lines = []
    for i, item in enumerate(pending_items[:5], 1):
        from_agent = item.get("from") or item.get("agent") or "?"
        detail = str(item.get("detail") or item.get("message") or "")[:100]
        ts = item.get("ts", "?")
        list_lines.append(f"  {i}. [ts={ts}] from={from_agent}: {detail}")
    if count > 5:
        list_lines.append(f"  ...等 {count} 个")

    # 用第一条作为 WAKE_INSTRUCTIONS 模板的示例填充（其余已在 list 显示）
    first = pending_items[0]
    # 注意：'from' 是 Python 关键字不能做 kwarg，用 dict 解包；
    # agent_name 与 _AGENT_NAME 派生同源（1414 分身署名）
    return WAKE_INSTRUCTIONS.format(**{
        "agent_name": _AGENT_NAME,
        "count": count,
        "list": "\n".join(list_lines),
        "from": first.get("from") or first.get("agent") or "?",
        "detail": str(first.get("detail") or first.get("message") or "")[:80],
        "ts": first.get("ts", "?"),
    })


def main():
    _write_trace()
    stdin_data = _STDIN

    pending = _read_pending()
    if not pending:
        # 空 pending 不输出（避免每轮空提醒）
        sys.exit(0)

    context = _format_wake(pending)

    # 推断 hookEventName：优先 stdin.hook_event_name，其次命令行 --event=，默认 UserPromptSubmit
    event_name = (
        stdin_data.get("hook_event_name")
        or stdin_data.get("event")
        or "UserPromptSubmit"
    )
    for arg in sys.argv[1:]:
        if arg.startswith("--event="):
            event_name = arg.split("=", 1)[1]
            break

    output = {
        "hookSpecificOutput": {
            "hookEventName": event_name,
            "additionalContext": context,
        },
    }
    if event_name == "Stop":
        # Stop 时有待收 → 强制处理（2026-11-23 用户裁定：block；重复触发由 ZCode 内置三次上限兜底）
        output["hookSpecificOutput"]["stopHookEndBehavior"] = "block"

    print(json.dumps(output, ensure_ascii=False))
    sys.exit(0)


if __name__ == "__main__":
    main()
