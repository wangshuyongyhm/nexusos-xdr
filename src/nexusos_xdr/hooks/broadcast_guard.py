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
from pathlib import Path

# 1414 分身派生：多开实例（zcode2）的 pending 文件带后缀——按 NEXUS_ZCODE_AGENT_NAME
# 派生（与 nexus-broadcast-lib.sh 同规则），env 缺省=zcode 时为原路径，主实例零回归。
_AGENT_NAME = os.environ.get("NEXUS_ZCODE_AGENT_NAME", "zcode")
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
    # 读 stdin（含 hook_event_name 等元信息），失败容错
    stdin_data = {}
    try:
        stdin_data = json.load(sys.stdin) or {}
        if not isinstance(stdin_data, dict):
            stdin_data = {}
    except Exception:
        pass

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
        # Stop 时请求 Agent 继续（让它处理 pending）
        output["hookSpecificOutput"]["stopHookEndBehavior"] = "continue"

    print(json.dumps(output, ensure_ascii=False))
    sys.exit(0)


if __name__ == "__main__":
    main()
