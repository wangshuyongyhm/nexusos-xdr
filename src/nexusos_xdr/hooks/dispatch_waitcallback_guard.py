#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""PostToolUse hook: dispatch 后注入强提醒——通道②必须主控自起 wait_callback。

触发：主控每次调 Bash 工具后，检测 command 是否含 dispatch 操作。
机制：命中则输出 hookSpecificOutput.additionalContext，提醒主控自起 wait_callback.sh。

背景（Task 471 / 未决池 #16）：
  07301644 "根治" 阉割了 start_wait_callback（只写标记文件不起进程），
  start_tool.sh 外层仍 log "回调监控已启动" —— 谎报。
  主控误信谎报不自起 wait_callback → done 永远收不到。
  本 hook 在 dispatch 后强提醒，堵住"主控忘记自起"的最后一环。
"""
import json
import re
import sys

# dispatch 检测模式（command 字符串中命中任一即触发）
DISPATCH_PATTERNS = [
    r"19998/dispatch",
    r"start_tool\.sh\s+\S+\s+dispatch",
    r"start_tool\.sh\s+dispatch",
]

_compiled_patterns = [re.compile(p) for p in DISPATCH_PATTERNS]

REMINDER = (
    "🔴 dispatch 检测到。通道②必须由主控自起：\n"
    "  run_in_background=true bash ~/.hermes/scripts/wait_callback.sh \"{task_id}\" \"{task_dir}\"\n"
    "否则 done 感知断链（靠用户提醒才发现）。\n"
    "见 dispatch-orchestrator.md §4「三层 done 感知通道」"
)


def main():
    try:
        data = json.load(sys.stdin)
    except Exception:
        return

    tool_name = data.get("tool_name", "")
    tool_input = data.get("tool_input", {})
    command = str(tool_input.get("command", ""))

    # 仅匹配 Bash 工具且 command 含 dispatch 操作
    if tool_name != "Bash":
        return
    if not any(p.search(command) for p in _compiled_patterns):
        return

    print(json.dumps({
        "hookSpecificOutput": {
            "hookEventName": "PostToolUse",
            "additionalContext": REMINDER,
        }
    }, ensure_ascii=False))


if __name__ == "__main__":
    main()
