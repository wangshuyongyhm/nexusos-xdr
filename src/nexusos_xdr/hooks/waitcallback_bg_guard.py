#!/usr/bin/env python3
"""waitcallback_bg_guard.py — PreToolUse hook (Bash matcher).

09101936 会话根修（用户定向「犯错误要做修复，以后不犯」）：wait_callback.sh
监控只能用 harness run_in_background 挂——用 shell `&`/nohup/disown/setsid
在前台 Bash 调用内后台化，工具调用结束进程即被杀，done 通知永远丢失
（0910 kimi 1253 实证：用户看着完工主控不知情；重蹈 Task597/§8.2 已知禁令）。

既有 dispatch_waitcallback_guard.py 在 PostToolUse（事后提醒，§11 记录 4 次
被忽略）——本 hook 在 PreToolUse 硬拦（deny），把禁令从自觉变成强制。

设计：
- 命中 wait_callback.sh 且带危险后台化（命令尾部/中部的 `&`、nohup、
  disown、setsid）→ deny，提示改用 run_in_background。
- `2>&1`/`>&1` 这类重定向里的 & 先剥除再扫描，防误报。
- 解析失败/不命中 → 放行（fail-open，同 dup_window_guard 惯例）。
"""
import json
import re
import sys


def _has_dangerous_bg(cmd: str) -> bool:
    # 先剥除重定向里的 &（2>&1 / >&1），防误报
    cleaned = re.sub(r"\d*>\s*&\d*", "", cmd)
    if re.search(r"\b(?:nohup|disown|setsid)\b", cleaned):
        return True
    # 剥除后仍剩裸 &（尾部或中部）= shell 后台化
    return "&" in cleaned


def main():
    try:
        payload = json.load(sys.stdin)
    except Exception:
        sys.exit(0)  # 解析失败不拦截

    command = payload.get("tool_input", {}).get("command", "")
    if "wait_callback.sh" not in command:
        sys.exit(0)  # 与监控无关，放行
    if not _has_dangerous_bg(command):
        sys.exit(0)  # 干净调用（run_in_background 载体），放行

    msg = (
        "wait_callback.sh 禁止用 shell &/nohup/disown/setsid 后台化——"
        "前台 Bash 调用结束进程即被杀，done 通知永远丢失"
        "（0910 kimi 1253 实证；重蹈 Task597/§8.2）。\n"
        "正确做法：Bash 工具调用加 run_in_background=true 裸跑"
        "`bash ~/.hermes/scripts/wait_callback.sh <task_id> <dir>`，"
        "挂完 30s 内 pgrep -fl wait_callback 确认进程活，通知走 task notification 通道。"
    )
    output = {
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": "deny",
            "additionalContext": msg,
        },
    }
    print(json.dumps(output, ensure_ascii=False))
    sys.exit(0)


if __name__ == "__main__":
    main()
