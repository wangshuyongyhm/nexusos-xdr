#!/usr/bin/env python3
"""dup_window_guard.py — PreToolUse hook (Bash matcher).

任务632（08200833 双窗事故 MEM#60906 治本）：拦截裸 osascript 重复开窗。
检测 tool_input.command 含 osascript + tmux attach 且目标 session 已
attached → deny 并提示改用 start_tool.sh popup。

设计依据：popup_window（dispatch.sh）有防重复（已 attached 则跳过），但主控
裸 osascript 绕过了它——tmux 同 session 允许多客户端 attach，第二窗=双窗事故。
在 Bash 通道上加硬拦，把"记得用 popup"从自觉变成强制。
解析不出 session 名 / tmux 不可用 / session 不存在 → 一律放行（fail-open）。
"""
import json
import re
import subprocess
import sys


def _extract_tmux_session(cmd: str):
    """从命令文本提取 tmux attach 的 -t session 名。

    覆盖 osascript 场景常见形态（含转义引号上下文）：
      tmux attach -t zcode / tmux attach-session -t zcode / -t'zcode'
    session 名后的 \\" 转义由字符类自然截断。解析不出返回 None（fail-open）。
    """
    m = re.search(
        r"tmux\s+attach(?:-session)?\b[^\n]*?\s-t\s*[\"']?([A-Za-z0-9_.-]+)",
        cmd,
        re.IGNORECASE,
    )
    return m.group(1) if m else None


def _session_already_attached(session: str) -> bool:
    """目标 session 是否已有客户端 attached（窗已存在）。

    行首 "<name>:" 精确匹配防子串误匹配（"ac" 会误匹配 "attached"，
    08041554 修过的坑）。attached 标记用括号包裹形式判定，
    防 session 名本身含 "attached" 的误报。
    """
    try:
        r = subprocess.run(
            ["tmux", "ls"], capture_output=True, text=True, timeout=3
        )
    except Exception:
        return False  # tmux 不可用 → fail-open
    if r.returncode != 0:
        return False  # 无 server / 无 session：首窗合法或 attach 自然失败，放行
    for line in r.stdout.splitlines():
        if line.startswith(session + ":") and (
            "(attached)" in line or "[attached]" in line
        ):
            return True
    return False


def main():
    try:
        payload = json.load(sys.stdin)
    except Exception:
        sys.exit(0)  # 解析失败不拦截

    command = payload.get("tool_input", {}).get("command", "")

    if "osascript" not in command:
        sys.exit(0)  # 非 osascript 命令，放行

    session = _extract_tmux_session(command)
    if not session:
        sys.exit(0)  # 无 tmux attach / 解析不出 session 名 → fail-open

    if not _session_already_attached(session):
        sys.exit(0)  # 未 attached（首窗合法）或 session 不存在，放行

    # 已 attached 再开窗 = 双窗事故（08200833，MEM#60906）→ deny
    msg = (
        f"tmux session {session} 已 attached（窗口已存在），"
        f"裸 osascript 再开=双窗事故（08200833，MEM#60906）。\n"
        f"改用 ~/.hermes/scripts/start_tool.sh popup {session}"
        "（内部有防重复）或跳过开窗。"
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
