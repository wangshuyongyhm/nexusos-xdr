#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""PreToolUse hook: Edit/Write 前检查 op-log（改文件前应声明意图防并发）。

触发：主控调 Edit/Write 工具时。
机制：查 xdr :19998/session/op-log 最近 30s 内有无针对该文件的声明，
      无则 WARN（不 deny，避免误拦紧急修复）。

2026-08-05 固化：本会话 51 文件改动 0 op-log（100% 违规）。
规则靠自觉无效，加 hook 硬提醒。
"""
import json
import sys
import urllib.parse
import urllib.request

# 豁免：这些文件不需要 op-log（个人配置/临时/频繁小改）
EXEMPT_PATTERNS = [
    "~/.zcode/", ".obsidian/", "/tmp/", "03-Daily/",  # 日志/配置/临时
    "agent_prompt_guard.py", "interaction_guard.py", "oplog_guard.py",  # hook 自身
]


def main():
    try:
        data = json.load(sys.stdin)
    except Exception:
        sys.exit(0)

    tool = data.get("tool_name", "")
    if tool not in ("Edit", "Write"):
        sys.exit(0)

    # 取目标文件路径
    tool_input = data.get("tool_input", {}) or {}
    target = tool_input.get("file_path", "") or tool_input.get("path", "")
    if not target:
        sys.exit(0)

    # 豁免检查
    if any(p in target for p in EXEMPT_PATTERNS):
        sys.exit(0)

    # 查 op-log（走 GET 只读端点；Task1704 件一池#285）
    # 窗口 30→300：长 Bash（构建/测试）耗尽 30s 窗 → 声明过期误报；
    # 匹配在服务端归一化（相对/~/组合串口径对齐），hook 侧只传原始路径。
    try:
        url = (
            "http://127.0.0.1:19998/session/op-log"
            f"?target={urllib.parse.quote(target)}&within=300"
        )
        with urllib.request.urlopen(url, timeout=3) as resp:
            data = json.loads(resp.read())
        recent_has = data.get("count", 0) > 0
    except Exception:
        sys.exit(0)  # xdr 不可用不拦

    if not recent_has:
        msg = (
            f"⚠️ 改文件前未声明 op-log: {target.split('/')[-1]}\n"
            "改文件前应 POST /session/op-log 声明意图（防并发冲突，AGENTS.md §NEXUS-PRESENCE）。\n"
            "— 如确为紧急修复或会话末批量落盘，可忽略本提醒。"
        )
        output = {
            "hookSpecificOutput": {
                "hookEventName": "PreToolUse",
                "permissionDecision": "allow",
            },
            "additionalContext": msg,
        }
        print(json.dumps(output, ensure_ascii=False))

    sys.exit(0)


if __name__ == "__main__":
    main()
