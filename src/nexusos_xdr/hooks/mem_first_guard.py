#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""mem_first_guard.py — Task452 铁律3 hook：§0 分流强制（PreToolUse Bash）。

触发：主控调 Bash 工具时。
机制：检测 grep/rg/find + 知识库路径 → 注入 additionalContext 提醒"用 MEM 不要 grep"。
设计：软警告不拦（permissionDecision: allow），主控有合理 grep 场景（如精确行号）可忽略。

2026-08-06 固化：规则存在≠执行（MEM#31186），靠自觉=0% 执行率。
本 hook 把 §0 分流铁律从"靠自觉"升级到"代码强制提醒"。
"""
import json
import re
import sys


def main():
    try:
        data = json.load(sys.stdin)
    except Exception:
        sys.exit(0)

    tool = data.get("tool_name", "")
    if tool != "Bash":
        sys.exit(0)

    tool_input = data.get("tool_input", {}) or {}
    cmd = str(tool_input.get("command", ""))

    if not cmd:
        sys.exit(0)

    # 检测：命令含 grep/rg/find + 目标是知识库路径
    is_search_tool = bool(re.search(r"\b(grep|rg|find)\b", cmd))
    is_kb_target = any(p in cmd for p in [
        "05-System/NEXUS-CONSTRAINT/",
        "05-System/Agent/",
        "05-System/NEXUS-MEM/",
        "知识库", "约束文件",
    ])
    # 排除：task/ 目录（任务书，不是知识库）/ /tmp/ / .codegraph/ / 06-Memory/
    is_excluded = any(p in cmd for p in ["task/", "/tmp/", ".codegraph/", "06-Memory/"])

    if is_search_tool and is_kb_target and not is_excluded:
        output = {
            "hookSpecificOutput": {
                "hookEventName": "PreToolUse",
                "permissionDecision": "allow",  # 软警告不拦
            },
            "additionalContext": (
                "🔴 §0 分流提醒（Task452 铁律3）：你在用 grep/rg 扫知识库文件。\n"
                "知识库操作必须用 MEM（:11537/search 带 agent_id 触发 annotate 走认知诚实闭环）。\n"
                "grep 是兜底不是首选。改用：\n"
                "  curl -H \"X-Credential: $(cat 05-System/Agent/zcode/search_cred.txt)\" \\\n"
                "    \"http://127.0.0.1:11537/search?q=关键词&top_k=5&agent_id=zcode\"\n"
                "（若确有 grep 必要场景如查精确行号，忽略此提醒）"
            ),
        }
        print(json.dumps(output, ensure_ascii=False))

    sys.exit(0)


if __name__ == "__main__":
    main()
