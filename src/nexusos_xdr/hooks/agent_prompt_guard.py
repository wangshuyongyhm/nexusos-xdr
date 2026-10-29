#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""PreToolUse hook: 防止主控派子 Agent 改代码（应走 dispatch 小弟）。

触发：ZCode 主控调 Agent/Task 工具时。
逻辑：读 stdin JSON 的 tool_input.prompt → 检测"改代码"意图关键词 →
      若命中且无调研白名单兜底 → permissionDecision:deny 拦截。

2026-08-05 固化：子 Agent 职责是隔离 context 辅助调研，不改代码。
让子 Agent 改代码 = 绕过 dispatch 全链（B11 裸派变种）。
"""
import json
import sys

# 改代码意图关键词（命中 = 想让子 Agent 动手改）
# Task578 #11: 补「文件写入」类触发词——573 dry-run 实证仅含"修改文件/写入文件"
# 的 prompt 不带"代码"字样即穿透（词表原只覆盖代码语义）。加词防过宽原则：
# 用≥3字词组，不加"写文件/改文件"短词（"读文件"白名单子串误伤风险）。
CODE_MODIFY_KEYWORDS = [
    "改代码", "修改代码", "实现", "新增函数", "新增文件", "新建文件",
    "write file", "edit file", "create file", "implement",
    "refactor", "重写", "补全代码", "修复 bug", "fix bug",
    "ALTER TABLE", "CREATE TABLE", "INSERT INTO", "UPDATE ", "DELETE FROM",
    "sed -i", "写脚本", "新建脚本",
    # Task578 #11: 文件写入类（不带"代码"字样的改动手势）
    "修改文件", "写入文件", "修改配置", "写入配置", "改配置",
    "修改 yaml", "修改 json", "写入 yaml", "写入 json",
]

# 调研白名单（prompt 含这些 = 允许，即使沾边关键词也放行）
# 注意：避免单字（读/查/看/ls 等易子串误命中），用词组或带空格的命令
RESEARCH_WHITELIST = [
    "调研", "分析", "评估", "只读", "不改", "不要做", "禁止项",
    "核验", "检查", "查询", "统计",
    "搜索", "search", "inspect", "review",
    "回报", "摘要", "结论", "给方案", "给建议",
    "不改任何", "只调研", "读文件", "返回结论",
]


def main():
    try:
        data = json.load(sys.stdin)
    except Exception:
        sys.exit(0)  # 解析失败不拦（防误伤）

    prompt = ""
    tool_input = data.get("tool_input", {}) or {}
    # Agent 工具的 prompt 在 tool_input.prompt
    prompt = str(tool_input.get("prompt", "")) or str(tool_input.get("description", ""))

    if not prompt:
        sys.exit(0)  # 无 prompt 不拦

    prompt_lower = prompt.lower()

    # 白名单优先（含调研关键词 = 放行，即使沾边改代码词）
    if any(w in prompt or w.lower() in prompt_lower for w in RESEARCH_WHITELIST):
        sys.exit(0)

    # 检测改代码意图
    hit = [k for k in CODE_MODIFY_KEYWORDS if k in prompt or k.lower() in prompt_lower]
    if hit:
        msg = (
            f"⚠️ Agent 工具禁做代码修改（命中关键词: {', '.join(hit[:3])}）。\n"
            "子 Agent 职责是隔离 context 辅助调研，不改代码。\n"
            "代码修改 >3 行 → 走 dispatch 小弟（oc/omp/ds，xdr /dispatch 全链）。\n"
            "详见 _shared.md §7 判断表第三行。\n"
            "— 如果这确实是调研任务，请在 prompt 加'调研/只读/不改'白名单词。"
        )
        output = {
            "hookSpecificOutput": {
                "hookEventName": "PreToolUse",
                "permissionDecision": "deny",
                "additionalContext": msg,
            },
        }
        print(json.dumps(output, ensure_ascii=False))
        sys.exit(0)  # exit 0 + permissionDecision:deny = 拦截但不算 hook 错误

    sys.exit(0)


if __name__ == "__main__":
    main()
