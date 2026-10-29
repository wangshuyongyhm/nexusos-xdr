#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""UserPromptSubmit hook: 每轮注入交互铁律提醒（防"规则存在≠执行"）。

触发：用户每次提交 prompt 时。
机制：输出 hookSpecificOutput.additionalContext 注入 conversation，强制主控本轮遵守：
  1. §0 动作前分流（动手前先查 MEM/图谱/分流）
  2. 回复含【认知四象限】+【待决事项】两段
  3. 发送前 7 问自检

2026-08-05 固化：ZCode 缺 hermes turn_context 的每轮注入机制，
交互铁律全靠 AGENTS.md 文字（=0 约束力）。本 hook 补齐。
2026-08-05 修正：systemMessage 不在 ZCode hook output schema（strict），
改用 additionalContext（官方规范，diagnosing-hooks SKILL.md #4）。
"""
import json
import sys
from datetime import datetime

# Task1541 D 件：当前时间注入——hook context 的 currentDate 无时分，主控曾
# 提前 2h 宣布"14:00 已到"（时间感知漂移两次实锤）。每轮注入 YYYY-MM-DD HH:MM。
NOW_PREFIX = f"⏰ 当前本地时间 {datetime.now():%Y-%m-%d %H:%M}（以本行为准，勿凭记忆推时刻）"

# 极简提醒（每轮注入，控制 <350 字符避免 context 膨胀）
REMINDER = """🔴 本轮强制（_shared.md §交互铁律）：
1. 动手前过 §0 分流：查 MEM 了吗？该用图谱用了吗？该派子 Agent 派了吗？
2. 回复必含【认知四象限·KK/KU/UK/UU】+【待决事项】两段
3. 一次只问一个问题（AskUserQuestion 单 question）
4. 改文件前 POST /session/op-log 声明意图
5. 子 Agent 只调研不改代码（代码走 dispatch）
6. 遇问题先搜 MEM(:11537/search 带 agent_id)，禁凭记忆干(B4)"""

# 高危模式（现有）
HIGH_RISK_KEYWORDS = ["继续", "全自动", "LOOP", "全面", "彻底"]

# Task452 铁律1：意图性指令词（易被当"全自动推进免提问例外"扩大解释）
INTENT_KEYWORDS = [
    "推实施", "推进", "做完", "搞定", "落盘", "收工", "今天先这样",
    "下一步", "然后", "接着",
]


def main():
    try:
        data = json.load(sys.stdin)
    except Exception:
        data = {}

    prompt = str(data.get("user_prompt", "") or data.get("prompt", ""))

    # 高危模式加强提醒（保留现有）
    extra = ""
    if any(k in prompt for k in HIGH_RISK_KEYWORDS):
        extra += "\n⚠️ 高危模式（LOOP/全自动/继续）：最易跳过 §0 分流+四象限。本轮务必先查 MEM 再动手。"

    # Task452 铁律1：意图性指令加强提醒
    intent_hits = [k for k in INTENT_KEYWORDS if k in prompt]
    if intent_hits:
        kw_display = intent_hits[0] if intent_hits else ""
        extra += (
            f"\n🔴 意图性指令检测（Task452 铁律1）：你说的\"{kw_display}\"是意图性指令，"
            "不是\"全自动推进免提问例外\"。\n"
            "强制：动手前先复述你对意图的理解（字面+真实意图+约束边界三层），确认到95%再行动。\n"
            "例外仅限：\"全自动推进\"/\"继续既定无脑执行\"（方案已95%确认过）。"
        )

    output = {
        "hookSpecificOutput": {
            "hookEventName": "UserPromptSubmit",
            "additionalContext": NOW_PREFIX + "\n" + REMINDER + extra,
        },
    }
    print(json.dumps(output, ensure_ascii=False))
    sys.exit(0)


if __name__ == "__main__":
    main()
