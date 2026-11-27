#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""selfcheck_evidence_guard.py — Task452 铁律4 hook：自检证据化（Stop hook）。

触发：主控生成回复结束时（Stop event）。
机制：检测回复缺"四象限/待决/证据"→ 警告。
设计：stopHookEndBehavior=continue 请求 Agent 重新考虑（不强制 block）。

2026-08-06 固化：自检走过场 = 认知诚实体系断链。
本 hook 把"自检证据化"从"靠自觉"升级到"代码强制提醒"。
"""
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path


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
        if isinstance(v, dict):
            for kk in ("content", "text", "value"):
                vv = v.get(kk)
                if isinstance(vv, str) and vv:
                    return vv
    return ""


def main():
    try:
        data = json.load(sys.stdin)
    except Exception:
        sys.exit(0)

    reply_text = _extract_reply_text(data)
    if not reply_text:
        sys.exit(0)  # 拿不到文本不强制

    # Task489 项4：四象限从"每轮强制"改"高风险决策触发式+证据绑定"
    # 高风险决策词触发（报完成/选方向/改范围/kill/被纠偏）
    HIGH_RISK = re.compile(
        r'完成|闭环|没问题|搞定|收工|done|已修复|已实现|选择|建议采用|决定|'
        r'扩展|缩减|超出本次|kill|终止|放弃|纠正|纠偏',
        re.IGNORECASE
    )
    is_high_risk = bool(HIGH_RISK.search(reply_text))

    # 待决事项：每次回复强制（不变）
    has_pending = "待决事项" in reply_text
    missing = []
    if not has_pending:
        missing.append("待决事项段")

    # 四象限 + 证据绑定：仅高风险决策时强制（Task489 v2.0 触发式）
    if is_high_risk:
        has_quadrant = "认知四象限" in reply_text or all(k in reply_text for k in ("KK", "KU"))
        has_evidence = bool(re.search(
            r"(MEM#\d+|文件:行|\.py:\d+|\.md:\d+|证据|实测|验证|grep|psql|curl|场景验证)",
            reply_text,
        ))
        if not has_quadrant:
            missing.append("认知四象限（高风险决策触发，需附证据绑定）")
        if not has_evidence:
            missing.append("证据绑定（高风险决策触发：MEM#/文件:行/实测/场景验证）")

    if missing:
        output = {
            "hookSpecificOutput": {
                "hookEventName": "Stop",
                "stopHookEndBehavior": "continue",
            },
            "additionalContext": (
                "🔴 自检证据化警告（Task452 铁律4）：回复缺：\n"
                + "\n".join(f"  - {m}" for m in missing)
                + "\n自检不能走过场——必须有证据支撑（不是\"我觉得\"）。补全后再结束。"
            ),
        }
        _write_trace(data, "selfcheck_evidence", missing)
        print(json.dumps(output, ensure_ascii=False))

    sys.exit(0)


if __name__ == "__main__":
    main()
