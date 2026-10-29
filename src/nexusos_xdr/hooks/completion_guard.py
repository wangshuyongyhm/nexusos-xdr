#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""completion_guard.py — Task452 铁律2 hook：完成度闭环（Stop hook）。

触发：主控生成回复结束时（Stop event）。
机制：检测回复含"落盘/收工"等意图词 → 查 xdr task_state 有无 PENDING/RUNNING → 有则警告。
设计：stopHookEndBehavior=continue 请求 Agent 重新考虑（不强制 block）。

2026-08-06 固化：未完成提落盘 = 认知诚实体系断链。
本 hook 把"完成度闭环"从"靠自觉"升级到"代码强制提醒"。

局限（方向X 接受）：拿不到 ZCode 内部 todo（阶段 todo），只查 dispatch task_state。
"""
import json
import sys
import urllib.request


# 落盘/收工意图词
CLOSURE_KEYWORDS = [
    "落盘", "收工", "今天先这样", "今天到此", "会话结束", "本次完成",
]


def _extract_reply_text(data: dict) -> str:
    """从 Stop hook stdin 提取回复文本（字段名实测多版本兼容）。"""
    for k in ("response", "reply", "output", "text", "assistant_response", "message"):
        v = data.get(k)
        if isinstance(v, str) and v:
            return v
        # 嵌套 dict 也试一层
        if isinstance(v, dict):
            for kk in ("content", "text", "value"):
                vv = v.get(kk)
                if isinstance(vv, str) and vv:
                    return vv
    return ""


def _query_active_tasks() -> list:
    """查 xdr :19998/task_state，返 PENDING/RUNNING 任务清单。失败返 []。"""
    try:
        resp = urllib.request.urlopen(
            "http://127.0.0.1:19998/task_state", timeout=3
        )
        data = json.loads(resp.read())
        # task_state 返回 {"active": [...], "recent": [...]} 或 [list]
        if isinstance(data, list):
            return [t for t in data if isinstance(t, dict)
                    and t.get("status") in ("PENDING", "RUNNING")]
        if isinstance(data, dict):
            active = data.get("active", []) or []
            return [t for t in active if isinstance(t, dict)
                    and t.get("status") in ("PENDING", "RUNNING")]
    except Exception:
        pass
    return []


def main():
    try:
        data = json.load(sys.stdin)
    except Exception:
        sys.exit(0)

    reply_text = _extract_reply_text(data)

    # 检测落盘/收工意图
    has_closure_intent = any(kw in reply_text for kw in CLOSURE_KEYWORDS)
    if not has_closure_intent:
        sys.exit(0)

    # 查 xdr task_state
    active = _query_active_tasks()

    if active:
        summary_lines = []
        for t in active[:3]:
            tid = t.get("task_id", "?")
            status = t.get("status", "?")
            result = t.get("result", {}) or {}
            summary = str(result.get("summary", ""))[:60] if isinstance(result, dict) else ""
            summary_lines.append(f"  - {tid}: {status} {summary}")
        suffix = f"\n  ...等 {len(active)} 个" if len(active) > 3 else ""

        output = {
            "hookSpecificOutput": {
                "hookEventName": "Stop",
                "stopHookEndBehavior": "continue",  # 请求继续（让 Agent 重新考虑）
            },
            "additionalContext": (
                f"🔴 完成度闭环警告（Task452 铁律2）：你说\"落盘/收工\"，"
                f"但 xdr 有 {len(active)} 个 PENDING/RUNNING 任务未完成：\n"
                + "\n".join(summary_lines) + suffix + "\n"
                "未 100% 完成不提落盘（认知诚实体系）。先完成或明确暂停原因。"
            ),
        }
        print(json.dumps(output, ensure_ascii=False))

    sys.exit(0)


if __name__ == "__main__":
    main()
