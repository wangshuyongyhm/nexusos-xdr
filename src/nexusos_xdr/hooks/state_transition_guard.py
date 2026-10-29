#!/usr/bin/env python3
"""state_transition_guard.py — PreToolUse hook (Bash matcher).

Task539 R7: dispatch 前强制 state_transition gate（LOOP 状态推进强制）。
检测 tool_input.command 含 dispatch 信号 → 查 events.jsonl 近 60s 有无
gate_check scenario=state_transition passed=True → 无则 deny。

设计依据：state 是概念的、无单一文件可拦——路径A 拦 dispatch 信号反向要求
state_transition gate（覆盖 LOOP 最关键状态推进边）。不拦非 dispatch 的 Bash。
"""
import collections
import json
import os
import re
import sys
from datetime import datetime, timezone, timedelta


# #75③ 文档/配置后缀——路径名（如 dispatch-orchestrator.md）里的 /dispatch 不是派发端点
_DOC_SUFFIX = r"\.(?:md|ya?ml|json|txt|py|sh|toml|plist)"


def _strip_quoted(s: str) -> str:
    """剥掉单/双引号内内容（层①）——引号内是教学文案/示例字面量，非真命令。"""
    return re.sub(r"'[^']*'|\"(?:[^\"\\]|\\.)*\"", " ", s)


def _curl_subcommands(cmd: str):
    """按 ; && || | 换行切子命令，yield curl 为首 token 的子命令（层②）。

    curl 必须出现在命令位（行首/分隔符后），而非 cat/echo/grep 等文本里的子串。
    T678b 修2（#39②/MEM#62544）：补命令替换形态——`VAR=$(curl …)` 的 token[0]
    是 `VAR=$(curl`（以变量名开头），原 lstrip("$(") 只剥前导字符，base 提取
    逃逸 → 真 dispatch 漏拦（08201651 两次实锤）。现按任务书正则
    `\$\(\s*curl\b` 识别 token[0]，命中视为 curl 子命令。
    """
    for sub in re.split(r";|&&|\|\||\||\n", cmd):
        tokens = sub.strip().split()
        if not tokens:
            continue
        base = tokens[0].rsplit("/", 1)[-1].lstrip("$(")  # /usr/bin/curl → curl
        if base == "curl":
            yield " ".join(tokens)
        elif re.search(r"\$\(\s*curl\b", tokens[0]):
            # VAR=$(curl …)——命令替换内嵌 curl，等价 curl 为命令位
            yield "curl " + " ".join(tokens[1:])


def _is_dispatch_command(cmd: str) -> bool:
    """检测是否为 dispatch 动作。

    2026-08-20 #75③ 三层收紧（08200833 一日三误拦固化）：
    a) MEM 教学文案含派发命令字样 b) 同命令 gate+dispatch 串联
    c) 路径名 dispatch-orchestrator.md 的 /dispatch 前缀。
    层① 文档后缀路径 + 引号内教学文本不拦；层② curl 必须是子命令首 token。
    """
    bare_cmd = _strip_quoted(cmd.lower())
    if re.search(r"start_tool\.sh\s+dispatch", bare_cmd):
        return True
    if "agent.send_task" in bare_cmd:
        return True
    for sub in _curl_subcommands(bare_cmd):
        if re.search(r"/dispatch[^\s]*" + _DOC_SUFFIX, sub):
            continue  # 层①：dispatch-*.md 等文档路径
        if "/dispatch" in sub:
            return True
    return False


def _has_recent_state_transition_gate(within_seconds: int = 60) -> bool:
    """查 events.jsonl 近 N 秒有无 gate_check scenario=state_transition passed=True。

    复用 Wave1 观测器的时区兜底逻辑（naive→UTC）。
    """
    tz8 = timezone(timedelta(hours=8))
    cutoff = datetime.now(tz8) - timedelta(seconds=within_seconds)

    # 261028 紧急修复（zcode3 留痕，工作区态）：G8 日切归档（Task1951）后 legacy
    # events.jsonl 启动即被迁移改名，guard 恒 False → 全体系 dispatch 被断。
    # 261028 二次补丁：1960 迁移把归档挪到 ~/.nexus/logs/（第三次"改存储没盘
    # 消费者"），candidates 双路径兜底。根治=guard import 包内 archive_path_today()
    # （随包走），已记 P1 单5。
    arch_env = os.environ.get("NEXUS_XDR_EVENTS_ARCHIVE")
    today = datetime.now(tz8).strftime("%Y-%m-%d")
    candidates = []
    if arch_env:
        candidates.append(os.path.join(os.path.expanduser(arch_env), today + ".jsonl"))
    candidates += [
        os.path.expanduser("~/.nexus/logs/xdr-events-archive/" + today + ".jsonl"),
        os.path.expanduser("~/.hermes/logs/xdr-events-archive/" + today + ".jsonl"),
        os.path.expanduser("~/.hermes/xdr-sessions/events.jsonl"),
    ]
    events_file = next((p for p in candidates if os.path.exists(p)), None)
    if events_file is None:
        return False

    try:
        with open(events_file, encoding="utf-8", errors="replace") as f:
            # tail 式流读最后 200 行（Task1958：禁 readlines 整读——事故期 7.8GB
            # 归档整读释放 3000 万 dict 霸占 GIL 致服务假死 20 分钟被迫重启）
            for line in reversed(list(collections.deque(f, maxlen=200))):
                try:
                    e = json.loads(line)
                    if e.get("type") != "gate_check":
                        continue
                    detail = e.get("detail", "") or ""
                    if "scenario=state_transition" not in detail:
                        continue
                    if "passed=True" not in detail:
                        continue
                    ts = e.get("ts", "")
                    if not ts:
                        continue
                    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    if dt > cutoff:
                        return True
                except (json.JSONDecodeError, ValueError, KeyError):
                    continue
    except Exception:
        # silent: 读档 IO 异常时按无近期 gate 处理（fail-closed 语义保持原设计，
        # 仅声明：内层单行解析异常已独立 continue，此处只兑 open/IO 错误）
        pass
    return False


def main():
    try:
        payload = json.load(sys.stdin)
    except Exception:
        sys.exit(0)  # 解析失败不拦截

    tool_input = payload.get("tool_input", {})
    command = tool_input.get("command", "")

    if not _is_dispatch_command(command):
        sys.exit(0)  # 非 dispatch 动作，放行

    if _has_recent_state_transition_gate():
        sys.exit(0)  # 有近期 state_transition gate，放行

    # 无 state_transition gate → deny
    msg = (
        "dispatch 前必须过 state_transition gate（LOOP 状态推进强制）。\n"
        "先调 gate_check(scenario=state_transition, "
        "context={from_state,to_state,session_id})，\n"
        "再 dispatch。R7 强制机制（Task539）。\n"
        "⚠️ gate 与 dispatch 必须分两次工具调用：同一条 Bash 里 && 串联时，"
        "本 hook 预检读不到 gate 结果，必被拦（#75③ 使用规范）。"
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
