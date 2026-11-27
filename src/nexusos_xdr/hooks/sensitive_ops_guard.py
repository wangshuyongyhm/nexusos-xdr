#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""sensitive_ops_guard.py — PreToolUse：敏感操作拦截 + 带编号的邮件询问（Task 2611230846）。

挂在 ZCode 的 Bash 组与 Edit|Write 组末尾，对每次工具调用做三选一：
  普通操作 → 什么都不做（零输出、零痕迹、退出码 0）
  log 类   → 记一行 auto，放行
  ask/deny → 拦下；ask 会生成申请编号并发邮件请用户决定
规则全在 nexusos_xdr/etc/sensitive-ops.yaml，本文件不写死任何业务规则。

绝不拦 Agent 之间的日常协作：广播、点名、派单、回调、commit 都不在任何规则里。
外发（邮件/事件/门禁）全部限时且失败不影响拦截结果；dryrun 下一律不外发。
"""
import json
import os
import subprocess
import sys
import urllib.request
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))  # 同目录 sensitive_ops.py（三处同步）
import sensitive_ops as so  # noqa: E402

MAIL_TIMEOUT, EVENT_TIMEOUT, GATE_TIMEOUT = 5, 3, 5
MAIL_CODE = ("import sys; sys.path.insert(0, sys.argv[1]); import notify_push; "
             "notify_push.deliver(sys.argv[2], sys.argv[3])")


def _resolve_identity(stdin_data: dict) -> tuple:
    """(实例名, 命中来源)。取法与 broadcast_guard 一致：环境变量 > 会话映射表 > 缺省 zcode。"""
    injected = os.environ.get("NEXUS_ZCODE_AGENT_NAME")
    if injected:
        return injected, "env"
    try:
        rows = (Path.home() / ".zcode" / "nexus-session-map.tsv").read_text(encoding="utf-8").splitlines()
    except OSError:
        rows = []
    for session, source in ((str(stdin_data.get("session_id") or "").strip(), "stdin"),
                            (os.environ.get("CLAUDE_SESSION_ID", "").strip(), "env_session")):
        if not session:
            continue
        for line in rows:
            sid, _, name = line.partition("\t")
            if sid == session and name.strip():
                return name.strip(), source
    return "zcode", "default"


def _mask(text: str, limit: int = 200) -> str:
    """命令/路径打码：家目录换 ~，超长截断。邮件正文里不出现完整绝对路径。"""
    s = str(text or "").replace(str(Path.home()), "~")
    return s if len(s) <= limit else s[:limit] + "…"


def _send_mail(title: str, body: str) -> str:
    """限时发信。返回投递结果字符串，失败也只回报不抛出。"""
    try:
        x = str(so.vault_root() / "05-System/自动化脚本/pypi/nexusos-xdr/src/nexusos_xdr")
        r = subprocess.run([sys.executable, "-c", MAIL_CODE, x, title, body],
                           capture_output=True, text=True, timeout=MAIL_TIMEOUT)
        return f"ok rc={r.returncode}"
    except Exception as e:
        return f"err {type(e).__name__}"


_EVENT_AGENT = ""      # 2611230863：当前触发方 Agent（handle() 里设置），发事件时用作 agent 名


def _send_event(detail: str) -> str:
    # 2611230863：原来写死 "sensitive-ops"，它不在登记表里，/session/event 身份校验直接 400，
    # 事件从来没进过事件流。改用触发方 Agent 的名字；取不到时退到 "xdr"（在登记表里）。
    who = (_EVENT_AGENT or "").strip() or "xdr"
    try:
        body = json.dumps({"agent": who, "type": "need_decision", "detail": detail},
                          ensure_ascii=False).encode()
        req = urllib.request.Request("http://127.0.0.1:19998/session/event", data=body,
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=EVENT_TIMEOUT) as resp:
            return f"ok {resp.status}"
    except Exception as e:
        return f"err {type(e).__name__}"


def _gate(agent: str, req_id: str) -> str:
    """preferences §356 要求的 user_decision 留痕。失败只记不拦。"""
    try:
        engine = so.vault_root() / "05-System/自动化脚本/nexusos_xdr/gate_engine.py"
        ctx = json.dumps({"agent": agent, "risk_level": "high_irreversible", "request_id": req_id})
        r = subprocess.run([sys.executable, str(engine), "user_decision", ctx],
                           capture_output=True, text=True, timeout=GATE_TIMEOUT)
        passed = '"passed": true' in (r.stdout or "")
        return f"rc={r.returncode} passed={passed}"
    except Exception as e:
        return f"err {type(e).__name__}"


def _deny(msg: str) -> str:
    return json.dumps({"hookSpecificOutput": {"hookEventName": "PreToolUse",
                                              "permissionDecision": "deny",
                                              "additionalContext": msg}}, ensure_ascii=False)


def handle(data: dict, cfg=None) -> str:
    """返回要打印的 JSON 字符串（None = 什么都不输出）。测试直接调它。"""
    tool = str(data.get("tool_name") or "")
    ti = data.get("tool_input") or {}
    if tool == "Bash":
        command, path = str(ti.get("command") or ""), None
    elif tool in ("Edit", "Write", "MultiEdit", "NotebookEdit"):
        command, path = None, str(ti.get("file_path") or "")
    else:
        return None
    if not command and not path:
        return None

    cfg = cfg if cfg is not None else so.load_config()
    agent, _source = _resolve_identity(data)
    global _EVENT_AGENT
    _EVENT_AGENT = agent        # 2611230863：发事件用触发方名字（取不到时 _send_event 退到 xdr）
    text = command or path
    dry = os.environ.get("NEXUS_SENSITIVE_DRYRUN") == "1"
    dry_file = os.environ.get("NEXUS_SENSITIVE_REQUESTS_FILE", "").strip()

    def _write(rec):
        """dryrun 时只写到指定文件（没指定就不写），保持「测试不落真账」。"""
        if not dry:
            so.append_request(rec, cfg=cfg)
        elif dry_file:
            so.append_request(rec, cfg=cfg, path=dry_file)

    # ① 授权名单：命中就只留痕
    entry = so.allow_hit(text, cfg=cfg)
    if entry:
        _write({"req_id": so.new_req_id(), "status": "auto", "rule": "allow-list", "agent": agent,
                "command": command, "path": path, "at": so.now_iso(),
                "note": f"授权名单 {entry.get('id')} 命中，放行"})
        return None

    # ② 规则匹配
    # 2611230863：用 classify_ex 拿「为什么放行」的说明（mem_search_loopback），写进 log 记录
    rid, action, action_note = so.classify_ex(command=command, path=path, cfg=cfg)
    if not rid:
        return None  # 普通操作：零输出零痕迹

    if action == "log":
        _write({"req_id": so.new_req_id(), "status": "auto", "rule": rid, "agent": agent,
                "command": command, "path": path, "at": so.now_iso(),
                "note": action_note})
        return None

    if action == "deny":
        req = so.new_req_id()
        _write({"req_id": req, "status": "denied", "rule": rid, "agent": agent,
                "command": command, "path": path, "at": so.now_iso()})
        return _deny(f"⛔ 敏感操作直接拒绝（规则 {rid}）：删库/无 WHERE 删除在任何库都不可接受（B2）。\n"
                     f"申请编号 {req}（已记 denied，不会发邮件）。")

    # ③ 批准后重试放行一次（Task 2611230872）：同指纹 + 已批准 + 未用过 + 24h 内 → 放行并标已用
    _fp = so.command_fingerprint(agent, text)
    try:
        _recs = so.load_requests(cfg=cfg)
    except Exception:
        _recs = {}
    _ok_req = so.reusable_approval(_recs, _fp, agent)
    if _ok_req:
        try:
            so.set_status(_ok_req, "consumed", note="批准后重试放行一次", actor=agent)
            so.append_request({"req_id": _ok_req, "consumed_at": so.now_iso()}, cfg=cfg)
        except Exception:
            pass
        # 留痕必须用「新的 req_id」：账本按 req_id 归并，沿用旧编号会把那条申请的
        # consumed 状态覆盖成 auto（实测踩过）。被引用的申请编号写在 note 里。
        _write({"req_id": so.new_req_id(), "status": "auto", "rule": rid, "agent": agent,
                "command": command, "path": path, "at": so.now_iso(),
                "note": f"按申请 {_ok_req} 放行（用户已批准，只放行这一次）"})
        return None
    _no_req = so.rejected_recent(_recs, _fp, agent)
    if _no_req:
        _write({"req_id": so.new_req_id(), "status": "rejected-repeat", "rule": rid, "agent": agent,
                "command": command, "path": path, "at": so.now_iso(),
                "note": f"同指纹被拒过: {_no_req}"})
        return _deny(f"⛔ 这条命令你最近被用户拒绝过（{_no_req}），不要换个写法重试。\n"
                     f"如确有需要，请向用户说明理由后再试，或换一个能被批准的做法。")

    # ④ ask：生成编号 → 发邮件 → 发事件 → user_decision 留痕 → 拦下
    req = so.new_req_id()
    _write({"req_id": req, "status": "pending", "rule": rid, "agent": agent,
            "command": command, "path": path, "at": so.now_iso(), "fp": _fp})
    outbound = {"mail": "skipped_dryrun", "event": "skipped_dryrun", "gate": "skipped_dryrun"}
    if not dry:
        what = f"实例 {agent} 想执行：{_mask(text)}"
        why = f"命中敏感规则 {rid}"
        how = f"回复邮件写 APPROVE {req}，或终端执行 sensitive-ops.sh approve {req}"
        outbound["mail"] = _send_mail(f"[需你决定] {req}", f"{what}。{why}。{how}。")
        outbound["event"] = _send_event(f"敏感操作待决定 {req}（{rid}）：{_mask(text, 80)}")
        outbound["gate"] = _gate(agent, req)
        so.append_request({"req_id": req, "outbound": outbound, "at": so.now_iso()}, cfg=cfg)
    return _deny(f"🔴 敏感操作已拦下（规则 {rid}），已发邮件请用户决定，申请编号 {req}。\n"
                 f"可先做别的事，用 `sensitive-ops.sh status {req}` 查询；批准后重试。")


def main() -> int:
    if sys.stdin is None or sys.stdin.isatty():
        return 0
    try:
        data = json.load(sys.stdin)
        if not isinstance(data, dict):
            return 0
    except Exception:
        return 0  # 解析失败一律放行（防误伤）
    out = handle(data)
    if out:
        print(out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
