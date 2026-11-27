#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""email_inbound — IMAP 回信接收桥（2611192240 §F，用户定向「你能接收我的回邮件对吗」）。

链路：scheduler 日频轮询（默认 5min，可 NEXUS_EMAIL_INBOUND_POLL_SEC 调）→
IMAP4_SSL 拉新 UID → 分类（指令回信/重复/陌生发件/非指令）→
指令回信：① POST xdr 事件（type=email_inbound）② 调目标 agent 的
on_mention.sh（stdin=事件 JSON，复用广播 pending_file 唤醒契约零改动）。

凭据：复用 ~/.nexus/secrets/push_email.json；可选键 imap_host/imap_port/
imap_auth_code 覆盖，缺省按 smtp 域推导（smtp.qq.com → imap.qq.com:993）。
推导不可用=凭据阻塞：probe/poll 返回 {"status":"blocked", "activation": ...}
如实上报，不臆造（任务书 §F 原话）。

分类与防重复（§F 对抗）：
  - UID 台账（state email_inbound_state.json，processed 上限 500）防重复回信
  - 发件人 ≠ 收件邮箱（to_addr）→ 陌生发件，静默留档不路由
  - 非指令文本（无 APPROVE/REJECT/按X/选项N/xdr 任务 ID/回xx：）→ 静默留档
  - 系统自发邮件（来源头 X-NEXUS-Origin: system，或主题以系统前缀开头）且不是回信 → 静默留档不路由
    （收件箱=发件箱，系统告警邮件曾被当成用户回信指令；回信与行首明确的 APPROVE/REJECT 永远往下判，
    判定见 classify_inbound）
纯标准库。CLI:
  python3 -m nexusos_xdr.email_inbound --probe      # 凭据/连通探测（只登录不取信）
  python3 -m nexusos_xdr.email_inbound --poll-once  # 单轮收信
  python3 -m nexusos_xdr.email_inbound --register   # 注册 scheduler 轮询行
"""
from __future__ import annotations

import email as _email
import email.header
import email.utils
import imaplib
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

from nexusos_common import subpath  # 1960：锚点单源

_NOTIFY_DIR = Path(os.environ["NEXUS_XDR_NOTIFY_DIR"]) if os.environ.get("NEXUS_XDR_NOTIFY_DIR") else subpath("xdr/notify")
STATE_FILE_DEFAULT = _NOTIFY_DIR / "email_inbound_state.json"
CRED_FILE_DEFAULT = Path(os.environ.get("NEXUS_XDR_PUSH_EMAIL_FILE")
                         or subpath("secrets/push_email.json"))
XDR_EVENT_URL = "http://127.0.0.1:19998/session/event"
AGENT_SELF = "omp"                      # 事件署名（已注册 agent；append_event 校验）
DEFAULT_TARGET = "zcode"                # 默认唤醒主控；指令点名其他 agent 则路由之
POLL_TASK_NAME = "nexus-email-inbound"
POLL_DSN_DEFAULT = "postgresql://localhost/nexus_mem"   # 与 scheduler.py 同源
PROCESSED_CAP = 500

# §F 指令形态：任务号（10 位日期串）/ xdr 任务 ID / APPROVE|REJECT / 按X / 选项N / 回xx：
# 2611230844 收紧：去掉「任意 10 位数字」——正文里出现日期串/任务号就命中太宽，系统邮件
# 尾部的回复指引+任务号让每封系统邮件都被当指令。裸任务号不再算指令；「按X」/xdr 任务 ID/
# 回xx： 等任务号回信形态照旧认得。原定义注释保留：
# INSTRUCTION_RE = re.compile(
#     r"APPROVE|REJECT|按\s*[A-Ca-c1-3]|选项\s*[0-9]|\b\d{10}\b|"
#     r"xdr-[a-z0-9]+-\d+-[a-f0-9]+|回\s*[a-z0-9]+\s*[：:]", re.IGNORECASE)
INSTRUCTION_RE = re.compile(
    r"APPROVE|REJECT|按\s*[A-Ca-c1-3]|选项\s*[0-9]|"
    r"xdr-[a-z0-9]+-\d+-[a-f0-9]+|回\s*[a-z0-9]+\s*[：:]", re.IGNORECASE)
AGENT_MENTION_RE = re.compile(r"回\s*([a-z0-9]+)\s*[：:]", re.IGNORECASE)

# 2611230844：系统外发邮件的来源标记头（notify_push.EmailChannel 每封都带；头名/值与发信端同名常量一致）。
# 收件箱=发件箱，入站见此头判「系统自发」归档，不当用户指令（回信除外，见 classify_inbound）。
ORIGIN_HEADER = "X-NEXUS-Origin"
ORIGIN_SYSTEM = "system"
# 2611230844：无标记头时的兜底——系统邮件的主题前缀（xdr 常驻进程重启前发出的邮件还不带头）。
# 归纳自事件归档里被误当指令的 10 封：「需你决定」= notify_push.IMMEDIATE_TITLE_PREFIX；
# 「NEXUS 日报」= daily_digest 标题；「NEXUS 告警消费」= alert_watchdog 推送标题；
# 「NEXUS 回信闭环测试」= 回信桥自测样张。str.startswith 需要元组，所以用元组不用列表。
SYSTEM_SUBJECT_PREFIXES = (
    "[需你决定]",
    "NEXUS 日报",
    "NEXUS 告警消费",
    "NEXUS 回信闭环测试",
)

# 2611230844 复核追加（主控裁定）：回信不算系统自发——带 In-Reply-To / References 头，或主题以 Re: 开头。
REPLY_SUBJECT_RE = re.compile(r"^\s*(?:re|回复)\s*[:：]", re.IGNORECASE)
# 2611230872 复核追加：转发 ≠ 用户本人的决定。主题以 Fwd/Fw/转发 开头的一律不算回信，
# 即使带着 References / In-Reply-To 头——转发件正文里可能整段引用别人的回信，
# 那行 APPROVE 不是本人在批准（主控实测：主题「转发：Re: x」+ References 头会被当成批准）。
# 同时覆盖「Re: Fwd: x」这种套着的写法（前导 re: 可选），只往更安全的方向收紧。
FORWARD_SUBJECT_RE = re.compile(r"^\s*(?:re\s*[:：]\s*)?(?:fwd|fw|转发)\s*[:：]", re.IGNORECASE)
# 正文里「明确」的同意/拒绝：某一行以 APPROVE / REJECT 开头（后面可跟申请编号等）。行首锚定是为了
# 不误吃系统邮件里的指引句（「回复 APPROVE 或 REJECT」「…或「APPROVE」」都在句子中间）。
EXPLICIT_APPROVAL_RE = re.compile(r"^[ \t\u3000]*(?:APPROVE|REJECT)(?![A-Za-z])", re.IGNORECASE | re.MULTILINE)


def _load_cred(path: Path) -> dict:
    need = ("smtp_host", "account", "auth_code", "to_addr")
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return raw if all(str(raw.get(k) or "").strip() for k in need) else {}


def imap_cfg(cred: dict) -> tuple:
    """(host, port, account, password)——imap_* 可选键覆盖，缺省按 smtp 域推导。"""
    domain = ".".join(str(cred["smtp_host"]).split(".")[-2:])
    host = str(cred.get("imap_host") or f"imap.{domain}")
    port = int(cred.get("imap_port") or 993)
    return host, port, str(cred["account"]), str(cred.get("imap_auth_code") or cred["auth_code"])


def _connect(cfg: tuple):
    """IMAP4_SSL 连接+登录（单测 monkeypatch 锚点）。返回已 login 的客户端。"""
    host, port, account, password = cfg
    cli = imaplib.IMAP4_SSL(host, port, timeout=15)
    cli.login(account, password)
    return cli


def _decode_header(raw) -> str:
    out = []
    for part in _email.header.decode_header(str(raw or "")):
        data, charset = part[0], part[1] or "utf-8"
        if isinstance(data, bytes):
            data = data.decode(charset, "replace")
        out.append(data)
    return "".join(out)


def _body_text(msg) -> str:
    """首个 text/plain 部分的前 2000 字（够指令匹配，不贪多解码）。"""
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_type() == "text/plain":
                payload = part.get_payload(decode=True) or b""
                return payload.decode(part.get_content_charset() or "utf-8", "replace")[:2000]
        return ""
    payload = msg.get_payload(decode=True) or b""
    return payload.decode(msg.get_content_charset() or "utf-8", "replace")[:2000]


def _load_state(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {"last_uid": 0, "processed": [], "counts": {"instructions": 0, "archived": 0, "repeats": 0}}


def _save_state(state: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    state["processed"] = state["processed"][-PROCESSED_CAP:]
    path.write_text(json.dumps(state, ensure_ascii=False, indent=1), encoding="utf-8")


def _archive(line: str, state: dict, execute: bool = True) -> None:
    """静默留档（§F 对抗：垃圾/非指令/陌生发件——不路由不丢弃）。"""
    if not execute:
        return
    arc = _NOTIFY_DIR / "email_inbound_archive.jsonl"
    try:
        arc.parent.mkdir(parents=True, exist_ok=True)
        with arc.open("a", encoding="utf-8") as f:
            f.write(json.dumps({"ts": time.strftime("%F %T"), "line": line[:300]},
                               ensure_ascii=False) + "\n")
        state["counts"]["archived"] = int(state["counts"].get("archived", 0)) + 1
    except OSError:
        pass  # 留档失败不影响主流程


def _post_event(detail: str) -> bool:
    body = json.dumps({"agent": AGENT_SELF, "type": "email_inbound",
                       "detail": detail[:2000], "target": ""},
                      ensure_ascii=False).encode()
    try:
        with urllib.request.urlopen(
                urllib.request.Request(XDR_EVENT_URL, data=body,
                                       headers={"Content-Type": "application/json"}),
                timeout=5) as resp:
            return resp.status == 200
    except (urllib.error.URLError, TimeoutError, OSError):
        return False


def _wake_agent(agent: str, event: dict) -> bool:
    """复用广播唤醒契约：on_mention.sh 读 stdin 事件 JSON → pending+wake flag。"""
    script = Path(__file__).resolve().parent / "agents" / agent / "on_mention.sh"
    if not script.exists():
        return False
    try:
        r = subprocess.run(["bash", str(script)], input=json.dumps(event, ensure_ascii=False),
                           capture_output=True, text=True, timeout=15)
        return r.returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        return False


def _route(instruction_text: str) -> dict:
    """指令 → (xdr 事件 + 唤醒)。点名 agent 有 on_mention 则路由之，否则默认主控。"""
    m = AGENT_MENTION_RE.search(instruction_text)
    target = m.group(1).lower() if m else DEFAULT_TARGET
    detail = f"[email_inbound] 用户回信指令：「{instruction_text.strip()[:200]}」"
    event = {"ts": time.strftime("%FT%T"), "agent": AGENT_SELF,
             "type": "email_inbound", "detail": detail, "target": ""}
    posted = _post_event(detail)
    woken = _wake_agent(target, event)
    return {"posted": posted, "woken": woken, "wake_target": target}


def _header(headers, name: str):
    """取一个头的值。email.message.Message 自带大小写不敏感；dict 入参也按不分大小写找。"""
    if headers is None:
        return None
    val = headers.get(name)
    if val is None and isinstance(headers, dict):
        val = next((v for k, v in headers.items() if str(k).lower() == name.lower()), None)
    return val


def _origin_header(headers) -> str:
    """来源标记头的值（小写去空白）。"""
    return str(_header(headers, ORIGIN_HEADER) or "").strip().lower()


def _is_reply(headers, subject) -> bool:
    """是不是回信：主题以 Re: / 回复： 开头，或带 In-Reply-To / References 头。

    2611230872 复核追加：主题以 Fwd/Fw/转发 开头的**不是**回信——转发不等于用户本人的决定，
    带不带引用头都不算（这样正文里被整段引进来的 APPROVE 就不会被当成批准）。
    """
    subj = str(subject or "")
    if FORWARD_SUBJECT_RE.match(subj):
        return False
    if _header(headers, "In-Reply-To") or _header(headers, "References"):
        return True
    return bool(REPLY_SUBJECT_RE.match(subj))



# ---------------- 回信里的 APPROVE / REJECT 作用到敏感操作申请（Task 2611230872） ----------------
REQ_ID_RE = re.compile(r"REQ-[0-9a-f]{8}(?![0-9a-f])")
DECISION_LINE_RE = re.compile(r"^[ \t\u3000]*(APPROVE|REJECT)(?![A-Za-z])", re.IGNORECASE)


def parse_req_decisions(subject, headers, body) -> list:
    """纯函数：从回信正文里收 [(req_id, 'approve'|'reject')]。

    严的三条：①必须是回信（In-Reply-To/References 或主题 Re:）；②决定必须在**行首**；
    ③编号必须完整（REQ- + 8 位小写十六进制，后面不能再跟十六进制字符）。
    系统自发邮件（没有回复头、主题不带 Re:）与正文中间提到的都不算。
    """
    if not _is_reply(headers, subject):
        return []
    out = []
    for line in str(body or "").splitlines():
        m = DECISION_LINE_RE.match(line)
        if not m:
            continue
        decision = "approve" if m.group(1).upper() == "APPROVE" else "reject"
        for rid in REQ_ID_RE.findall(line):
            out.append((rid, decision))
    return out


def apply_decisions(decisions, subject: str, execute: bool = True) -> dict:
    """把回信决定作用到申请记录：待决 → 批准/拒绝；未知/已结 → 只记一条提示。"""
    stats = {"applied": 0, "ignored": 0, "details": []}
    if not decisions:
        return stats
    try:
        sys.path.insert(0, str(Path(__file__).resolve().parent / "hooks"))
        import sensitive_ops as _so          # noqa: E402  与守卫同一份实现
    except Exception as exc:                 # 实现拿不到就只记，不装作处理过
        stats["details"].append(f"敏感操作模块不可用: {type(exc).__name__}")
        return stats
    note = f"用户邮件回信: {str(subject or '')[:40]}"
    for rid, decision in decisions:
        recs = _so.load_requests()
        cur = (recs.get(rid) or {}).get("status")
        if cur == "pending":
            if execute:
                _so.set_status(rid, "approved" if decision == "approve" else "rejected",
                               note=note, actor="email-reply")
            stats["applied"] += 1
            stats["details"].append(f"{rid} -> {decision}")
        else:
            if execute:
                _post_event(f"邮件回信提到未知或已结的申请: {rid}（当前状态 {cur or '不存在'}）")
            stats["ignored"] += 1
            stats["details"].append(f"{rid} 忽略（当前状态 {cur or '不存在'}）")
    return stats

def classify_inbound(sender, headers, subject, body, to_addr) -> tuple:
    """纯函数判定一封来信（不连网）→ (类别, 指令文本)。

    类别 ∈ stranger / system / instruction / non_instruction，按序首个命中：
      1. 发件人非空且 ≠ 收件地址 → stranger
      2. 不是回信，且头 X-NEXUS-Origin: system → system
      3. 正文某行以 APPROVE / REJECT 开头（EXPLICIT_APPROVAL_RE）→ instruction
      4. 不是回信，且主题以系统前缀开头（SYSTEM_SUBJECT_PREFIXES）→ system
      5. INSTRUCTION_RE 命中「主题+正文」→ instruction
      6. 其余 → non_instruction
    回信（In-Reply-To / References / 主题 Re:）永远不走 2、4 两条系统自发判定：客户端若把标记头
    透传进回信、或回信主题仍是系统前缀，用户的批准也不会被吞掉。
    只有 instruction 类的指令文本有值（主题+正文，与 _route 入参同源）。
    """
    sender = str(sender or "")
    if sender and sender.lower() != str(to_addr or "").lower():
        return "stranger", ""
    reply = _is_reply(headers, subject)
    if not reply and _origin_header(headers) == ORIGIN_SYSTEM:
        return "system", ""
    combined = f"{subject or ''}\n{body or ''}".strip()
    if EXPLICIT_APPROVAL_RE.search(str(body or "")):
        return "instruction", combined
    if not reply and str(subject or "").strip().startswith(SYSTEM_SUBJECT_PREFIXES):
        return "system", ""
    if INSTRUCTION_RE.search(combined):
        return "instruction", combined
    return "non_instruction", ""

def probe(cred_file: Path = CRED_FILE_DEFAULT) -> dict:
    """凭据/连通探测（§F：阻塞如实报，不臆造）。"""
    cred = _load_cred(cred_file)
    if not cred:
        return {"status": "blocked",
                "reason": f"凭据缺失或不完整（{cred_file}）",
                "activation": "补齐 smtp_host/account/auth_code/to_addr（现有发信已具备，此为防御分支）"}
    cfg = imap_cfg(cred)
    try:
        cli = _connect(cfg)
    except imaplib.IMAP4.error as e:
        return {"status": "blocked",
                "reason": f"IMAP 登录被拒（{type(e).__name__}）——授权码对 IMAP 无效或 IMAP 服务未开启",
                "activation": "邮箱网页版 → 设置 → 账户 → 开启「IMAP/SMTP 服务」；"
                              "如需单独授权码，写入 push_email.json 的 imap_auth_code 键",
                "endpoint": f"{cfg[0]}:{cfg[1]}"}
    except (OSError, TimeoutError) as e:
        return {"status": "blocked",
                "reason": f"IMAP 端点不可达（{type(e).__name__}: {e}）",
                "activation": f"确认 {cfg[0]}:{cfg[1]} 可达（或 push_email.json 配 imap_host 覆写推导值）"}
    finally:
        try:
            cli.logout()
        except Exception:
            pass
    return {"status": "ok", "endpoint": f"{cfg[0]}:{cfg[1]}"}


def poll_once(state_path: Path = STATE_FILE_DEFAULT,
              cred_file: Path = CRED_FILE_DEFAULT, execute: bool = True) -> dict:
    """单轮收信：拉新 UID → 分类 → 指令路由/静默留档。execute=False 干跑（只分类）。"""
    cred = _load_cred(cred_file)
    if not cred:
        return {"status": "blocked", "reason": "凭据缺失", "activation": probe.__doc__ or ""}
    cfg = imap_cfg(cred)
    state = _load_state(state_path)
    counts = {"polled": 0, "instructions": 0, "archived": 0, "repeats": 0, "system": 0}
    try:
        cli = _connect(cfg)
    except Exception as e:
        return probe(cred_file) if isinstance(e, imaplib.IMAP4.error) else {
            "status": "blocked", "reason": f"IMAP 连接失败（{type(e).__name__}: {e}）"}
    try:
        cli.select("INBOX")
        last = int(state.get("last_uid") or 0)
        typ, data = cli.uid("SEARCH", None, f"UID {last + 1}:*")
        uids = [int(u) for u in (data[0] or b"").split() if int(u) > last]
        processed = set(state.get("processed") or [])
        for uid in uids:
            counts["polled"] += 1
            if uid in processed:
                counts["repeats"] += 1
                continue
            typ, msg_data = cli.uid("FETCH", str(uid), "(RFC822)")
            if typ != "OK" or not msg_data or msg_data[0] is None:
                continue
            msg = _email.message_from_bytes(msg_data[0][1])
            sender = _email.utils.parseaddr(msg.get("From", ""))[1]
            subject = _decode_header(msg.get("Subject", ""))
            body = _body_text(msg)
            combined = f"{subject}\n{body}".strip()
            processed.add(uid)
            category, instruction_text = classify_inbound(
                sender, msg, subject, body, cred["to_addr"])
            # 2611230844：判定抽成纯函数 classify_inbound；原分支注释保留——
            # if sender and sender.lower() != str(cred["to_addr"]).lower():
            #     _archive(f"[陌生发件] {sender} | {combined[:120]}", state, execute)
            #     counts["archived"] += 1
            # elif INSTRUCTION_RE.search(combined):
            #     if execute:
            #         route = _route(combined)
            #         counts["instructions"] += 1
            #         counts["last_route"] = route
            #     else:
            #         counts["instructions"] += 1
            # else:
            #     _archive(f"[非指令] {sender} | {combined[:120]}", state, execute)
            #     counts["archived"] += 1
            if category == "stranger":
                _archive(f"[陌生发件] {sender} | {combined[:120]}", state, execute)
                counts["archived"] += 1
            elif category == "system":
                # 系统自发（收件箱=发件箱）：留档，不路由、不唤醒任何 Agent
                _archive(f"[系统自发] {subject[:120]}", state, execute)
                counts["system"] += 1
            elif category == "instruction":
                # Task 2611230872：先看这封回信是不是在批/拒敏感操作申请；是就到此为止
                _decisions = parse_req_decisions(subject, msg, body)
                if _decisions:
                    _stats = apply_decisions(_decisions, subject, execute)
                    counts["instructions"] += 1
                    counts["last_route"] = {"kind": "req_decision", **_stats}
                    _archive(f"[回信决定] {subject[:80]} | {_stats['details'][:3]}", state, execute)
                    state["last_uid"] = max(int(state.get("last_uid") or 0), uid)
                    continue
                if execute:
                    route = _route(instruction_text)
                    counts["instructions"] += 1
                    counts["last_route"] = route
                else:
                    counts["instructions"] += 1
            else:
                _archive(f"[非指令] {sender} | {combined[:120]}", state, execute)
                counts["archived"] += 1
            state["last_uid"] = max(int(state.get("last_uid") or 0), uid)
        state["processed"] = sorted(processed)
        for k in ("instructions", "archived", "repeats", "system"):
            state["counts"][k] = int(state["counts"].get(k, 0)) + counts[k]
        if execute:
            _save_state(state, state_path)
        cli.logout()
    except imaplib.IMAP4.error as e:
        return {"status": "error", "reason": f"IMAP 会话异常（{type(e).__name__}: {e}）"}
    return {"status": "ok", **counts}


def register_poll(interval_sec: int = None) -> dict:
    """注册 scheduler 轮询行（interval 参数化，默认 env NEXUS_EMAIL_INBOUND_POLL_SEC/300）。"""
    interval = interval_sec or int(os.environ.get("NEXUS_EMAIL_INBOUND_POLL_SEC", "300"))
    try:
        import psycopg2
    except ImportError:
        return {"ok": False, "error": "psycopg2 不可用（本机 nexus-rel venv 有）"}
    cmd = json.dumps({"command": f'"{sys.executable}" "{Path(__file__).resolve()}" --poll-once',
                      "timeout": 120}, ensure_ascii=False)
    conn = psycopg2.connect(os.environ.get("NEXUS_PG_DSN", POLL_DSN_DEFAULT))
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO scheduled_tasks
                        (name, action, action_args, interval_sec, recurring,
                         max_iterations, expire_at, next_run_at, run_count, created_at)
                    VALUES (%s, 'shell', %s::jsonb, %s, true, NULL, NULL, now() + interval '30 seconds', 0, now())
                    ON CONFLICT (name) DO UPDATE SET
                        action_args = EXCLUDED.action_args,
                        interval_sec = EXCLUDED.interval_sec,
                        next_run_at = now() + interval '30 seconds',
                        last_error = NULL
                    """, (POLL_TASK_NAME, cmd, interval))
    finally:
        conn.close()
    return {"ok": True, "name": POLL_TASK_NAME, "interval_sec": interval}


def baseline(state_path: Path = STATE_FILE_DEFAULT,
             cred_file: Path = CRED_FILE_DEFAULT) -> dict:
    """部署基线：last_uid 推到当前 INBOX 最大 UID（存量不回扫，只收上线后新回信）。"""
    cred = _load_cred(cred_file)
    if not cred:
        return {"status": "blocked", "reason": "凭据缺失"}
    cli = _connect(imap_cfg(cred))
    try:
        cli.select("INBOX")
        typ, data = cli.uid("SEARCH", None, "ALL")
        uids = [int(u) for u in (data[0] or b"").split()]
        top = max(uids) if uids else 0
        state = {"last_uid": top, "processed": [],
                 "counts": {"instructions": 0, "archived": 0, "repeats": 0,
                            "baseline_at": time.strftime("%F %T"),
                            "baseline_skipped": len(uids)}}
        _save_state(state, state_path)
        cli.logout()
    except imaplib.IMAP4.error as e:
        return {"status": "error", "reason": f"{type(e).__name__}: {e}"}
    return {"status": "ok", "baseline_uid": top, "skipped_existing": len(uids)}


def main(argv=None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if "--probe" in argv:
        result = probe()
        print(json.dumps(result, ensure_ascii=False, indent=1))
        return 0 if result.get("status") == "ok" else 1
    if "--poll-once" in argv:
        result = poll_once()
        print(json.dumps(result, ensure_ascii=False, indent=1))
        return 0 if result.get("status") == "ok" else 1
    if "--baseline" in argv:
        result = baseline()
        print(json.dumps(result, ensure_ascii=False, indent=1))
        return 0 if result.get("status") == "ok" else 1
    if "--register" in argv:
        result = register_poll()
        print(json.dumps(result, ensure_ascii=False, indent=1))
        return 0 if result.get("ok") else 1
    print(__doc__)
    return 0


if __name__ == "__main__":
    sys.exit(main())
