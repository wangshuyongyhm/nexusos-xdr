#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""sensitive_ops.py — 敏感操作分类器 + 申请/授权命令行（Task 2611230846）。

单一判断入口：守卫（sensitive_ops_guard.py）与命令行（nexusos_xdr/sensitive-ops.sh）都调这里，
不许各写一套匹配逻辑。规则全部来自 nexusos_xdr/etc/sensitive-ops.yaml（用户可改）。

判定顺序（守卫侧）：授权名单 → 逐条规则 → 都不命中就什么都不做。
写文件命中路径规则时命令规则不适用；Bash 命令里出现的路径也不去套路径规则。
"""
import fnmatch
import hashlib
import json
import os
import re
import secrets
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

SEVERITY = {"deny": 3, "ask": 2, "log": 1}
CONFIG_REL = "05-System/自动化脚本/nexusos_xdr/etc/sensitive-ops.yaml"


def vault_root() -> Path:
    """找 Vault 根：环境变量 > 包内单源锚点 > 从本文件向上找 > ~/.nexus/vault-root 哨兵。找不到返回空路径。"""
    env = os.environ.get("NEXUS_VAULT_ROOT", "").strip()
    if env and Path(env).is_dir():
        return Path(env)
    try:
        from nexusos_xdr._vault_env import VAULT_ROOT as _VR
        p = Path(str(_VR))
        if "nonexistent" not in str(p) and p.is_dir():
            return p
    except Exception:
        pass
    for base in Path(__file__).resolve().parents:
        if (base / "05-System/NEXUS-CONSTRAINT").is_dir():
            return base
    sentinel = Path.home() / ".nexus" / "vault-root"
    try:
        p = Path(sentinel.read_text(encoding="utf-8").strip())
        if p.is_dir():
            return p
    except OSError:
        pass
    return Path("")


def config_path() -> Path:
    env = os.environ.get("NEXUS_SENSITIVE_OPS_CONFIG", "").strip()
    if env:
        return Path(env).expanduser()
    return vault_root() / CONFIG_REL


def _expand(s) -> str:
    """只对路径类字段展开 <R> 与 ~（命令正则不能展开）。"""
    return str(s).replace("<R>", str(vault_root())).replace("~", str(Path.home()), 1) if isinstance(s, str) else str(s)


def load_config(path=None) -> dict:
    p = Path(path).expanduser() if path else config_path()
    try:
        import yaml
        cfg = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    except Exception:
        return {"rules": [], "allow_file": "~/.nexus/xdr/sensitive-ops-allow.json",
                "requests_file": "~/.nexus/xdr/sensitive-requests.jsonl", "_error": str(p)}
    for rule in cfg.get("rules") or []:
        if isinstance(rule.get("paths"), list):
            rule["paths"] = [_expand(x) for x in rule["paths"]]
    for key in ("allow_file", "requests_file"):
        if cfg.get(key):
            cfg[key] = _expand(cfg[key])
    return cfg


def _hit(patterns, text, expand=False) -> bool:
    if not text or not patterns:
        return False
    t = str(text)
    for pat in patterns:
        p = _expand(pat) if expand else str(pat)
        if p and re.search(p, t):
            return True
    return False


def _path_hit(patterns, path: str) -> bool:
    """路径规则用 glob 语义；'*' 一律跨目录（fnmatch 行为），够用且不会漏。"""
    return any(p and fnmatch.fnmatchcase(path, p) for p in (patterns or []))


def _push_action(command: str, rule: dict) -> str:
    trusted = [str(x) for x in (rule.get("trusted_remotes") or [])]
    toks = [t for t in command.split() if not t.startswith("-")]
    remote = toks[toks.index("push") + 1] if "push" in toks and toks.index("push") + 1 < len(toks) else ""
    return "log" if remote and remote in trusted else "ask"


def _prod_action(command: str, rule: dict) -> str:
    hosts = [str(x) for x in (rule.get("prod_hosts") or [])]
    m = re.search(r"[\w.+-]+@([\w.-]+)", command)
    if not hosts:
        return "ask"
    if m and m.group(1) in hosts:
        return "ask"
    return "log" if m else "ask"



# ---------------- 凭据类操作的结构判断（Task 2611230863） ----------------
# 判断标准：凭据类操作只在三种情况要问 —— ①值会进入对话/日志 ②值会离开本机 ③凭据文件被改。
# 读本 Agent 自己的检索凭据并只发给本机 11537 的检索服务，三种都不是 → 放行并留痕（log）。
# 实现原则：不看「命令里出现了什么字」，而看「读到的凭据去了哪里」。
_READ_CMDS = {"cat", "less", "more", "head", "tail", "grep", "sed", "awk",
              "strings", "xxd", "base64"}
_CRED_TAIL_RE = re.compile(r"(^|/)05-System/Agent/[^/\s]+/search_cred\.txt$")
_LOOPBACK = ("127.0.0.1:11537", "localhost:11537")
_CRED_NAME_RE = re.compile(r"(cred|secret|token|\.pem$|\.key$|id_rsa)", re.I)
_BARE_SECRET_FILES = {".env", ".netrc", ".pgpass", "id_rsa", ".git-credentials"}
_FORBIDDEN_TRANSPORT = re.compile(r"\b(ssh|scp|rsync|nc|netcat|telnet|ftp|git)\b")
_SAFE_PIPE_TARGETS = {"python3", "python", "jq", "head"}


def _is_cred_path(tok: str) -> bool:
    """参数像不像「凭据文件路径」：含 / 或本身就是 .env/.netrc/.pgpass/id_rsa 这类完整文件名。"""
    a = str(tok).strip("\"'")
    base = a.rsplit("/", 1)[-1]
    if a in _BARE_SECRET_FILES or base in _BARE_SECRET_FILES:
        return True
    if "/" not in a:
        return False
    return bool(_CRED_NAME_RE.search(base))


def _read_cred_files(command: str) -> list:
    """找出「真正去读凭据类文件」的位置：返回 [(变量名或None, 路径, 所在行)]。

    只认两种形态：① 行首 `VAR=…`（或裸命令）带着真实读命令；② 行内嵌在 `$( … )` 里的读命令。
    出现在 Python 源码、字符串字面量、注释里的字面量不算 —— 它们不是「被执行的首词」。
    """
    hits = []
    for raw in (command or "").splitlines():
        line = raw.strip()
        if not line:
            continue
        var, body = None, line
        m = re.match(r"^(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)=(.*)$", line)
        if m:
            var, body = m.group(1), m.group(2)
        core = body.strip()
        if core.startswith("$(") and core.endswith(")"):
            core = core[2:-1].strip()
        toks = core.replace("(", " ").replace(")", " ").split()
        while toks and "=" in toks[0] and not toks[0].startswith("-"):
            toks = toks[1:]
        path = None
        if toks and toks[0].strip("\"'") in _READ_CMDS:
            for a in toks[1:]:
                if a.startswith("-"):
                    continue
                if _is_cred_path(a):
                    path = a.strip("\"'")
                    break
        if path:
            hits.append((var, path, line))
            continue        # 本行已有结论，不再扫 $()，避免同一处读被算两次
        for mm in re.finditer(r"\$\(([^()]*)\)", line):
            stoks = mm.group(1).strip().split()
            if not stoks or stoks[0].strip("\"'") not in _READ_CMDS:
                continue
            for a in stoks[1:]:
                if a.startswith("-"):
                    continue
                if _is_cred_path(a):
                    hits.append((None, a.strip("\"'"), line))
                    break
            break
    return hits


def _var_uses(command: str, var: str) -> list:
    pat = re.compile(r"\$\{?" + re.escape(var) + r"\}?")
    return [ln for ln in (command or "").splitlines() if pat.search(ln)]


def _cred_header_values(line: str) -> list:
    return [m.group(2) for m in re.finditer(r"-H\s+(['\"])X-Credential:\s*([^'\"]*)\1", line)]


def _var_only_in_cred_header(line: str, var: str) -> bool:
    pat = re.compile(r"\$\{?" + re.escape(var) + r"\}?")
    leftover = line
    for m in re.finditer(r"-H\s+(['\"])X-Credential:\s*([^'\"]*)\1", line):
        leftover = leftover.replace(m.group(0), " ")
    return not pat.search(leftover)


def _curl_output_ok(line: str) -> bool:
    """curl 的输出只能直出，或接 python3/jq/head 这类格式化；不许写文件、不许 tee。"""
    if re.search(r"\btee\b", line):
        return False
    if re.search(r"(?<![0-9&])>\s*[^\s|&]", line):
        return False
    m = re.search(r"\|\s*([\w./-]+)", line)
    if not m:
        return True
    return Path(m.group(1)).name in _SAFE_PIPE_TARGETS


def _network_loopback_only(command: str) -> bool:
    """命令里出现的网络地址只能是本机 11537；出现别的 http(s) 或 host:port 就不满足。"""
    urls = re.findall(r"https?://[^\s\"')\]]+", command or "")
    for u in urls:
        if not any(host in u for host in _LOOPBACK):
            return False
    for host, _port in re.findall(r"\b([a-zA-Z0-9][\w.-]*):(\d{2,5})\b", command or ""):
        if f"{host}:{_port}" not in _LOOPBACK:
            return False
    return not _FORBIDDEN_TRANSPORT.search(command or "")


def _cred_action(command: str, rule: dict):
    """返回 "log"（只发本机检索服务，放行留痕）或 "ask"（三种情况之一）。None=没读到凭据。"""
    reads = _read_cred_files(command)
    if not reads:
        return None
    if not _network_loopback_only(command):
        return "ask"
    for var, path, line in reads:
        if not _CRED_TAIL_RE.search(path):
            return "ask"            # 读的不是本 Agent 的检索凭据
        if var:
            for use in _var_uses(command, var):
                if "curl" not in use:
                    return "ask"     # 凭据变量在本机检索之外被用到
                if not _var_only_in_cred_header(use, var):
                    return "ask"     # echo/printf/拼串/写文件/别的头
                if not _curl_output_ok(use):
                    return "ask"
        else:
            if "curl" not in line:
                return "ask"
            if not any(path.split("/")[-1] in v or path in v for v in _cred_header_values(line)):
                return "ask"         # 单行写法：读出来的值不在 X-Credential 头里
            if not _curl_output_ok(line):
                return "ask"
    return "log"


def classify_ex(command=None, path=None, cfg=None):
    """返回 (规则编号, 动作, 说明)。动作 "log" 时说明为 mem_search_loopback（放行留痕的理由）。"""
    cfg = cfg if cfg is not None else load_config()
    found = []
    for rule in cfg.get("rules") or []:
        rid, action = rule.get("id"), rule.get("action", "ask")
        if command:
            if _hit(rule.get("never_ask_bash"), command):
                continue
            if rid == "git_push" and _hit(rule.get("bash"), command):
                found.append((rid, _push_action(command, rule), None))
            elif rid == "prod_ops" and _hit(rule.get("bash"), command):
                found.append((rid, _prod_action(command, rule), None))
            elif rid == "money_or_creds" and _read_cred_files(command):
                _ca = _cred_action(command, rule) or "ask"
                found.append((rid, _ca, "mem_search_loopback" if _ca == "log" else None))
            elif _hit(rule.get("deny_bash"), command):
                found.append((rid, "deny", None))
            elif _hit(rule.get("local_log_bash"), command):
                found.append((rid, "log", None))
            elif _hit(rule.get("bash"), command):
                found.append((rid, action, None))
        if path and _path_hit(rule.get("paths"), path):
            found.append((rid, action, None))
    if not found:
        return None, None, None
    rid, action, note = max(found, key=lambda x: SEVERITY.get(x[1], 0))
    return rid, action, note

def classify(command=None, path=None, cfg=None):
    """返回 (规则编号, 动作)；不命中返回 (None, None)。多条命中取最严（deny>ask>log）。

    真正的判定在 classify_ex；这里保留二元组签名给既有调用方/测试用。
    """
    rid, action, _note = classify_ex(command=command, path=path, cfg=cfg)
    return rid, action



# ---------------- 批准后重试放行一次（Task 2611230872） ----------------
def command_fingerprint(agent: str, command: str) -> str:
    """命令指纹：对「Agent 名 + 换行 + 命令全文」算 SHA-256 取前 16 位。

    覆盖被拦命令的完整文本——命令有任何改动都算另一条（不做归一化/模糊匹配）。
    """
    raw = f"{agent}\n{command}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:16]


def _is_fresh(rec: dict, now: float, window_hours: float = 24.0) -> bool:
    ts = rec.get("approved_at") or rec.get("at") or ""
    try:
        return (now - _epoch(ts)) <= window_hours * 3600
    except Exception:
        return False


def reusable_approval(records: dict, fp: str, agent: str, now: float = None,
                      window_hours: float = 24.0):
    """找「同一个人 + 同指纹 + 已批准 + 还没用过 + 24h 内」的最新一条，返回 req_id 或 None。"""
    now = time.time() if now is None else now
    best, best_at = None, -1.0
    for rid, rec in (records or {}).items():
        if rec.get("status") != "approved":
            continue
        if rec.get("fp") != fp or (rec.get("agent") or "") != agent:
            continue
        if rec.get("consumed_at"):
            continue
        if not _is_fresh(rec, now, window_hours):
            continue
        at = _epoch(rec.get("approved_at") or rec.get("at"))
        if at >= best_at:
            best, best_at = rid, at
    return best


def rejected_recent(records: dict, fp: str, agent: str, now: float = None,
                    window_hours: float = 24.0):
    """同指纹最近被拒绝过的申请（24h 内），返回 req_id 或 None。"""
    now = time.time() if now is None else now
    best, best_at = None, -1.0
    for rid, rec in (records or {}).items():
        if rec.get("status") != "rejected":
            continue
        if rec.get("fp") != fp or (rec.get("agent") or "") != agent:
            continue
        if not _is_fresh(rec, now, window_hours):
            continue
        at = _epoch(rec.get("at"))
        if at >= best_at:
            best, best_at = rid, at
    return best

def new_req_id() -> str:
    return "REQ-" + secrets.token_hex(4)


def now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def _epoch(ts) -> float:
    try:
        return datetime.fromisoformat(str(ts)).timestamp()
    except Exception:
        return 0.0


# ---------------- 批量授权名单 ----------------

def allow_path(cfg=None) -> Path:
    env = os.environ.get("NEXUS_SENSITIVE_ALLOW_FILE", "").strip()
    cfg = cfg if cfg is not None else load_config()
    return Path(env or cfg.get("allow_file") or "~/.nexus/xdr/sensitive-ops-allow.json").expanduser()


def load_allow(cfg=None) -> list:
    try:
        data = json.loads(allow_path(cfg).read_text(encoding="utf-8"))
        return data.get("entries", []) if isinstance(data, dict) else []
    except Exception:
        return []


def _save_allow(entries, cfg=None) -> None:
    p = allow_path(cfg)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps({"entries": entries}, ensure_ascii=False, indent=1), encoding="utf-8")


def allow_hit(text, entries=None, cfg=None):
    """命中未过期、未撤销的授权条目则返回该条目，否则 None。"""
    for e in (entries if entries is not None else load_allow(cfg)):
        if e.get("revoked"):
            continue
        if e.get("expires_at") and _epoch(e["expires_at"]) <= time.time():
            continue
        if e.get("pattern") and fnmatch.fnmatchcase(str(text), str(e["pattern"])):
            return e
    return None


# ---------------- 申请记录（JSONL，只追加；读时按 req_id 折叠成最后状态） ----------------

def requests_path(cfg=None) -> Path:
    env = os.environ.get("NEXUS_SENSITIVE_REQUESTS_FILE", "").strip()
    cfg = cfg if cfg is not None else load_config()
    return Path(env or cfg.get("requests_file") or "~/.nexus/xdr/sensitive-requests.jsonl").expanduser()


def append_request(rec: dict, cfg=None, path=None) -> None:
    p = Path(path).expanduser() if path else requests_path(cfg)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")


def load_requests(cfg=None, path=None) -> dict:
    """按 req_id 折叠：后写的状态覆盖先写的（批准/拒绝就是追加一条同编号的新状态）。"""
    p = Path(path).expanduser() if path else requests_path(cfg)
    folded = {}
    try:
        lines = p.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return folded
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(rec, dict) and rec.get("req_id"):
            folded[rec["req_id"]] = {**folded.get(rec["req_id"], {}), **rec}
    return folded


def set_status(req_id: str, status: str, note: str = "", actor: str = "") -> bool:
    records = load_requests()
    if req_id not in records:
        return False
    append_request({"req_id": req_id, "status": status, "at": now_iso(),
                    "actor": actor or os.environ.get("USER", "user"), "note": note})
    return True


# ---------------- 命令行 ----------------

def _usage() -> int:
    print("用法: sensitive-ops.sh {list|status REQ|approve REQ [--note 文字]|reject REQ [--note 文字]|"
          "wait REQ [--timeout 秒]|allow add --path 通配 --hours N --reason 文字|allow list|allow revoke 编号|"
          "check \"命令\"}")
    return 2


def main(argv) -> int:
    if not argv:
        return _usage()
    action, rest = argv[0], argv[1:]
    if action == "list":
        pend = [r for r in load_requests().values() if r.get("status") == "pending"]
        for r in sorted(pend, key=lambda x: str(x.get("at"))):
            print(f"{r['req_id']}  {r.get('at','')}  {r.get('agent','')}  {r.get('rule','')}  {str(r.get('command') or r.get('path') or '')[:60]}")
        if not pend:
            print("（没有 pending 申请）")
        return 0
    if action in ("status", "approve", "reject", "wait"):
        if not rest:
            return _usage()
        req_id = rest[0]
        note = ""
        if "--note" in rest:
            i = rest.index("--note")
            note = rest[i + 1] if i + 1 < len(rest) else ""
        rec = load_requests().get(req_id)
        if action == "status":
            if not rec:
                print(f"不存在: {req_id}")
                return 3
            print(f"{req_id}  status={rec.get('status')}  rule={rec.get('rule')}  agent={rec.get('agent')}  at={rec.get('at')}")
            return 0
        if action in ("approve", "reject"):
            if not rec:
                print(f"不存在: {req_id}")
                return 3
            set_status(req_id, "approved" if action == "approve" else "rejected", note=note)
            print(f"{req_id} -> {'approved' if action == 'approve' else 'rejected'}")
            return 0
        # wait：每秒查一次
        timeout = 60
        if "--timeout" in rest:
            i = rest.index("--timeout")
            timeout = int(rest[i + 1]) if i + 1 < len(rest) else 60
        if not rec:
            print(f"不存在: {req_id}")
            return 3
        for _ in range(max(1, timeout)):
            cur = load_requests().get(req_id, {}).get("status")
            if cur == "approved":
                print(f"{req_id} approved")
                return 0
            if cur == "rejected":
                print(f"{req_id} rejected")
                return 4
            time.sleep(1)
        print(f"{req_id} 超时（{timeout}s 内未见决定，仍为 {load_requests().get(req_id, {}).get('status')}）")
        return 5
    if action == "allow":
        if not rest:
            return _usage()
        sub = rest[0]
        if sub == "add":
            pattern = hours = ""; reason = ""
            args = rest[1:]
            for flag, name in (("--path", "pattern"), ("--hours", "hours"), ("--reason", "reason")):
                if flag in args:
                    i = args.index(flag)
                    val = args[i + 1] if i + 1 < len(args) else ""
                    if name == "pattern":
                        pattern = val
                    elif name == "hours":
                        hours = val
                    else:
                        reason = val
            if not pattern or not hours:
                return _usage()
            entries = load_allow()
            entry = {"id": "ALW-" + secrets.token_hex(4), "pattern": pattern,
                     "expires_at": (datetime.now(timezone.utc) + timedelta(hours=float(hours))).isoformat(timespec="seconds"),
                     "reason": reason, "created_at": now_iso()}
            entries.append(entry)
            _save_allow(entries)
            print(json.dumps(entry, ensure_ascii=False))
            return 0
        if sub == "list":
            for e in load_allow():
                flag = "revoked" if e.get("revoked") else ("expired" if _epoch(e.get("expires_at")) <= time.time() else "active")
                print(f"{e.get('id')}  {flag}  到期 {e.get('expires_at')}  {e.get('pattern')}  {e.get('reason','')}")
            return 0
        if sub == "revoke":
            if len(rest) < 2:
                return _usage()
            entries = load_allow()
            keep = [e for e in entries if e.get("id") != rest[1]]
            if len(keep) == len(entries):
                print(f"不存在: {rest[1]}")
                return 3
            _save_allow(keep)
            print(f"{rest[1]} 已撤销")
            return 0
        return _usage()
    if action == "check":
        if not rest:
            return _usage()
        target = " ".join(rest)
        rid, act = classify(command=target)
        print(f"{rid or '（不命中）'}  action={act or 'none'}")
        return 0
    return _usage()


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
