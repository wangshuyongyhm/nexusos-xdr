#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""proc_origin.py — 服务端自己查写请求的进程来历（2611239006，根修第一步：只记录、不拦）。

为什么有它：身份核实这一层至今做在**客户端工具**里，判定发生在调用方自己的进程里——「进程自己
证明自己」。主控一晚攻破十种（环境变量、命令行、第 0 个参数、假 tmux、包内程序当祖先、手工放行
变量、同进程注入、出错放行…），原理上只防误操作。根修只有一条路：**让服务端自己取事实**。

一条统一规则（本单只记录，第二步才按它拒绝）：

    自报的名字，必须和内核看到的进程来历对得上。
      报主控名字 → 发请求的进程必须真的在主控进程之下；
      报执行者名字 E → 进程必须真的在执行者会话 E 里；
      对不上 / 判不出来 → 将来拒绝。

服务端能自己取到的事实（全部由本模块取，**不从请求体、请求头、工具参数里读任何当作来历的字段**）：
  1. 对端端口 → 用 `/usr/sbin/lsof -nP -iTCP:<port> -F pn` 查这条连接的持有进程；
  2. 进程号 → 控制终端（`ps -o tty=`）、父进程号（`ps -o ppid=`）、可执行文件真实路径（`proc_pidpath`）；
  3. 进程内调用（服务自己的代码调的）→ 服务自己的进程号，来历是「系统」。

红队五轮（任务 2611239004）对第 1 步的三条实测结论，本模块照做：
  - 按端口查会把**服务自己**那一侧也查进来（E1）→ 去掉服务进程（及同机其他监听同端口的进程）；
  - **一条连接可以有两个持有者**（描述符继承，E2）→ 持有者是集合，**多于一个一律按不满足处理**，
    但每个持有者的来历都记下来；
  - 查询那一刻持有者可能已经不在（客户端发完就退出，E3）→ 所以要在请求**一到**就查，
    一个持有者都查不到时，原因写「查不到持有者」而不是笼统的「判不出来」。

本单不拦任何请求：只计数、记结构化日志、并把来历并进已有的留痕（给结果写进结果行的 client 块；
派单/回调/签收写进台账已有的留痕位置）。查询有 2 秒超时，超时按「判不出来」；不做跨请求缓存。

🔴 防不住谁（不要美化）：
  1. 真的在主控进程之下的进程——那就是主控自己。
  2. 由执行者启动一个 ZCode 主程序实例、并让那个实例派生执行者的命令：红队只读查到本体的权限位允许
     用环境变量注入动态库、并且有本机通信入口。所以「可执行路径等于本体 + 父进程是 1」这条结构规则
     **挡不住会启动第二个实例的人**。第二步的收紧方向：只认登记过的那个主程序实例的进程号（本模块
     已经把查到的本体进程号留在留痕里，就是为了第二步能这么收），必要时再加 macOS 的「负责进程」。
  3. 服务进程自己被攻破。
  4. 本单只记录：交付之后，第 0 节列的那些口子一个都没关；它的价值是把「谁在从哪里调」变成服务端
     自己看到的事实。
"""
from __future__ import annotations

import json
import logging
import os
import subprocess
import threading
import time
from datetime import datetime, timezone
from typing import Iterable, Optional

logger = logging.getLogger("nexusos_xdr.proc_origin")

# ── 与服务端/客户端共用的一份常量（客户端 A/nexus_env.py 里是同一组，有一致性测试）──
ORCH_MAIN_BINARY = "/Applications/ZCode.app/Contents/MacOS/ZCode"
LAUNCHD_PID = 1
ORCHESTRATORS = ("zcode", "zcode2", "zcode3")

# 三类来历
CLASS_ORCH = "orchestrator"
CLASS_EXEC = "executor"
CLASS_UNKNOWN = "unknown"

_MAX_DEPTH = 20
_QUERY_TIMEOUT = 2.0

# 只记录阶段的开关（性能对比用；第二步改成拒绝时这里会换成策略）
ENABLED = True

_START = time.time()
_LOCK = threading.Lock()
# counters[entry][class][matched] = n   matched ∈ {"yes","no","unknown"}
_COUNTERS: dict = {}
_RECENT_LIMIT = 20
_RECENT: list = []


# ══════════════════ 五个取事实的函数（都可注入，测试用）══════════════════

LOOPBACK_ADDRS = ("127.0.0.1", "::1", "localhost", "[::1]")


def _lsof_holders(port: int, server_pids: Iterable[int] = (), timeout: float = _QUERY_TIMEOUT,
                  server_port: int = 0) -> dict:
    """这条连接（对端端口=port）的持有进程集合。返回 {"holders": [...], "raw": str, "err": str}。

    解析只认：本地地址回环、本地端口 == port、状态已建立（`-F pn` 的 p 行进程、n 行连接）。
    服务自己那一侧（server_pids 里）与同机其他监听同端口的进程由调用方剔除——本函数只负责取。
    """
    try:
        r = subprocess.run(["/usr/sbin/lsof", "-nP", f"-iTCP:{int(port)}",
                            "-sTCP:ESTABLISHED", "-F", "pn"],
                           capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        return {"holders": [], "raw": "", "err": "timeout"}
    except Exception as e:  # noqa: BLE001 — 取不到就按判不出来
        return {"holders": [], "raw": "", "err": f"{type(e).__name__}: {e}"}
    holders, cur = [], None
    for line in (r.stdout or "").splitlines():
        if line.startswith("p"):
            try:
                cur = int(line[1:])
            except ValueError:
                cur = None
        elif line.startswith("n"):
            # n127.0.0.1:54321->127.0.0.1:19998（LISTEN 的没有 "->"，一律不算）
            if cur is None:
                continue
            addr = line[1:]
            if "->" not in addr:
                continue
            local, remote = addr.split("->", 1)
            try:
                laddr, lport_s = local.rsplit(":", 1)
                _, rport_s = remote.rsplit(":", 1)
                lport, rport = int(lport_s), int(rport_s)
            except Exception:  # noqa: BLE001 — 解析不出来就跳过这一行
                continue
            # 2611239906：四个条件都要满足（原来只比了本地端口，会造出假的「多持有者」）
            #   ① 本地地址是回环 ② 本地端口 == 对端端口 ③ 远端端口 == 本服务端口 ④ 状态已建立
            if laddr.strip("[]") not in [a.strip("[]") for a in LOOPBACK_ADDRS]:
                continue
            if lport != int(port):
                continue
            if int(server_port) and rport != int(server_port):
                continue
            if cur not in holders:
                holders.append(cur)
    return {"holders": holders, "raw": (r.stdout or "")[:2000],
            "err": "" if r.returncode == 0 else f"rc={r.returncode}"}


def _proc_pidpath(pid: int) -> str:
    """内核报告的可执行文件真实路径（进程改不了）。取不到返回空串。"""
    try:
        import ctypes
        buf = ctypes.create_string_buffer(4096)
        n = ctypes.CDLL("/usr/lib/libproc.dylib").proc_pidpath(int(pid), buf, 4096)
        return buf.value.decode("utf-8", "replace") if n > 0 else ""
    except Exception:  # noqa: BLE001 — 取不到就是空串（调用方判不了）
        return ""


def _ps_field(pid: int, field: str, timeout: float = _QUERY_TIMEOUT) -> str:
    """`ps -o <field>= -p <pid>`。取不到返回空串。"""
    try:
        r = subprocess.run(["/bin/ps", "-o", f"{field}=", "-p", str(int(pid))],
                           capture_output=True, text=True, timeout=timeout)
        return (r.stdout or "").strip()
    except Exception:  # noqa: BLE001
        return ""


# 2611239906：tmux 路径不要只写死一个——先按 PATH 找，再回落几个常见位置；
# 都找不到时把原因留在 _TMUX_LAST_ERR 里，由来历的 reason 带出去（不悄悄当成「没有执行者会话」）
_TMUX_CANDIDATES = ("/opt/homebrew/bin/tmux", "/usr/local/bin/tmux", "/usr/bin/tmux",
                    "/opt/local/bin/tmux")
_TMUX_LAST_ERR = ""


def _tmux_bin() -> str:
    """取 tmux 可执行文件路径：PATH 优先（与客户端 nexus_env 的按名调用保持一致），再回落。"""
    import shutil
    found = shutil.which("tmux")
    if found:
        return found
    for c in _TMUX_CANDIDATES:
        if os.path.exists(c):
            return c
    return ""


def _tmux_panes(timeout: float = _QUERY_TIMEOUT) -> dict:
    """真实 tmux 服务（默认套接字，显式 -S、去掉 TMUX）的窗格清单 {tty: session}。"""
    global _TMUX_LAST_ERR
    bin_path = _tmux_bin()
    if not bin_path:
        _TMUX_LAST_ERR = "找不到 tmux（PATH 与常见位置都没有）"
        logger.warning("[2611239906] %s", _TMUX_LAST_ERR)
        return {}
    env = {k: v for k, v in os.environ.items() if k != "TMUX"}
    sock = f"/tmp/tmux-{os.getuid()}/default"
    try:
        r = subprocess.run([bin_path, "-S", sock, "list-panes", "-a",
                            "-F", "#{pane_tty} #{session_name}"],
                           capture_output=True, text=True, timeout=timeout, env=env)
        panes = {}
        for line in (r.stdout or "").splitlines():
            parts = line.split()
            if len(parts) == 2 and parts[0].startswith("/dev/"):
                panes[parts[0][5:]] = parts[1]
        return panes
    except Exception as e:  # noqa: BLE001 — 没有 tmux / 取不到 → 空表（原因带走）
        _TMUX_LAST_ERR = f"tmux 查询失败: {type(e).__name__}"
        return {}


def _registry_lookup(name: str) -> Optional[dict]:
    """登记表里这个会话名的条目（会话必须在登记表里且不可主控才算执行者会话）。"""
    try:
        from nexusos_xdr.agents import AGENT_REGISTRY, may_orchestrate
        if name not in AGENT_REGISTRY:
            return None
        if may_orchestrate(name):
            return {"name": name, "may_orchestrate": True}
        return {"name": name, "may_orchestrate": False}
    except Exception as e:  # noqa: BLE001 — 登记表读不到按判不出来
        logger.warning("[2611239006] 登记表读不到（按判不出来）: %s: %s", type(e).__name__, e)
        return None


# ══════════════════ 进程号 → 来历 ══════════════════

def origin_of_pid(pid: int, pathof=None, psofield=None, panes=None) -> dict:
    """沿父进程链判一个进程的来历。返回 {
        "class": orchestrator|executor|unknown, "session": str, "orchestrator_pid": int|None,
        "chain": [ {pid, path, ppid, tty} … ], "reason": str }
    """
    pathof = _proc_pidpath if pathof is None else pathof
    psofield = _ps_field if psofield is None else psofield
    panes = _tmux_panes() if panes is None else panes
    chain, ttys = [], []
    cur = int(pid)
    orch_pid = None
    for _ in range(_MAX_DEPTH):
        if cur <= 0:
            break
        path = pathof(cur) or ""
        ppid_s = psofield(cur, "ppid")
        tty = (psofield(cur, "tty") or "").strip()
        try:
            ppid = int(ppid_s) if ppid_s else -1
        except ValueError:
            ppid = -1
        chain.append({"pid": cur, "path": path, "ppid": ppid, "tty": tty})
        if tty and tty != "??" and tty not in ttys:
            ttys.append(tty)
        # 主控：路径等于本体 且 父进程是 1（结构规则；第二步要收紧成「登记过的那个实例」）
        if path == ORCH_MAIN_BINARY and ppid == LAUNCHD_PID and orch_pid is None:
            orch_pid = cur
        if ppid <= LAUNCHD_PID:
            break
        cur = ppid
    if orch_pid is not None:
        return {"class": CLASS_ORCH, "session": "", "orchestrator_pid": orch_pid,
                "chain": chain, "reason": "ancestor_is_orchestrator_main"}
    for tty in ttys:
        sess = panes.get(tty)
        if not sess:
            continue
        ent = _registry_lookup(sess)
        if ent and not ent.get("may_orchestrate"):
            return {"class": CLASS_EXEC, "session": sess, "orchestrator_pid": None,
                    "chain": chain, "reason": f"pane_tty={tty}"}
    reason = "no_orchestrator_ancestor_no_executor_pane"
    if _TMUX_LAST_ERR:
        reason = f"{reason}（{_TMUX_LAST_ERR}）"
    return {"class": CLASS_UNKNOWN, "session": "", "orchestrator_pid": None,
            "chain": chain, "reason": reason}


def resolve(peer_addr: str, peer_port: int, server_port: int,
            server_pids: Iterable[int] = (), holders_fn=None, origin_fn=None) -> dict:
    """对端 → 持有者集合 → 来历。红队三条结论都在这里实现。"""
    holders_fn = _lsof_holders if holders_fn is None else holders_fn
    origin_fn = origin_of_pid if origin_fn is None else origin_fn
    try:
        got = holders_fn(int(peer_port), server_port=int(server_port))
    except TypeError:  # 旧签名/测试注入的假件只收一个参数
        got = holders_fn(int(peer_port))
    raw_holders = list(got.get("holders") or [])
    server_set = {int(p) for p in server_pids}
    # ① 去掉服务自己那一侧（以及同机监听同端口的其他进程）
    holders = [h for h in raw_holders if h not in server_set]
    out = {"peer": f"{peer_addr}:{peer_port}", "server_port": int(server_port),
           "raw_holders": raw_holders, "holders": holders,
           "query_err": got.get("err", ""), "holder_origins": []}
    if got.get("err") and not raw_holders:
        out.update({"class": CLASS_UNKNOWN, "session": "", "orchestrator_pid": None,
                    "reason": f"lsof_error: {got.get('err')}"})
        return out
    if not holders:
        out.update({"class": CLASS_UNKNOWN, "session": "", "orchestrator_pid": None,
                    "reason": "no_holder" if raw_holders or not got.get("err")
                    else "no_holder(lsof 无输出)"})
        return out
    # ③ 多于一个持有者 → 一律按不满足处理，但每个都记下来
    origins = [origin_fn(h) for h in holders]
    out["holder_origins"] = origins
    if len(holders) > 1:
        out.update({"class": CLASS_UNKNOWN, "session": "", "orchestrator_pid": None,
                    "reason": "multiple_holders"})
        return out
    o = origins[0]
    out.update({"class": o["class"], "session": o.get("session", ""),
                "orchestrator_pid": o.get("orchestrator_pid"),
                "reason": o.get("reason", ""), "chain": o.get("chain", [])})
    return out


# ══════════════════ 规则与计数 ══════════════════

def expected_origin(claimed: str) -> str:
    """按第 4 节那张表，自报名字「应有的来历」：主控名单里的名字→orchestrator，其余→executor:<名字>。"""
    name = str(claimed or "").strip()
    if not name:
        return "unknown"
    if name in ORCHESTRATORS:
        return CLASS_ORCH
    return f"{CLASS_EXEC}:{name}"


def matches(claimed: str, origin: dict) -> Optional[bool]:
    """自报的名字与来历对不对得上。True/False；判不出来时 None。"""
    exp = expected_origin(claimed)
    cls = origin.get("class")
    if cls == CLASS_UNKNOWN:
        return None
    if exp == CLASS_ORCH:
        return cls == CLASS_ORCH
    if exp.startswith(f"{CLASS_EXEC}:"):
        want = exp.split(":", 1)[1]
        return cls == CLASS_EXEC and str(origin.get("session")) == want
    return None


# 2611239906 熔断：连续 3 次查询出错/超时 → 60 秒内跳过查询，来历记「判不出来」，
# 原因写「熔断中」。这是**只记录阶段的有意取舍**（坏掉系统工具不能拖慢所有写请求）。
# 🔴 到强制阶段这条要重新想：那时「判不出来」= 拒绝，熔断就等于把全部写请求都拒了——
#    第二步必须改成「熔断期间只对带凭据的请求放行」或干脆去掉熔断（留给第二步定）。
_BREAKER = {"fail": 0, "open_until": 0.0}
BREAKER_THRESHOLD = 3
BREAKER_COOLDOWN_S = 60.0


def _now() -> float:
    """当前时间（秒，单调）；测试可替换以注入时间。"""
    return time.monotonic()


def breaker_state() -> dict:
    """熔断状态（测试与状态接口用）。"""
    now = _now()
    return {"fail_streak": _BREAKER["fail"],
            "open": now < _BREAKER["open_until"],
            "cooldown_left": round(max(0.0, _BREAKER["open_until"] - now), 1)}


def _breaker_trip() -> None:
    _BREAKER["fail"] += 1
    if _BREAKER["fail"] >= BREAKER_THRESHOLD:
        _BREAKER["open_until"] = _now() + BREAKER_COOLDOWN_S
        _BREAKER["fail"] = 0
        logger.warning("[2611239906] 来历查询连续出错 %d 次，熔断 %.0f 秒（期间跳过查询）",
                       BREAKER_THRESHOLD, BREAKER_COOLDOWN_S)


def _breaker_ok() -> None:
    _BREAKER["fail"] = 0


def _record(entry: str, claimed: str, origin: dict, matched: Optional[bool],
            extra: Optional[dict] = None) -> None:
    key = str(entry or "?")
    m = "unknown" if matched is None else ("yes" if matched else "no")
    cls = str(origin.get("class") or CLASS_UNKNOWN)
    with _LOCK:
        _COUNTERS.setdefault(key, {}).setdefault(cls, {"yes": 0, "no": 0, "unknown": 0})[m] += 1
        if matched is False:
            sample = {"ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                      "entry": key, "claimed": claimed, "origin_class": cls,
                      "origin_session": origin.get("session", ""),
                      "orchestrator_pid": origin.get("orchestrator_pid"),
                      "reason": origin.get("reason", ""),
                      "would_reject": True}
            if extra:
                sample.update({k: v for k, v in extra.items() if k in
                               ("peer", "holders", "holder_origins", "query_err")})
            _RECENT.append(sample)
            del _RECENT[:-_RECENT_LIMIT]
    if matched is False:
        logger.warning(
            "[2611239006] 来历对不上（只记录，未拦） entry=%s claimed=%s origin=%s session=%s "
            "orchestrator_pid=%s reason=%s", key, claimed, cls, origin.get("session", ""),
            origin.get("orchestrator_pid"), origin.get("reason", ""))
    elif matched is None:
        logger.info("[2611239006] 来历判不出来 entry=%s claimed=%s reason=%s",
                    key, claimed, origin.get("reason", ""))


def observe(entry: str, claimed: str, request=None, peer=None, server_port: Optional[int] = None,
            server_pids: Iterable[int] = (), resolve_fn=None) -> dict:
    """入口埋点：查来历 + 计数 +（由调用方把返回值并进已有留痕）。**绝不抛异常、绝不改返回**。"""
    if not ENABLED:
        return {"class": CLASS_UNKNOWN, "reason": "disabled"}
    if _now() < _BREAKER["open_until"]:
        origin = {"class": CLASS_UNKNOWN, "session": "", "orchestrator_pid": None,
                  "reason": "熔断中（连续出错后跳过查询）", "matched": None, "would_reject": False}
        _record(entry, claimed, origin, None)
        return origin
    try:
        if peer is None and request is not None:
            peer = getattr(request, "client", None) or getattr(request, "client_address", None)
        if not peer:
            origin = {"class": CLASS_UNKNOWN, "session": "", "orchestrator_pid": None,
                      "reason": "no_peer_info"}
        else:
            addr, port = peer[0], int(peer[1])
            sp = int(server_port) if server_port else int(getattr(request, "url", None) and 0 or 0)
            resolve_fn = resolve if resolve_fn is None else resolve_fn
            origin = resolve_fn(addr, port, sp, tuple(server_pids))
        # 查询本身出错/超时 → 计入熔断（不静默）
        if str(origin.get("reason", "")).startswith("lsof_error") or \
                str(origin.get("reason", "")) == "no_holder(lsof 无输出)":
            _breaker_trip()
        else:
            _breaker_ok()
        matched = matches(claimed, origin)
        _record(entry, claimed, origin, matched,
                extra={"peer": origin.get("peer"), "holders": origin.get("holders"),
                       "holder_origins": origin.get("holder_origins"),
                       "query_err": origin.get("query_err")})
        origin["matched"] = matched
        origin["would_reject"] = (matched is False)
        return origin
    except Exception as e:  # noqa: BLE001 — 埋点绝不影响业务（但仍然要能看见）
        _breaker_trip()
        logger.warning("[2611239006] 来历埋点自身异常（忽略，不影响请求）: %s: %s",
                       type(e).__name__, e)
        return {"class": CLASS_UNKNOWN, "reason": f"observe_error: {type(e).__name__}",
                "matched": None, "would_reject": False}


def trace_block(origin: dict) -> dict:
    """并进已有留痕用的紧凑块（给结果写进 client，派单/回调/签收写进台账留痕位）。"""
    if not isinstance(origin, dict):
        return {}
    chain = origin.get("chain") or []
    return {
        "origin_class": origin.get("class"),
        "origin_session": origin.get("session", ""),
        "origin_reason": origin.get("reason", ""),
        "orchestrator_pid": origin.get("orchestrator_pid"),
        "origin_matched": origin.get("matched"),
        # 第二步要靠「登记过的那个主程序实例」收紧，这里先把根进程号留全
        "origin_root_pid": chain[-1]["pid"] if chain else None,
        "origin_holders": origin.get("holders"),
    }


def stats() -> dict:
    """状态接口用：进程启动以来的计数 + 最近 20 条对不上的样例。只读。"""
    with _LOCK:
        counters = json.loads(json.dumps(_COUNTERS))
        recent = json.loads(json.dumps(_RECENT))
    totals = {"yes": 0, "no": 0, "unknown": 0}
    for entry in counters.values():
        for cls in entry.values():
            for k in totals:
                totals[k] += int(cls.get(k, 0))
    return {
        "ok": True,
        "mode": "record_only",          # 本单只记录：任何情况下都不拦
        "uptime_seconds": round(time.time() - _START, 1),
        "totals": totals,
        "by_entry": counters,
        "recent_mismatches": recent,
        "breaker": breaker_state(),
        "constants": {"orchestrators": list(ORCHESTRATORS), "orchestrator_main": ORCH_MAIN_BINARY,
                      "launchd_pid": LAUNCHD_PID},
    }


# ══════════════════ 注册层包装（一处接入，不改 handler 体）══════════════════
# 各入口「自报的名字」从哪个字段取（见参考件第 4 节的规则表）
CLAIM_KEYS = {
    "dispatch": ("caller",),
    "cancel": ("agent", "caller"),
    "ack": ("agent",),
    "ack_delivery": ("agent",),
    "register": ("agent",),
    "heartbeat": ("agent",),
    "unregister": ("agent",),
    "session_event": ("agent",),
    "broadcast": ("sender", "agent"),
    "op_log": ("agent",),
    "callback": ("source", "session"),
    "apply_outcome": ("agent_id", "by"),
}


def wrap(handler_fn, entry: str, server_port: int = 19998):
    """把一个 HTTP 处理函数包成「先查来历、再照原样执行」。返回与接入前完全一样。

    Starlette 的 Request 会缓存 body，所以这里先读一次 body 取自报名字，处理函数再读还是同一份。
    """
    async def wrapper(request):
        try:
            body = await request.json()
        except Exception:  # noqa: BLE001 — 读不出 body 就当没有自报名字（处理函数自己会报 400）
            body = {}
        if not isinstance(body, dict):
            body = {}
        claimed = ""
        for k in CLAIM_KEYS.get(entry, ("agent",)):
            v = str(body.get(k) or "").strip()
            if v:
                claimed = v
                break
        # 2611239906：查询挪到线程里——lsof 43ms + ps 23ms 不能占住事件循环；
        # 但**顺序不变**：先查完再执行处理函数（红队要求「请求一到就查」）。
        try:
            import anyio
            _fn = lambda: observe(entry, claimed, request=request, server_port=server_port)  # noqa: E731
            if hasattr(anyio, "to_thread") and hasattr(anyio.to_thread, "run_sync"):
                await anyio.to_thread.run_sync(_fn)
            else:  # 老版本 anyio：退回同步（记一行，别静默）
                logger.warning("[2611239906] anyio.to_thread 不可用，来历查询退回同步")
                _fn()
        except Exception as _w_e:  # noqa: BLE001 — 包装层绝不改变返回
            logger.warning("[2611239906] 来历查询调度异常（忽略）: %s", type(_w_e).__name__)
        return await handler_fn(request)
    try:
        wrapper.__name__ = getattr(handler_fn, "__name__", "wrapped")
        wrapper.__doc__ = getattr(handler_fn, "__doc__", None)
    except Exception:  # noqa: BLE001 — silent: 只是名字，失败无影响
        pass
    return wrapper
