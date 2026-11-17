#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""nexusos_xdr.ack — P0-1 回执确认协议（ACK 链，task 2611111762）。

why：#108819 假发送 / #615 steer 假阴性 / patrol §1.6 盲重投（180s 无回调即
重投——「已读未回执」任务被双投，探针 task/2611111760 实证双投消费 2 遍）。
回执信号源弃 TUI 词法域：执行者收到任务书第一动作 POST /ack，xdr 凭回执
精确判定投递成败。

状态机（S3 同款纯增量字段——status 枚举禁动，哨兵 should_fire 硬编码）。
全部 ACK 态收在单一顶层字段 `ack`（dict），每次变迁一次 update_field 原子写
——不嵌套 _state_lock（flock 非重入，嵌套 update_field = 自死锁）：

    ack = {state: pending|acked|late, key, kind, attempts, deadline}
    ├─ register_delivery（dispatch send 成功后/重投时）
    ├─ POST /ack（key 匹配）→ state=acked（重复同键幂等 OK）
    ├─ 超 deadline + 无进度物证 + attempts<2 → retry（patrol 重投，同键）
    ├─ 超 deadline + 无进度物证 + attempts≥2 → FAILED（end_reason=delivery_failed）
    └─ 超 deadline + 有进度物证（callbacks 非空）→ late（仅落账一次）

- ACK 超时窗与执行超时分离：窗默认 300s（推导：task_state 近 400 单实测
  created→首回调 p50=40s/p90=118s/p95=306s——回执=小弟第一动作，取 p95 取整）；
  执行超时仍由 patrol 原逻辑管，互不挤占。
- 进度物证=callbacks 非空——与 patrol §1.6 池#238 同判据族，但从「无回调」
  升级为「无回执且无回调」双缺（已读未回执不再触发重投）。
- steer 链（kind="steer"）：steer 无 task_state 生命周期，走独立 jsonl 台账
  （~/.nexus/xdr/steer-ledger.jsonl）同幂等键语义；词法层修复在
  task/2611111761（steer.sh），协议层在本模块——分层衔接见两单 report。
"""
import datetime
import json
import os
import time
import uuid
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from nexusos_xdr import task_state

# ACK 窗（秒）：env 可覆盖（测试/紧急调参单一通道）
ACK_WINDOW_SEC = int(os.environ.get("NEXUS_ACK_WINDOW_SEC", "300"))
ACK_STATES = ("pending", "acked", "late")
ACK_KINDS = ("dispatch", "steer")

STEER_LEDGER = Path(os.environ.get("NEXUS_STEER_LEDGER")
                    or (Path.home() / ".nexus" / "xdr" / "steer-ledger.jsonl"))


def _deadline_iso(now: Optional[float] = None) -> str:
    dt = datetime.datetime.fromtimestamp((now or time.time()) + ACK_WINDOW_SEC,
                                         tz=datetime.timezone.utc)
    return dt.isoformat()


def _epoch(iso_ts: Any) -> float:
    if not iso_ts:
        return 0.0
    try:
        return datetime.datetime.fromisoformat(str(iso_ts)).timestamp()
    except ValueError:
        return 0.0


def register_delivery(task_id: str, kind: str = "dispatch",
                      now: Optional[float] = None) -> str:
    """登记一次投递（dispatcher 侧幂等）：pending/acked 任务重投复用同键。

    patrol 重投（§1.6 升级版）与本函数都走这里——键不变（executor 侧凭键
    识别重复投递：已 ack 的仅再回执不重执行），attempts 递增，deadline 重置。
    单 update_field 原子写（无锁嵌套）。
    """
    if kind not in ACK_KINDS:
        raise ValueError(f"未知 ack kind: {kind}（合法: {ACK_KINDS}）")
    state = task_state.load_state(task_id)
    if state is None:
        raise ValueError(f"任务不存在: {task_id}")
    ack = dict(state.get("ack") or {})
    key = ack.get("key")
    if not key or ack.get("state") not in ACK_STATES:
        key = f"ack-{uuid.uuid4().hex[:10]}"
        ack = {"key": key, "kind": kind, "attempts": 1}
    else:
        ack["attempts"] = int(ack.get("attempts") or 0) + 1
    # 已 acked 的重复登记保持 acked（不回退 pending——重复投递不改已确认事实）
    if ack.get("state") != "acked":
        ack["state"] = "pending"
    ack["deadline"] = _deadline_iso(now)
    task_state.update_field(task_id, "ack", ack)
    return key


def handle_ack(task_id: str, key: str, agent: str,
               kind: str = "dispatch") -> Dict[str, Any]:
    """处理执行者回执（POST /ack 落点）。

    幂等：同键重复 ack → ok+duplicate=True（executor 侧重复投递的「直接确认」）。
    FileNotFound=任务不存在（404 面）；PermissionError=键不匹配（400 面）；
    ValueError=参数缺失（400 面）。回执落账 events 流（探针 1760 结论②：
    ACK 必须落账可考古——2010 stderr 即焚教训）。
    """
    if kind not in ACK_KINDS:
        raise ValueError(f"未知 ack kind: {kind}（合法: {ACK_KINDS}）")
    state = task_state.load_state(task_id)
    if state is None:
        raise FileNotFoundError(f"任务不存在: {task_id}")
    ack = dict(state.get("ack") or {})
    expect_key = ack.get("key")
    if expect_key and key and key != expect_key:
        raise PermissionError(
            f"ack key 不匹配: 期望 {expect_key} 收到 {key}（task {task_id}）")
    if not expect_key and not key:
        raise ValueError("任务无投递登记且回执未带 key——拒绝盲 ack")

    already = ack.get("state") == "acked"
    if not already:
        ack.update({"state": "acked", "at": task_state.now_iso(), "agent": agent})
        task_state.update_field(task_id, "ack", ack)
    # 落账（重复回执也记——重复投递审计面）；落账失败不吞回执（warning 可观测）
    try:
        from nexusos_xdr.session_state import append_event
        append_event(agent, "ack",
                     f"[ack] {task_id} 投递回执（kind={kind}, key={key or '?'}, "
                     f"duplicate={already}）")
    except Exception as e:
        task_state.append_warning(task_id, f"[ack] {task_id} 回执落账失败: {e}")
    return {"ok": True, "task_id": task_id, "acked": True,
            "duplicate": already, "agent": agent, "kind": kind}


def ack_timeout_action(state: Dict[str, Any],
                       now: Optional[float] = None) -> Tuple[str, str]:
    """超时判定（纯函数，patrol 每轮调 + 测试直测）。

    返回 (action, reason)：
      skip  —— 非 pending（无登记/已 ack/已 late）
      wait  —— 未到 deadline
      late  —— 超窗但有进度物证（callbacks 非空）——执行已开始，不重投
      retry —— 超窗无物证且 attempts<2 —— 重投（同键，patrol 侧执行）
      fail  —— 超窗无物证且 attempts>=2 —— 投递失败终态
    """
    ack = state.get("ack") or {}
    if ack.get("state") != "pending":
        return "skip", f"ack.state={ack.get('state') or 'unregistered'}"
    now = now or time.time()
    deadline = _epoch(ack.get("deadline"))
    if now < deadline:
        return "wait", f"deadline 差 {deadline - now:.0f}s"
    if state.get("callbacks"):
        return "late", "超 ACK 窗但有回调物证——执行已开始，不重投"
    attempts = int(ack.get("attempts") or 1)
    if attempts >= 2:
        return "fail", f"重投后仍无回执无物证（attempts={attempts}）"
    return "retry", f"attempts={attempts} 无回执无物证"


# ── steer 链（协议层；词法层见 task/2611111761 的 steer.sh） ──────────────

def register_steer(agent: str, note: str = "",
                   now: Optional[float] = None) -> Dict[str, Any]:
    """登记一次 steer 投递（jsonl 台账，同幂等键语义）。

    steer 无 task_state 生命周期（不打扰任务台账），台账行={ts, key, agent,
    note, ack:null|{ts,agent}}。消费方（steer.sh v2 / 主控）拿 key 让小弟回执。
    """
    key = f"steer-{uuid.uuid4().hex[:10]}"
    STEER_LEDGER.parent.mkdir(parents=True, exist_ok=True)
    row = {"ts": task_state.now_iso(), "key": key, "agent": agent,
           "note": note[:200], "ack": None}
    with open(STEER_LEDGER, "a", encoding="utf-8") as f:
        import fcntl
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        try:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
        finally:
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)
    try:
        from nexusos_xdr.session_state import append_event
        append_event(agent, "op", f"[steer] 投递登记 key={key} note={note[:60]}")
    except Exception:
        pass  # 事件流写失败不阻塞登记（台账已是真相源）
    return {"steer_key": key, "ack_deadline": _deadline_iso(now),
            "ack_cmd": ("curl -s -X POST http://127.0.0.1:19998/ack -d "
                        f'{{"kind":"steer","key":"{key}","agent":"{agent}"}}')}


def handle_steer_ack(key: str, agent: str) -> Dict[str, Any]:
    """steer 回执：台账行 ack 字段填值（幂等——重复回执保持首次时间）。"""
    if not STEER_LEDGER.is_file():
        raise FileNotFoundError(f"steer 台账不存在: {STEER_LEDGER}")
    import fcntl
    lines = STEER_LEDGER.read_text(encoding="utf-8").splitlines()
    hit = False
    for i, line in enumerate(lines):
        try:
            row = json.loads(line)
        except ValueError:
            continue
        if row.get("key") == key:
            hit = True
            if not row.get("ack"):
                row["ack"] = {"ts": task_state.now_iso(), "agent": agent}
                lines[i] = json.dumps(row, ensure_ascii=False)
    if not hit:
        raise FileNotFoundError(f"steer key 不存在: {key}")
    tmp = STEER_LEDGER.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        try:
            f.write("\n".join(lines) + "\n")
        finally:
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)
    tmp.replace(STEER_LEDGER)
    return {"ok": True, "steer_key": key, "acked": True, "agent": agent}
