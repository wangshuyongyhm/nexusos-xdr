#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""ack_policy.py — 派单的人验收（第二层，2611239003）。

两件事：
1. **签收归属**：签收必须出示派单方自己的检索凭据；持有者要等于任务台账里的 `caller`，
   且 caller 在主控名单里。派单方不在主控名单的任务（系统派单方，如自测影子任务）允许
   主控名单里的任一主控**代签**，签收记录写明代签。
2. **后果延后**：派单方是主控的任务，完成回调时不再跑自动判定，改到签收后；一个任务只判一次。

开关：`~/.nexus/xdr/ack-policy.json`，两个键各取 `record` / `enforce`，**每次请求读**
（缺失/坏/值不认识 → 一律 record，默认只记录）。不用环境变量（那要重启才生效）。

热路径：读一个小文件 + 追加一行 JSONL + 一条事件；签收路径上多一次按主键查凭据持有者
（`cred_bearer`，单行 SELECT），没有别的连库开销。
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger("nexusos_xdr.ack_policy")

MODE_RECORD = "record"
MODE_ENFORCE = "enforce"
KEYS = ("ack_identity", "defer_judge")

# 签收判定结果
V_VERIFIED_CALLER = "verified_caller"      # 派单方本人
V_VERIFIED_DELEGATE = "verified_delegate"  # 派单方不是主控 → 主控代签
V_MISSING = "missing"                      # 没出示凭据（只报了名字）
V_WRONG = "wrong"                          # 凭据无效/持有者对不上


def _state_dir() -> Path:
    """开关与台账目录（测试可替换本函数）。"""
    return Path(os.path.expanduser("~")) / ".nexus" / "xdr"


def _policy_file() -> Path:
    """开关文件（测试可替换本函数）。"""
    return _state_dir() / "ack-policy.json"


def _ledger_file() -> Path:
    """判定台账（JSONL，追加写；测试可替换本函数）。"""
    return _state_dir() / "ack-policy-events.jsonl"


def read_policy() -> dict:
    """读开关。返回 {"ack_identity": mode, "defer_judge": mode, "note": str}。

    缺失/内容坏/键值不认识 → 该键按 record。每次请求读（文件只有几十字节）。
    """
    out = {"ack_identity": MODE_RECORD, "defer_judge": MODE_RECORD, "note": ""}
    path = _policy_file()
    try:
        raw = path.read_text(encoding="utf-8", errors="replace")
    except FileNotFoundError:
        out["note"] = "开关文件不存在（按 record 处理）"
        return out
    except Exception as e:  # noqa: BLE001 — 读不了也不能让请求挂掉，按 record
        logger.warning("[2611239003] 开关文件读不了，按 record: %s: %s", type(e).__name__, e)
        out["note"] = f"开关文件读不了: {type(e).__name__}"
        return out
    try:
        data = json.loads(raw) or {}
    except Exception as e:  # noqa: BLE001
        logger.warning("[2611239003] 开关文件内容坏了，按 record: %s: %s", type(e).__name__, e)
        out["note"] = f"开关文件内容坏了: {type(e).__name__}"
        return out
    for k in KEYS:
        v = str(data.get(k, "") or "").strip().lower()
        if v in (MODE_RECORD, MODE_ENFORCE):
            out[k] = v
        elif v:
            logger.warning("[2611239003] 开关 %s 的值不认识（%r），按 record", k, v)
            out["note"] = (out["note"] + f"；{k} 值不认识").lstrip("；")
    return out


def is_orchestrator(name: str) -> bool:
    """名字是否在主控名单（读不到一律 False——身份判断宁可误拒）。"""
    try:
        from nexusos_xdr.agents import may_orchestrate
        return bool(may_orchestrate(str(name or "")))
    except Exception as e:  # noqa: BLE001
        logger.warning("[2611239003] 主控名单读不到，按不是主控处理: %s: %s",
                       type(e).__name__, e)
        return False


def cred_bearer(credential_id: str) -> Optional[str]:
    """按主键查凭据持有者（单行 SELECT）。查不到 → None。（测试可替换本函数）"""
    cid = str(credential_id or "").strip()
    if not cid:
        return None
    try:
        import psycopg2
        conn = psycopg2.connect("host=127.0.0.1 port=5432 dbname=nexus_mem user=wang")
        cur = conn.cursor()
        cur.execute("SELECT bearer FROM credentials WHERE credential_id = %s", (cid,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        return (row[0] or "").strip() if row else None
    except Exception as e:  # noqa: BLE001 — 查不了按「查不到」处理（拒绝侧）
        logger.warning("[2611239003] 查凭据持有者失败（按查不到处理）: %s: %s",
                       type(e).__name__, e)
        return None


def ack_verdict(presented_cred: str, ledger_caller: str,
                verify_fn=None, bearer_fn=None, orch_fn=None) -> tuple:
    """签收归属判定。返回 (verdict, detail)。

    - 出示的凭据有效、持有者 == 台账派单方、且派单方在主控名单 → verified_caller
    - 派单方**不在**主控名单（系统派单方）、持有者是主控名单里的人 → verified_delegate（代签）
    - 没出示 → missing
    - 其余（凭据无效 / 持有者对不上 / 派单方是主控但凭据是别人的）→ wrong
    """
    presented = str(presented_cred or "").strip()
    caller = str(ledger_caller or "").strip()
    if not presented:
        return V_MISSING, "没出示凭据（只有自报名字）"
    bearer_fn = cred_bearer if bearer_fn is None else bearer_fn
    orch_fn = is_orchestrator if orch_fn is None else orch_fn
    verify_fn = verify_fn or _default_verify
    holder = bearer_fn(presented)
    if not holder:
        return V_WRONG, "凭据不存在"
    valid, detail = verify_fn(presented)
    if not valid:
        return V_WRONG, f"凭据无效: {detail}"
    caller_is_orch = orch_fn(caller)
    if caller_is_orch:
        if holder == caller:
            return V_VERIFIED_CALLER, "派单方本人"
        return V_WRONG, f"凭据持有者 {holder} ≠ 派单方 {caller}"
    # 派单方不是主控（系统派单方）→ 允许主控代签
    if orch_fn(holder):
        return V_VERIFIED_DELEGATE, f"派单方 {caller} 不是主控，由 {holder} 代签"
    return V_WRONG, f"派单方 {caller} 不是主控，且凭据持有者 {holder} 也不是主控"


def _default_verify(credential_id: str) -> tuple:
    try:
        from nexusos_xdr.credential import verify_credential
        return verify_credential(credential_id, "search_cred")
    except Exception as e:  # noqa: BLE001 — 校验本身出错按无效（拒绝侧）
        logger.warning("[2611239003] 凭据校验异常（按无效处理）: %s: %s",
                       type(e).__name__, e)
        return False, f"校验异常: {type(e).__name__}"


def should_reject(verdict: str, mode: str) -> bool:
    """签收要不要拒。verified_* 一律放行；其余看开关（enforce 才拒）。"""
    if verdict in (V_VERIFIED_CALLER, V_VERIFIED_DELEGATE):
        return False
    return mode == MODE_ENFORCE


def should_defer_judge(caller: str, mode: str) -> bool:
    """自动判定要不要延后到签收。只有「派单方是主控」+ 开关 enforce 才延后。"""
    return bool(mode == MODE_ENFORCE and is_orchestrator(caller))


def record(kind: str, task_id: str, caller: str, detail: str = "", extra: Optional[dict] = None) -> None:
    """记一次事件：追加 JSONL + 发一条事件（fire-and-forget）。"""
    entry = {
        "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "kind": str(kind)[:40],
        "task_id": str(task_id)[:100],
        "caller": str(caller)[:50],
        "detail": str(detail)[:200],
    }
    if extra:
        entry.update({k: v for k, v in extra.items() if isinstance(v, (int, float, str, bool))})
    try:
        path = _ledger_file()
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception as e:  # noqa: BLE001 — 计数失败不影响请求
        logger.warning("[2611239003] 台账追加失败: %s: %s", type(e).__name__, e)
    try:
        from nexusos_xdr import session_state as _ss
        _ss.emit_system_event(
            "xdr", f"ack_policy_{kind}",
            detail=f"{task_id} caller={caller or '?'}｜{detail}"[:300],
            target=f"task:{task_id}",
            extra={"kind": kind, "caller": caller},
        )
    except Exception as e:  # noqa: BLE001 — 事件失败不影响主链
        logger.warning("[2611239003] 事件发布失败: %s: %s", type(e).__name__, e)


def pending_ack_tasks() -> int:
    """完成但没签收的任务数（状态接口用；扫台账文件，不在热路径）。"""
    try:
        import glob
        n = 0
        for f in glob.glob(str(_state_dir() / "tasks" / "*.json")):
            try:
                st = json.loads(Path(f).read_text(encoding="utf-8", errors="replace"))
            except Exception:
                continue
            if str(st.get("status", "")).upper() == "DONE" and not st.get("acked"):
                n += 1
        return n
    except Exception as e:  # noqa: BLE001
        logger.warning("[2611239003] 统计待签收任务数失败: %s: %s", type(e).__name__, e)
        return -1


def stats(hours: int = 24) -> dict:
    """状态接口：两个开关 + 最近 N 小时的计数 + 等待数。"""
    pol = read_policy()
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    counts = {"acked_verified": 0, "acked_unverified": 0, "acked_rejected": 0,
              "judge_deferred": 0, "deferred_records": 0}
    bad = 0
    try:
        path = _ledger_file()
        if path.exists():
            with open(path, encoding="utf-8", errors="replace") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        row = json.loads(line)
                        ts = datetime.fromisoformat(str(row.get("ts", "")))
                    except Exception:
                        bad += 1
                        continue
                    if ts.tzinfo is None:
                        ts = ts.replace(tzinfo=timezone.utc)
                    if ts < cutoff:
                        continue
                    kind = str(row.get("kind", ""))
                    if kind in counts:
                        counts[kind] += 1
                    if kind == "judge_deferred":
                        counts["deferred_records"] += int(row.get("would_defer", 0) or 0)
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": f"台账读取失败: {type(e).__name__}",
                "policy": pol, "window_hours": hours}
    return {
        "ok": True,
        "policy": pol,
        "enforce_active": {"ack_identity": pol["ack_identity"] == MODE_ENFORCE,
                           "defer_judge": pol["defer_judge"] == MODE_ENFORCE},
        "window_hours": hours,
        "counts": counts,
        "pending_ack_tasks": pending_ack_tasks(),
        "policy_file": str(_policy_file()),
        "ledger_bad_lines": bad,
    }


def count_pending_for_task(task_id: str) -> int:
    """本任务名下还没结案的曝光条数（record 模式用来报「若延展会推迟多少条」）。

    口径：agent_activity 里 action='apply_attempt' 且 outcome 为空/pending，
    task_id 命中任务别名集合。bind_task 归入的那部分不算（与自动判定函数的口径
    略有差异，只用于「量级」展示）。查不了返回 -1。
    """
    try:
        import psycopg2
        from nexusos_xdr.task_aliases import task_id_aliases
        aliases = task_id_aliases(task_id, "")
        conn = psycopg2.connect("host=127.0.0.1 port=5432 dbname=nexus_mem user=wang")
        cur = conn.cursor()
        cur.execute(
            "SELECT count(*) FROM agent_activity WHERE action='apply_attempt' "
            "AND (outcome IS NULL OR outcome='pending') AND task_id = ANY(%s)",
            (aliases,))
        n = int(cur.fetchone()[0])
        cur.close()
        conn.close()
        return n
    except Exception as e:  # noqa: BLE001 — 只是量级展示，查不了给 -1
        logger.warning("[2611239003] 统计待结记录数失败: %s: %s", type(e).__name__, e)
        return -1
