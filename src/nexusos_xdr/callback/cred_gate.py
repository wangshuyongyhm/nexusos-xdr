#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""cred_gate.py — 完成回调的「出示派单凭据」闸门（2611231133，第一层：系统验）。

背景：回调入口原来只查「任务自己名下的派单凭据在库里还有效」，**不要求调用方拿出来**——
知道任务号的进程就能伪造一次「完成」，把该任务名下的套用记录结成不适用。
本模块做的是**第二段**：要求调用方出示凭据，且必须等于该任务台账里的 `credential_id`。

三种结果（规则见 references §2.1）：
  verified：出示且与台账一致 → 照常处理（事件记一笔「凭据已核」）
  missing ：没出示（或台账里没有可比对的凭据，无法验）→ 按开关走
  wrong   ：出示了但与台账不一致（别人的/编的）→ **两种模式下都拒**

开关：`~/.nexus/xdr/callback-cred.json` 里的 `{"mode": "record"|"enforce"}`，
**每次请求时读**（文件缺失/坏掉/字段不认识 → 一律按 record）。不用环境变量——那要重启才生效。

计数：每次判定追加一行到 `~/.nexus/xdr/callback-cred-events.jsonl`（几十字节的追加，
不在请求里连库）；状态接口按需读它算最近 24 小时按来源的三类计数。

热路径纪律：本模块只读一个小文件 + 追加一行 + 发一条事件；不做数据库查询。
"""
from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger("nexusos_xdr.callback.cred_gate")

MODE_RECORD = "record"
MODE_ENFORCE = "enforce"
MODES = (MODE_RECORD, MODE_ENFORCE)

OUTCOME_VERIFIED = "verified"
OUTCOME_MISSING = "missing"
OUTCOME_WRONG = "wrong"

# 2611239908：拿不到派单凭据的执行者报「不规范派单」的**只记录**通道。
# 状态必须是 need_decision，且请求体带这个标记字段（JSON true / 1）；机器判定不看摘要文字。
MISSING_CRED_REPORT_FIELD = "dispatch_credential_missing"
OUTCOME_MISSING_CRED_REPORT = "missing_cred_report"


def _state_dir() -> Path:
    """开关与计数文件所在目录（测试可替换本函数）。"""
    return Path(os.path.expanduser("~")) / ".nexus" / "xdr"


def _mode_file() -> Path:
    """开关文件（测试可替换本函数）。"""
    return _state_dir() / "callback-cred.json"


def _ledger_file() -> Path:
    """判定计数台账（JSONL，追加写；测试可替换本函数）。"""
    return _state_dir() / "callback-cred-events.jsonl"


def read_mode() -> tuple:
    """读开关。返回 (mode, err)：mode ∈ {record, enforce}；读不到/坏了 → record。

    每次请求都读（文件很小，几十字节）——这样主控观察完计数改文件即可生效，不用重启服务。
    """
    path = _mode_file()
    try:
        raw = path.read_text(encoding="utf-8", errors="replace")
    except FileNotFoundError:
        return MODE_RECORD, "开关文件不存在（按 record 处理）"
    except Exception as e:  # noqa: BLE001 — 读不了也不能让回调挂掉，按 record
        logger.warning("[2611231133] 开关文件读不了，按 record 处理: %s: %s", type(e).__name__, e)
        return MODE_RECORD, f"开关文件读不了: {type(e).__name__}"
    try:
        data = json.loads(raw)
        mode = str((data or {}).get("mode", "")).strip().lower()
    except Exception as e:  # noqa: BLE001
        logger.warning("[2611231133] 开关文件内容坏了，按 record 处理: %s: %s", type(e).__name__, e)
        return MODE_RECORD, f"开关文件内容坏了: {type(e).__name__}"
    if mode not in MODES:
        logger.warning("[2611231133] 开关值不认识（%r），按 record 处理", mode)
        return MODE_RECORD, f"开关值不认识: {mode!r}"
    return mode, ""


def decide(presented: str, ledger_cred: str, mode: str) -> str:
    """纯判定：出示的凭据 vs 台账凭据 → verified / missing / wrong。

    - 出示了、台账有、且相等 → verified
    - 出示了、台账有、但不相等 → wrong（两种模式下都拒）
    - 出示了、台账没有（老任务，没有可比对的凭据）→ missing（无法验，按开关走）
    - 没出示 → missing
    """
    p = (presented or "").strip()
    l = (ledger_cred or "").strip()
    if not p:
        return OUTCOME_MISSING
    if not l:
        return OUTCOME_MISSING
    return OUTCOME_VERIFIED if p == l else OUTCOME_WRONG


def should_reject(outcome: str, mode: str) -> bool:
    """要不要因为凭据问题拒掉这次回调。"""
    if outcome == OUTCOME_WRONG:
        return True
    if outcome == OUTCOME_MISSING and mode == MODE_ENFORCE:
        return True
    return False


def is_missing_cred_report(body, status) -> bool:
    """这次回调是不是「拿不到派单凭据 → 报不规范派单」那条只记录通道。

    两个条件都要满足：① 状态是 need_decision（**不能拿它报完成或别的状态**）；
    ② 请求体带明确标记字段 `dispatch_credential_missing`，值为 JSON true 或 1
    （不看摘要文字——那是人读的，机器判定不许靠它）。判定不了/字段格式不对 → False，
    按正常凭据规则走：不往「放过」那一侧倒。
    """
    if str(status or "").strip().lower() != "need_decision":
        return False
    if not isinstance(body, dict):
        return False
    val = body.get(MISSING_CRED_REPORT_FIELD)
    return val is True or (isinstance(val, int) and not isinstance(val, bool) and val == 1)


def record_missing_cred_report(task_id: str, source: str, status: str, detail: str = "") -> None:
    """记一次「无派单凭据报不规范派单」：与 verified/missing/wrong 分开记（状态接口单列一项）。

    这条通道**只记录**：不改任何任务状态、不触发自动判定。主控看状态接口的
    `missing_cred_reports` 就知道要不要去找派单方问为什么没给凭据。
    """
    record(OUTCOME_MISSING_CRED_REPORT, task_id, source, status,
           detail=("无派单凭据报不规范派单" + (f"｜{detail}" if detail else "")))


def mask(cred: Optional[str]) -> str:
    """凭据号打码（报告/事件/日志里一律用这个）。"""
    s = str(cred or "")
    if len(s) <= 12:
        return "cred_<…>" if s else ""
    return s[:9] + "…"


def record(outcome: str, task_id: str, source: str, status: str, detail: str = "") -> None:
    """记一次判定：追加 JSONL 台账 + 发一条事件（fire-and-forget，失败只告警）。"""
    entry = {
        "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "outcome": outcome,
        "task_id": str(task_id)[:100],
        "source": str(source)[:50],
        "status": str(status)[:24],
        "detail": str(detail)[:200],
    }
    try:
        path = _ledger_file()
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception as e:  # noqa: BLE001 — 计数失败不影响回调主链
        logger.warning("[2611231133] 判定台账追加失败: %s: %s", type(e).__name__, e)
    try:
        from nexusos_xdr import session_state as _ss
        evt_type = {"verified": "callback_cred_verified",
                    "missing": "callback_cred_missing",
                    "wrong": "callback_cred_wrong",
                    "missing_cred_report": "callback_cred_missing_report"}.get(
                        outcome, "callback_cred_other")
        _ss.emit_system_event(
            source or "xdr", evt_type,
            detail=(f"{task_id} 回调凭据={outcome} status={status or '?'}"
                    + (f"｜{detail}" if detail else "")),
            target=f"task:{task_id}",
            extra={"outcome": outcome, "callback_status": status, "source": source},
        )
    except Exception as e:  # noqa: BLE001 — 事件失败不影响回调主链（口径同 callback_arrived）
        logger.warning("[2611231133] 凭据事件发布失败: %s: %s", type(e).__name__, e)


def stats(hours: int = 24) -> dict:
    """最近 N 小时按来源统计的三类计数（状态接口用；按需读台账，不在热路径）。"""
    mode, err = read_mode()
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    counts: dict = {}
    scanned = 0
    bad = 0
    n_report = 0  # 2611239908：无凭据报不规范派单（只记录通道）的次数
    try:
        path = _ledger_file()
        if path.exists():
            with open(path, encoding="utf-8", errors="replace") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    scanned += 1
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
                    src = str(row.get("source") or "(空)")
                    bucket = counts.setdefault(src, {"verified": 0, "missing": 0, "wrong": 0})
                    oc = str(row.get("outcome", ""))
                    if oc in bucket:
                        bucket[oc] += 1
                    elif oc == OUTCOME_MISSING_CRED_REPORT:
                        # 2611239908：只记录通道的计数单列（不进 by_source/totals 的三类桶，
                        # 2611231133 的接口形状保持不变）
                        n_report += 1
    except Exception as e:  # noqa: BLE001 — 状态接口本身要能答，坏了就报错原因
        logger.warning("[2611231133] 判定台账读取失败: %s: %s", type(e).__name__, e)
        return {"ok": False, "error": f"判定台账读取失败: {type(e).__name__}", "mode": mode,
                "mode_note": err, "window_hours": hours}
    totals = {"verified": sum(b["verified"] for b in counts.values()),
              "missing": sum(b["missing"] for b in counts.values()),
              "wrong": sum(b["wrong"] for b in counts.values())}
    return {
        "ok": True,
        "mode": mode,
        "mode_note": err or "开关文件正常",
        "enforce_active": mode == MODE_ENFORCE,
        "window_hours": hours,
        "by_source": counts,
        "totals": totals,
        # 2611239908：只记录通道单列一项（「无派单凭据报不规范派单」次数）
        "missing_cred_reports": n_report,
        "ledger_lines": scanned,
        "ledger_bad_lines": bad,
        "mode_file": str(_mode_file()),
    }


def _now_ms() -> int:
    """小工具：毫秒时间戳（测试与计时用）。"""
    return int(time.time() * 1000)
