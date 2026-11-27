"""P0-2 gate REJECT 硬拦（Task 2611111759，用户预批 11-17）。

上游：task/2611111747 提案 v1.1 P0-2 + 红队修订（2611111748 §一攻击2）
     + 存量复核（2611111750：假拦 60%，硬拦必须后置且带复核通道）。
前置：①G12/G14 假 REJECT 修复（2611111749，merge 6061ff8d1）②存量复核分类（1750）。

本模块只做三件与收口强绑定的纯函数/写入件（xdr 收口链域，禁碰 gate 检查器
本身——那是 nexusos-gate 仓，OMP 域，任务书禁止项第 1 条）：

1. ``snapshot_reject_context`` — REJECT 触发快照：触发行上下文 + 检查器版本
   + gate 输出 JSON，原子写入任务目录 ``output/gate-block-snapshot.json``。
   1750 踩坑①②实证：主控代补抹形态 + 检查器活体修改致归因漂移——done 时点
   快照是历史归因唯一解。原子写（tmp+replace）防半截文件；禁 try/except 吞错。

2. ``write_owner_claim_row`` — owner 认领行（七列规范行）：锁内追加到未决问题池
   （pool_write_channel 统一写通道，与 shell 封装同名锁互斥互通）。
   复核通道的登记载体：gate_blocked 非终态语义的出路之一（主控按行认领处置）。

3. ``emit_state_transition_event`` — state_transition gate 事件（engine 同款
   event 形态落 xdr event 流）。写入前调用，观测层可配对（Task544 先例）。
"""
from __future__ import annotations

import json
import logging
import os
import re
import tempfile
import threading
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from nexusos_common import subpath  # 1960：锚点单源（$NEXUS_HOME 相对，仅 xdr 数据目录用）
from nexusos_xdr._vault_env import VAULT_ROOT  # 1957 锚点单源（vault 相对路径唯一真源）

logger = logging.getLogger("nexusos_xdr.p02_hardblock")

# 池单点真源：偏好约束固定 06-Memory/未决问题池.md（禁自创第二池）。
# 测试缝（G16 先例）：P02_POOL_FILE 覆盖指向 fixture，selftest 不碰真池。
_POOL_REL = Path("06-Memory") / "未决问题池.md"


def _pool_path() -> Path:
    env = os.environ.get("P02_POOL_FILE", "").strip()
    return Path(env) if env else VAULT_ROOT / _POOL_REL


def _events_url() -> str:
    env = os.environ.get("P02_EVENTS_URL", "").strip()
    if env:
        return env
    return f"http://127.0.0.1:{_xdr_port()}/session/event"


def _xdr_port() -> int:
    return int(os.environ.get("NEXUS_XDR_PORT", "19998"))


def _gate_fingerprint() -> Dict[str, Any]:
    """检查器指纹：done.yaml sha256 + _checks/*.py mtime 清单（活体修改归因锚）。

    1750 踩坑①：复核期间并行单改检查器，同文件重放 FAIL→PASS 漂移——包
    __version__ 不随文件改动变化，必须文件级指纹才能锁「检查器版本」。
    元数据级尽力采集：失败记 error 字段可观测（不静默），快照主体不受影响。
    """
    import hashlib

    out: Dict[str, Any] = {"done_yaml_sha256": None, "checks": None}
    try:
        import nexusos_gate

        gate_dir = Path(nexusos_gate.__file__).parent
        cfg = gate_dir / "etc" / "gates" / "done.yaml"
        if cfg.is_file():
            out["done_yaml_sha256"] = hashlib.sha256(cfg.read_bytes()).hexdigest()[:16]
        checks = gate_dir / "etc" / "gates" / "_checks"
        if checks.is_dir():
            out["checks"] = sorted(
                f"{p.name}:{int(p.stat().st_mtime)}" for p in checks.glob("*.py"))
    except Exception as fp_err:  # noqa: BLE001 — 指纹是元数据，降级可观测不吞
        out["error"] = str(fp_err)[:120]
    return out


def _resolve_task_dir(task_path: str) -> Optional[Path]:
    """task_path（.../input/task.md 或任务目录）→ 任务根目录绝对路径。"""
    if not task_path:
        return None
    p = Path(task_path)
    root = p.parent.parent if "input" in p.parts else p
    if not root.is_absolute():
        root = VAULT_ROOT / root
    return root if root.is_dir() else None


def snapshot_reject_context(
    task_id: str,
    task_path: str,
    gate_result: Dict[str, Any],
    report_full: str = "",
    summary: str = "",
) -> str:
    """REJECT 触发快照 → 任务目录 ``output/gate-block-snapshot.json``（原子写）。

    内容三件（任务书 §3.3）：触发行上下文（REJECT 明细逐条）+ 检查器版本
    （nexusos-gate 包 __version__，锁版本防归因漂移——1750 踩坑①）+ gate
    输出全量 JSON。 Raises: OSError/TypeError 不吞（禁 try/except 吞错）。
    返回快照绝对路径。
    """
    try:
        from nexusos_gate import __version__ as _gate_version
    except Exception:
        _gate_version = "unknown"  # 包元数据缺失降级，快照本身必须成功
    task_dir = _resolve_task_dir(task_path)
    payload = {
        "task_id": task_id,
        "snapshot_at": _utcnow(),
        "checker_version": str(_gate_version),
        "checker_fingerprint": _gate_fingerprint(),  # 1750 踩坑①：文件级指纹锁版本
        "rejects": [
            {
                "id": r.get("id"),
                "severity": r.get("severity"),
                "detail": r.get("detail"),
            }
            for r in (gate_result.get("failures") or [])
            if r.get("status") == "fail" and r.get("severity") == "REJECT"
        ],
        "gate_result": gate_result,
        "report_path": report_full,
        "summary": (summary or "")[:2000],
    }
    if task_dir is None:
        # 无任务目录（合成单/目录已清）——快照落 xdr 数据目录兜底，不静默丢
        task_dir = Path(subpath("xdr")) / "gate-snapshots"
        task_dir.mkdir(parents=True, exist_ok=True)
    out_dir = task_dir / "output"
    out_dir.mkdir(parents=True, exist_ok=True)
    dst = out_dir / "gate-block-snapshot.json"
    fd, tmp = tempfile.mkstemp(dir=str(out_dir), prefix=".gate-snap.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=1, default=str)
        os.replace(tmp, dst)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
    logger.info("[P0-2] %s 触发快照已落盘: %s", task_id, dst)
    return str(dst)


def _owner_hint(task_id: str, task_path: str) -> str:
    """池 #969-A: owner 归属推导——task_id 前缀 xdr-<agent>-* 优先，
    task_path 形态 task/NN-*-<agent>-* 兜底（非硬编码名单）。"""
    m = re.match(r"xdr-([a-z0-9]+)-", task_id or "")
    if m:
        return m.group(1)
    m = re.search(r"task/\d+(?:\.\d+)?-([a-z0-9]+)-", task_path or "")
    return m.group(1) if m else "unknown"


def _deadline_str(days: int = 7) -> str:
    from datetime import datetime, timedelta

    return (datetime.now() + timedelta(days=days)).strftime("%Y-%m-%d")


def build_claim_row(task_id: str, code: str, snap: str, owner: str,
                    deadline: str, day: str) -> str:
    """拼 owner 认领行（七列规范行；只拼字符串，不读写，测试直接调）。

    首格 [ ]（闭环后由认领者改 [x]），提出会话 xdr自动，状态 待办；
    owner= 写在问题格，处置提示写在备注格。幂等锚「[gate_blocked] <task_id> 」留在问题格。
    """
    problem = (f"[gate_blocked] {task_id} REJECT={code or '?'} 未过硬拦已转复核"
               f"（owner={owner}，处置: 修复产出后走 owner 认领/need_decision 裁决）"
               f" 快照={snap} owner_hint={owner} deadline={deadline}"
               f"（入池+7天，超期 pool-lifecycle-sweep 升级）")
    note = "处置: 修复产出后重过 done gate 或请主控裁决"
    return f"| [ ] | {problem} | P0-2硬拦自动入池 | {day} | xdr自动 | 待办 | {note} |\n"


def write_owner_claim_row(
    task_id: str,
    task_path: str,
    rejects: List[Dict[str, Any]],
    snapshot_path: str = "",
) -> None:
    """owner 认领行追加进未决问题池（pool_write_channel 锁内 append）。

    行是七列规范行（口径见 persist-orchestrator §7 第 5 项），带 task_id+owner+快照指针，
    主控/owner 可直接定位处置。
    池 #969-A（2611192126）：行内追加 owner_hint（task_id/路径推导）+
    deadline（入池+7天，pool-lifecycle-sweep.py 超期升级锚）。
    Raises: PoolWriteConflict/OSError 上抛（fail-loud，写痕已在通道内落）。
    """
    from nexusos_gate.pool_write_channel import pool_write_lock, commit_write

    pool = _pool_path()
    code = ",".join(sorted({str(r.get("id")) for r in rejects if r.get("id")}))
    # B6 (Task 2611111774): 快照指针——fallback 快照（xdr gate-snapshots 目录）
    # 不在 task 目录，池行写绝对路径防 owner 按 task 目录扑空；正常快照仍写文件名。
    snap_p = Path(snapshot_path) if snapshot_path else None
    snap = "no-snapshot"
    if snap_p is not None:
        snap = str(snap_p) if "gate-snapshots" in snap_p.parts else snap_p.name
    row = build_claim_row(task_id, code, snap, _owner_hint(task_id, task_path),
                          _deadline_str(7), _utcnow()[:10])
    with pool_write_lock(pool):
        # P0-② (Task 2611111774) 第二道保险：池行 lookaround 锚（同 G16 :110 先例）
        # ——once_flag 是进程内单源，跨进程/重启后的重复 done 靠池锚兜底。
        anchor = f"[gate_blocked] {task_id} "
        existing = pool.read_text(encoding="utf-8") if pool.exists() else ""
        if anchor in existing:
            logger.info("[P0-2] %s 认领行已存在（池锚命中），跳过重复入池", task_id)
            return
        commit_write(pool, row, "append")
    logger.info("[P0-2] %s owner 认领行已入池", task_id)


def emit_state_transition_event(task_id: str, detail: str) -> None:
    """state_transition gate 事件落 xdr event 流（engine 同款形态，观测配对）。

    dispatch 完成登记=dispatch 事件（合法场景锚），stage=RUNNING；
    状态终态化是收口链路的一部分，沿用 dispatch 完成登记锚（当前唯一
    既有锚点；state_transition 专属锚待 gate 仓扩 Y1 后切换）。
    事件失败只 log 不抛——事件是观测件，不能反过来阻断收口主链。
    """
    try:
        import urllib.request

        state: Optional[Dict[str, Any]] = None
        try:
            from nexusos_xdr import task_state as _ts

            state = _ts.load_state(task_id)
        except Exception:
            state = None
        agent = (state or {}).get("callee") or (state or {}).get("caller") or "xdr"
        body = json.dumps(
            {
                "agent": agent,
                "type": "state_transition",
                "detail": detail[:500],
                "scenario": "dispatch",
                "stage": "RUNNING",
            },
            ensure_ascii=False,
        ).encode("utf-8")
        req = urllib.request.Request(
            _events_url(),
            data=body,
            headers={"Content-Type": "application/json"},
        )
        urllib.request.urlopen(req, timeout=5)
    except Exception as ev_err:  # noqa: BLE001 — 观测件 fail-open（注释为证）
        logger.warning("[P0-2] %s gate 事件落流失败(不阻断): %s", task_id, ev_err)


def close_pool_claim_row(task_id: str) -> bool:
    """池行闭环（P0-① Task 2611111774）：[gate_blocked] <task_id> 行翻 [x]+注记。

    lookaround 锚=行内含 `[gate_blocked] <task_id> `（与写行同源锚，唯一）；
    列数保持（注记插入描述列内，不新增列）；锁内读改写（pool_write_channel
    replace 原子换入+行数校验）。找不到行/池不存在返回 False（幂等 no-op——
    1750 前科单本就无行）。 Raises: PoolWriteConflict/OSError 上抛（fail-loud）。
    """
    from nexusos_gate.pool_write_channel import pool_write_lock, commit_write

    pool = _pool_path()
    if not pool.exists():
        return False
    anchor = f"[gate_blocked] {task_id} "
    with pool_write_lock(pool):
        lines = pool.read_text(encoding="utf-8").splitlines()
        hit = False
        for i, line in enumerate(lines):
            if anchor not in line or not line.lstrip().startswith("| [ ] |"):
                continue
            cols = line.split(" | ")
            if len(cols) >= 5:
                cols[0] = "| [x]"
                cols[1] = f"{cols[1]}（✅ 重发done复核闭环: gate PASS 清账，出路验证 Task 2611111774）"
                lines[i] = " | ".join(cols)
            else:  # 列结构异常保守降级：只翻 checkbox 不动描述
                lines[i] = line.replace("| [ ] |", "| [x] |", 1)
            hit = True
        if not hit:
            return False
        commit_write(pool, "\n".join(lines) + "\n", "replace")
    return True


def maybe_clear_on_pass(
    task_id: str,
    task_path: str,
    detail: str = "",
) -> Optional[Dict[str, Any]]:
    """bg-gate PASS 分支出路编排（P0-① Task 2611111774，handler 调用）。

    清账（task_state.clear_gate_block）成功才接池行闭环；池闭环失败不回滚
    state 清账（真源已正，池是次要台账——warning 留痕可对账，B5 同款降级语义）。
    非 blocked 单零开销 no-op。
    """
    from nexusos_xdr import task_state as _ts

    state = _ts.clear_gate_block(task_id, detail=detail)
    if state is None:
        return None
    try:
        close_pool_claim_row(task_id)
    except Exception as pool_err:  # noqa: BLE001 — 降级可见不吞（warning 落 task_state+log）
        logger.warning("[P0-2] %s 池行闭环失败（state 已清账，warning 可对账）: %s",
                       task_id, pool_err)
    logger.warning("[P0-2] %s 复核出路走通: gate_blocked → completed（池行已闭环）", task_id)
    return state


def _fallback_claim(task_id: str, codes: str, snap: str, pool_err: Exception) -> None:
    """B5 兜底（Task 2611111774）：池写失败 → jsonl 第二台账降级（防孤儿快照）。

    台账 = $NEXUS_XDR_TASK_DIR 同域 gate-block-fallback.jsonl（flock 追加，
    与 STEER_LEDGER 同款互斥语义）。失败只 log 不抛——硬拦主链（state 已
    blocked）不被次要台账拖死；warning 留 task_state 供对账。
    """
    try:
        import fcntl as _fcntl

        # 测试缝（同 STEER_LEDGER 的 NEXUS_STEER_LEDGER 先例）：fixture 隔离不写真实 home
        ledger = Path(os.environ.get("NEXUS_P02_FALLBACK_LEDGER").strip()
                      if os.environ.get("NEXUS_P02_FALLBACK_LEDGER", "").strip()
                      else "") or (Path(subpath("xdr")) / "gate-block-fallback.jsonl")
        ledger.parent.mkdir(parents=True, exist_ok=True)
        row = json.dumps({
            "ts": _utcnow(), "task_id": task_id, "rejects": codes,
            "snapshot": snap, "pool_error": str(pool_err)[:120],
            "note": "池写失败降级台账（B5）——owner 需手工补池行",
        }, ensure_ascii=False)
        with open(ledger, "a", encoding="utf-8") as f:
            _fcntl.flock(f.fileno(), _fcntl.LOCK_EX)
            try:
                f.write(row + "\n")
            finally:
                _fcntl.flock(f.fileno(), _fcntl.LOCK_UN)
        logger.warning("[P0-2] %s 池写失败已降级台账 %s: %s", task_id, ledger, pool_err)
    except Exception as fb_err:  # noqa: BLE001 — 台账也失败则仅 log（主链已在 blocked 态）
        logger.error("[P0-2] %s B5 兜底台账也失败（快照=%s）: %s", task_id, snap, fb_err)


def hardblock_rejects(
    task_id: str,
    task_path: str,
    gate_result: Dict[str, Any],
    report_full: str = "",
    summary: str = "",
    source: str = "",
    done_event: Optional[threading.Event] = None,
) -> Optional[Dict[str, Any]]:
    """硬拦编排（bg-gate 线程调用）：快照 → end_reason 翻转 → 认领行 → 事件/回调/通知。

    时序竞态防御：callback 主流程在线程 start 之后才 update_status(DONE)
    （handler Task412 通电前置），bg 线程可能先于 DONE 到达——主控侧传
    threading.Event（主流程终态写入完成后 set），wait 高效阻塞替代 sleep
    轮询（C2 反模式根因修复，pre-commit 拦截实证）；直调（测试/监控复用）
    不传事件时单次尝试，非 DONE 即放弃留快照，不忙等。异常不吞（fail-loud），
    由 bg 线程外层 except 兜 log。返回翻转后的 state；未翻转（非 DONE）返回 None。
    """
    from nexusos_xdr import task_state as _ts

    reject_items = [
        {"id": r.get("id"), "severity": r.get("severity"), "detail": r.get("detail")}
        for r in (gate_result.get("failures") or [])
        if r.get("status") == "fail" and r.get("severity") == "REJECT"
    ]
    if not reject_items:
        return None

    # P0-② (Task 2611111774) 第一道保险：编排幂等门——同任务只走一次硬拦副作用
    # （1770 P0② 实证：enforce 幂等但编排层无门，重复 done 场景重复认领行/通知）。
    # once_flag 进程内单源 + 状态持久双语义；命中即静默返回（首次已拦已登记）。
    from nexusos_xdr import task_state as _ts

    if not _ts.once_flag(task_id, "p02_hardblocked"):
        logger.info("[P0-2] %s 已硬拦过（幂等门命中），跳过重复复核副作用", task_id)
        return None

    # ① 快照先行（纯写件，翻转成败都留证据）
    snap = snapshot_reject_context(task_id, task_path, gate_result,
                                   report_full=report_full, summary=summary)

    # ② 等 DONE 落定再翻转（竞态窗口：主流程 update_status 在线程启动之后）。
    # done_event 由 handler 在终态写入完成后 set——wait 高效阻塞，无忙等；
    # 事件超时或未传事件时 enforce 一次，非 DONE 即放弃（快照已留证据）。
    if done_event is not None:
        done_event.wait(timeout=10)  # 主流程终态落定（10s 封顶）
    state = None
    try:
        state = _ts.enforce_gate_block(task_id, reject_items)
    except ValueError as e:
        cur = _ts.load_state(task_id) or {}
        if cur.get("status") == "DONE":
            raise  # DONE 却翻不动=真异常，不吞
        logger.info("[P0-2] %s status=%s 非 DONE，硬拦不适用（快照已留）: %s",
                    task_id, cur.get("status"), e)
        return None

    # ③ 复核通道三路出路（blocked≠死）
    codes = ",".join(sorted({str(r["id"]) for r in reject_items}))
    try:
        write_owner_claim_row(task_id, task_path, reject_items, snapshot_path=snap)
    except Exception as pool_err:
        # P1-B5 (Task 2611111774): 池写失败兜底——jsonl 第二台账降级，防孤儿快照
        # （池锁 >5s 被占/vault 瞬断时 gate_blocked 单在池无行无人消费）。
        _fallback_claim(task_id, codes, snap, pool_err)
    emit_state_transition_event(
        task_id, f"gate_blocked: REJECT={codes} 快照={Path(snap).name} 待 owner 认领")
    # Task 2611230853 件三：不再把每一次硬拦自动转成一条「请决策」（14 天 61 次里 54 次是这么来的）。
    # 完成回调本身照常送达派单方（callback 通道不变），派单方从回调就能看到被拦。
    # 显形改走三处：/task_state 的 blocked_* 汇总字段、gate_debt_tick 的 48h 日汇总告警、
    # 以及池里的 owner 认领行（上面已写）。原实现注释保留：
    # try:
    #     _ts.append_callback(task_id, {
    #         "source": "gate", "session": "p02-hardblock",
    #         "status": "need_decision",
    #         "summary": f"REJECT 硬拦转复核: {codes}；快照 {snap}",
    #     })
    # except Exception as cb_err:
    #     logger.warning("[P0-2] %s need_decision 回调记录失败(不阻断): %s", task_id, cb_err)
    # try:
    #     from nexusos_xdr.needdecision_notify import fire as _nd_fire
    #     _nd_fire(task_id, f"gate_blocked 待复核: REJECT={codes}", source=source)
    # except Exception as nd_err:
    #     logger.warning("[P0-2] %s need_decision 通知失败(不阻断): %s", task_id, nd_err)
    logger.warning("[P0-2] %s REJECT 硬拦完成: %s（快照 %s，已转复核）",
                   task_id, codes, snap)
    return state


def _xdr_port() -> int:
    return int(os.environ.get("NEXUS_XDR_PORT", "19998"))


def _utcnow() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+00:00")


# === Task 2611230853 件三：被硬拦超 48h 的日汇总告警（一天最多一条）===
_DEBT_FILE = Path.home() / ".nexus" / "xdr" / "gate-debt.jsonl"
_DEBT_TICK_LAST = 0.0
_DEBT_TICK_INTERVAL = 300.0   # patrol 秒级轮询下别每轮都扫


def gate_debt_tick() -> None:
    """读欠账 → 若当天还没记过账且存在超 48h 的被拦任务，发一条 alert 并记账。

    为什么用 end_reason 而不是新状态：TERMINAL_STATES 被哨兵脚本硬编码，动枚举全线出问题；
    被拦任务的 status 一直是 DONE，靠 end_reason=gate_blocked + 汇总字段 + 告警显形。
    """
    global _DEBT_TICK_LAST
    now = time.time()
    if now - _DEBT_TICK_LAST < _DEBT_TICK_INTERVAL:
        return
    _DEBT_TICK_LAST = now
    try:
        today = datetime.now().astimezone().strftime("%Y-%m-%d")
        if _DEBT_FILE.exists():
            for line in _DEBT_FILE.read_text(encoding="utf-8", errors="replace").splitlines()[-50:]:
                try:
                    if json.loads(line).get("date") == today:
                        return
                except json.JSONDecodeError:
                    continue
        from nexusos_xdr import task_state as _ts  # 局部导入：task_state→p02 是反向依赖，避免循环
        rows = _ts.list_recent(limit=5000) or []
        blocked, over = [], []
        for r in rows:
            if str(r.get("end_reason") or "") != "gate_blocked":
                continue
            blocked.append(r.get("task_id"))
            ts = str(r.get("finished") or r.get("created") or "")
            try:
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                hours = (datetime.now(timezone.utc) - dt).total_seconds() / 3600.0
            except Exception:
                continue
            if hours >= 48:
                over.append((r.get("task_id"), round(hours, 1)))
        if not over:
            return
        oldest = max(h for _, h in over)
        ids = ", ".join(str(t) for t, _ in over[:5])
        detail = f"被拦超 48 小时 {len(over)} 单，最老 {oldest:.1f} 小时，编号：{ids}"
        try:
            body = json.dumps({"agent": "xdr", "type": "alert", "detail": detail},
                              ensure_ascii=False).encode()
            req = urllib.request.Request(_events_url(), data=body,
                                         headers={"Content-Type": "application/json"})
            urllib.request.urlopen(req, timeout=3).close()
        except Exception as e:  # noqa: BLE001 — 观测件，发不出去不影响
            logger.warning("[gate-debt] alert 发送失败(不阻断): %s", e)
        _DEBT_FILE.parent.mkdir(parents=True, exist_ok=True)
        with _DEBT_FILE.open("a", encoding="utf-8") as f:
            f.write(json.dumps({"date": today, "at": datetime.now().astimezone().isoformat(timespec="seconds"),
                                "blocked_total": len(blocked), "over_48h": len(over),
                                "oldest_hours": round(oldest, 1)}, ensure_ascii=False) + "\n")
    except Exception:
        pass
