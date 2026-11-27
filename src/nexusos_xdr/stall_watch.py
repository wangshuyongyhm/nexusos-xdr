#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""stall_watch.py — 执行者停摆看门狗（Task 2611230867 件三建；2611230953 加处置；2611230959 修正）。

两层结构：
- 提示层（不变）：在跑任务超 20 分钟无回调 → notice 事件 + stall-watch.jsonl 留痕，
  同一任务 30 分钟冷却。
- 处置层（2611230953）：任务 RUNNING 且创建/最近回调满 3 分钟（不吃提示层的 20 分钟
  门槛）+ 面板停在报错空闲态 + 25 秒复核 → steer.sh 轻推（累计 3 次、间隔 2/5/10 分钟）
  → 三次无效：取消原任务（cancel_task 内部参数 stall-redispatch）→ 任务书追加接续段
  → 同一任务书改派空闲执行者（台账无在途任务者优先筛）；无空闲执行者不取消、通知并
  标「待主控处置」。开关 NEXUS_STALL_AUTO_RESUME 默认开。

触发与线程模型（2611230959 甲）：不在消息总线事件循环线程上同步执行——由
mcp_server 的 session 心跳守护线程每 120s 驱动一轮；record_poll 处只异步触发
（trigger_async 起 daemon 线程）。tick 单飞：非阻塞锁拿不到就跳过本次。
tick 里任何一步抛错都记警告日志，不静默。不杀会话、不发邮件。
"""
import json
import logging
import os
import re
import subprocess
import threading
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

logger = logging.getLogger("stall_watch")

STALL_FILE = Path.home() / ".nexus" / "xdr" / "stall-watch.jsonl"
_TICK_INTERVAL_SEC = 120.0        # 每 2 分钟最多跑一次（2611230953：轻推递增间隔 2/5/10 分钟需要 2 分钟节奏；提示层仍有 30 分钟冷却，通知不会变密）
_REMIND_COOLDOWN_SEC = 30 * 60    # 同一任务 30 分钟内只提示一次
_TICK_LAST = 0.0


# ────────────────────────── 纯函数（可单测）──────────────────────────

def _parse_ts(ts: Any) -> Optional[datetime]:
    """ISO 时间串 → aware datetime（naive 当 UTC）；解析不了返回 None。"""
    if not ts:
        return None
    try:
        dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def last_callback_ts(task: Dict[str, Any]) -> Optional[datetime]:
    """一条任务记录最近一次回调的时间；没有回调记录就用创建时间。"""
    latest: Optional[datetime] = None
    for cb in task.get("callbacks") or []:
        dt = _parse_ts((cb or {}).get("ts") if isinstance(cb, dict) else None)
        if dt is not None and (latest is None or dt > latest):
            latest = dt
    if latest is not None:
        return latest
    return _parse_ts(task.get("created") or task.get("started"))


def stalled_tasks(tasks: Iterable[Dict[str, Any]], now: datetime,
                  threshold_min: float = 20) -> List[Dict[str, Any]]:
    """在跑的任务里，距今超过阈值没有回调的那些。纯函数。

    返回 [{"task_id", "callee", "minutes", "task"}, ...]，minutes 为整数（向下取整）。
    """
    out: List[Dict[str, Any]] = []
    for t in tasks or []:
        if str((t or {}).get("status") or "").upper() != "RUNNING":
            continue
        last = last_callback_ts(t)
        if last is None:
            continue
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        if last.tzinfo is None:
            last = last.replace(tzinfo=timezone.utc)
        minutes = int((now - last).total_seconds() // 60)
        if (now - last).total_seconds() >= threshold_min * 60:
            out.append({
                "task_id": str(t.get("task_id") or ""),
                "callee": str(t.get("callee") or t.get("to") or ""),
                "minutes": minutes,
                "task": t,
            })
    return out


def pane_tail(session: str, n: int = 2) -> str:
    """取执行者面板最后几行非空内容；取不到返回空串。整体包 5 秒超时。

    会话名一般就是执行者名（ds2 的会话名是 ds2）。tmux 指定会话用 -t "=name:"
    （等号精确匹配 + 冒号默认窗口，防 ds 前缀匹配到 ds2）。
    """
    if not session:
        return ""
    try:
        r = subprocess.run(
            ["tmux", "capture-pane", "-t", f"={session}:", "-p"],
            capture_output=True, text=True, timeout=5,
        )
        if r.returncode != 0:
            return ""
        lines = [ln.rstrip() for ln in r.stdout.splitlines() if ln.strip()]
        return " | ".join(lines[-n:]) if lines else ""
    except Exception as e:  # noqa: BLE001 — 观测件，取不到就空串
        logger.debug("[stall-watch] pane_tail 取不到(%s): %s", session, e)
        return ""


# ───────────────── 2611230953：停摆自动接续与改派（默认开）─────────────────
# 开关：NEXUS_STALL_AUTO_RESUME=0/false/off 关掉 → 回到只提示（上面 remind 层不变）。
# 判据（面板停在报错后的空闲态）：末 10 行有报错行且无活动迹象，25 秒复核仍如此。
# 轻推：每任务累计最多 3 次、间隔递增（2/5/10 分钟）；三次接不上 → 取消原任务
# （cancel_task 内部参数 "stall-redispatch"）→ 任务书追加接续段 → 同一任务书改派
# 空闲执行者（派单方登记 xdr-stall-resume，不冒用主控名）；找不到空闲执行者 →
# 不取消，发 notice + 标「待主控处置」留痕。全程不杀会话、不发邮件。

_PUSH_INTERVALS_SEC = (120.0, 300.0, 600.0)   # 第 1/2/3 次轻推之后各自的等待
_MAX_PUSHES = 3
_ERROR_IDLE_MARKERS = ("Error: [provider.", "If this persists, run")
_ACTIVITY_MARKERS = ("Working for", "Thinking ·", "Retrying",
                     "Running", "Generating", "esc interrupt")
_RESUME_CALLER = "xdr-stall-resume"           # 改派登记的系统身份（非任何主控名）
_RESUME_MSG = ("模型服务连接抖动造成的中断，和你的做法无关；请从中断的那一步接着做，"
               "已落盘的不要重做；每做完一步落一次盘。任务号：{tid}")
_RECHECK_SEC = 25.0                            # 复核间隔（防两次工具调用之间的空窗）
_REDEPATCH_RETRY_SEC = 600.0                   # 改派失败（无空闲执行者）后的重试间隔


def auto_resume_enabled() -> bool:
    """整体开关（默认开）；tick 时现读，测试可 monkeypatch env。"""
    return os.environ.get("NEXUS_STALL_AUTO_RESUME", "1").strip().lower() not in ("0", "false", "off")


def pane_error_idle(pane_text: str) -> bool:
    """面板是否停在「模型服务报错后的空闲态」。纯函数。"""
    if not pane_text:
        return False
    has_err = any(m in pane_text for m in _ERROR_IDLE_MARKERS)
    has_act = any(m in pane_text for m in _ACTIVITY_MARKERS)
    return has_err and not has_act


def _last_callback_status(task: Dict[str, Any]) -> str:
    """任务最近一次回调的 status（无回调返空串）。need_decision 的任务不碰。"""
    cbs = [c for c in (task.get("callbacks") or []) if isinstance(c, dict)]
    if not cbs:
        return ""
    latest = max(cbs, key=lambda c: str(c.get("ts") or ""))
    return str(latest.get("status") or "")


def _load_stall_state(path: Path) -> Dict[str, Dict[str, Any]]:
    """读记录文件重建每任务处置状态：push_count/last_push_epoch/phase/redispatch_epoch。

    2611230959 己：读全文件不再截尾——末 N 行窗口会让老任务的 push 记录掉出窗口，
    累计封顶 3 次就可能被突破。jsonl 单行百字节级、日增量小，全量读代价可忽略。
    """
    state: Dict[str, Dict[str, Any]] = {}
    try:
        if path.exists():
            for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                tid = rec.get("task_id")
                if not tid:
                    continue
                st = state.setdefault(str(tid), {"push_count": 0, "last_push_epoch": 0.0,
                                                 "phase": "", "redispatch_epoch": 0.0})
                act = rec.get("action")
                if act == "push":
                    st["push_count"] += 1
                    st["last_push_epoch"] = float(rec.get("at_epoch") or 0)
                    st["phase"] = "pushing"
                elif act == "resumed":
                    st["phase"] = "resumed"      # 接上了：当前轮次清零，累计次数不清
                elif act == "redispatch-skip-no-idle":
                    st["redispatch_epoch"] = float(rec.get("at_epoch") or 0)
                    st["phase"] = "pending-orchestrator"
                elif act == "redispatch-failed":
                    st["redispatch_epoch"] = float(rec.get("at_epoch") or 0)
                    st["phase"] = "redispatch-failed"
                elif act == "redispatched":
                    st["phase"] = "redispatched"
    except Exception as e:  # noqa: BLE001 — 状态重建失败按空态继续（宁漏处置不误判）
        logger.warning("[stall-watch] 处置状态重建失败(按空态继续): %s", e)
    return state


def _steer_push(session: str, message: str) -> bool:
    """用现有轻推工具 steer.sh 发「接着做」（带各客户端提交差异与消费验证）。

    脚本路径：环境变量 NEXUS_STEER_SCRIPT 优先，未设时默认 ~/.hermes/scripts/steer.sh（与改前一致）。
    """
    try:
        r = subprocess.run(
            ["bash", os.environ.get("NEXUS_STEER_SCRIPT") or str(Path.home() / ".hermes" / "scripts" / "steer.sh"),
             session, message],
            capture_output=True, text=True, timeout=60,
        )
        if r.returncode != 0:
            logger.warning("[stall-watch] steer 消费验证未过(%s): rc=%s", session, r.returncode)
            return False
        return True
    except Exception as e:  # noqa: BLE001 — 轻推失败记日志，下一轮再试
        logger.warning("[stall-watch] steer 调用失败(%s): %s", session, e)
        return False


def _find_idle_executor(exclude: str = "", busy: Optional[set] = None):
    """找空闲执行者：登记 active + 台账无在途任务 + has_session + is_idle，排除原执行者。

    2611230959 乙：先过台账（list_active 的在途任务 callee 一律排除）再看面板空闲——
    is_idle 只比对面板末 3 行的固定忙碌标记，「面板空闲但台账有活」是常态，不筛台账
    会选中被派单忙碌硬检查拒绝的执行者。台账拿不到时保守返回 None（不选不错选）。
    上下文占用读得到就避开，读不到不判（presence 的 context_pct 由 Agent 自报，
    xdr 进程内无可靠源）。
    """
    if busy is None:
        try:
            from nexusos_xdr import task_state as _ts  # 局部导入：避免循环依赖
            busy = {str(t.get("callee") or t.get("to") or "")
                    for t in (_ts.list_active() or [])}
        except Exception as e:  # noqa: BLE001
            logger.warning("[stall-watch] 台账在途清单获取失败，本轮不选人: %s", e)
            return None
    try:
        from nexusos_xdr import agents as _agents  # 局部导入：避免循环依赖
        for name, status in _agents.list_agents_with_status().items():
            if status != "active" or name == exclude or name in busy:
                continue
            agent = _agents.get_agent(name)
            if agent is None:
                continue
            try:
                if agent.has_session() and agent.is_idle():
                    return name
            except Exception as e:  # noqa: BLE001 — 单个执行者探测失败就跳过它
                logger.warning("[stall-watch] 执行者 %s 空闲探测失败(跳过): %s", name, e)
    except Exception as e:  # noqa: BLE001
        logger.warning("[stall-watch] 执行者清单获取失败: %s", e)
    return None


def _append_continuation(task_md: Path, tid: str, callee: str, new_callee: str) -> bool:
    """给原任务书追加接续段（身份用法 + 已完成/还差什么/怎么检查），并把
    frontmatter 的 executor 改成新执行者（原值留在注释里）——2611230959 丁。"""
    try:
        seg = (
            "\n\n---\n\n## 自动接续段（停摆改派，机制任务 2611230953/2611230959）\n\n"
            f"**你的身份是 {new_callee}**：本任务书正文里出现的原执行者名字（{callee}）"
            "一律读成你自己的名字——回调命令的 source 与 session 用你自己的名字，"
            "MEM 检索用你自己的身份；凭据以你这份任务书副本头部注入的为准。\n"
            f"原执行者 {callee} 因模型服务报错停摆，系统自动轻推 {_MAX_PUSHES} 次未接上，"
            f"原任务 {tid} 已按停摆改派取消（未杀会话）。\n"
            "- 已完成什么：以 output/files/ 与 process/full-log.md 现有内容为准，已落盘的不要重做\n"
            "- 还差什么：对照本任务书「完成标准」逐条核对，缺什么补什么\n"
            "- 怎么检查：先读 process/full-log.md 与 output/ 现状再继续\n"
            f"本单由系统自动改派给 {new_callee}（派单方 {_RESUME_CALLER}）。\n"
        )
        text = task_md.read_text(encoding="utf-8")
        # frontmatter 的 executor 覆盖成新执行者（原值留注释）；只动 frontmatter 段
        if text.startswith("---"):
            end = text.find("\n---", 3)
            if end != -1:
                head, rest = text[:end], text[end:]
                head = re.sub(r"(?m)^(executor:\s*)\S+.*$",
                              lambda m: f"{m.group(1)}{new_callee}"
                                        f"  # 原值 {callee}（停摆改派覆盖，机制 2611230953）",
                              head, count=1)
                text = head + rest
        task_md.write_text(text + seg, encoding="utf-8")
        return True
    except Exception as e:  # noqa: BLE001
        logger.warning("[stall-watch] 接续段写入失败(%s): %s", task_md, e)
        return False


def _stall_redispatch(tid: str, task: Dict[str, Any], now_epoch: float,
                      cancel_fn=None, dispatch_fn=None, idle_fn=None,
                      warn_fn=None) -> Dict[str, Any]:
    """三次轻推无效后的改派。返回 {outcome, ...}。

    2611230959 乙的顺序与兜底：先确认可派（台账筛过的空闲执行者）→ 再取消 → 再派单。
    任一步抛错都接住：取消失败=原任务原样不动；派单失败=原任务已取消的事实写成更正
    说明（此前不发「已取消并改派」的成功警告——成功警告只在派单成功后发），记录文件
    写 redispatch-failed，由 _auto_resume 发通知。无空闲执行者 → outcome="no-idle"。
    """
    callee = str(task.get("callee") or task.get("to") or "")
    if cancel_fn is None:
        from nexusos_xdr.dispatchd import cancel_task as _ct  # 局部导入：避免循环依赖
        cancel_fn = _ct
    if dispatch_fn is None:
        from nexusos_xdr.dispatchd import dispatch as _dp
        dispatch_fn = _dp
    if idle_fn is None:
        idle_fn = _find_idle_executor
    if warn_fn is None:
        from nexusos_xdr import task_state as _ts
        warn_fn = _ts.append_warning

    new_callee = idle_fn(exclude=callee)
    if not new_callee:
        logger.info("[stall-watch] %s 无空闲执行者，不改派（待主控处置）", tid)
        return {"outcome": "no-idle"}

    task_md = Path(str(task.get("task_path") or ""))
    try:
        cancel_fn(tid, _internal_reason="stall-redispatch")
    except Exception as e:  # noqa: BLE001 — 取消失败=原任务原样，主控可正常接手
        logger.warning("[stall-watch] %s 改派中止：取消原任务失败: %s", tid, e)
        return {"outcome": "redispatch-failed", "stage": "cancel", "error": str(e)}
    try:
        _append_continuation(task_md, tid, callee, new_callee)
        result = dispatch_fn(new_callee, str(task_md), _RESUME_CALLER, wait=False)
    except Exception as e:  # noqa: BLE001 — 派单失败必须把「原任务已取消」写到明处
        logger.warning("[stall-watch] %s 已取消但派单失败(%s): %s", tid, new_callee, e)
        try:
            warn_fn(tid, f"停摆改派失败更正：原任务已取消但派给 {new_callee} 失败（{e}）；"
                         f"本任务停在「已取消、未改派」，待主控手工重派（{_RESUME_CALLER}）")
        except Exception as e2:  # noqa: BLE001 — 更正留痕失败也要把错误带回去
            logger.warning("[stall-watch] %s 更正留痕失败: %s", tid, e2)
        return {"outcome": "redispatch-failed", "stage": "dispatch",
                "error": str(e), "new_callee": new_callee}
    new_tid = str((result or {}).get("task_id") or "")
    warn_fn(tid, f"停摆改派：轻推 {_MAX_PUSHES} 次未接上，已取消并改派 {new_callee}"
                 f"（新任务 {new_tid}，{_RESUME_CALLER}）")
    logger.info("[stall-watch] %s 已改派 %s → 新任务 %s", tid, new_callee, new_tid)
    return {"outcome": "redispatched", "new_callee": new_callee, "new_task_id": new_tid}


def _auto_resume(tid: str, callee: str, task: Dict[str, Any], now_epoch: float,
                 st: Dict[str, Any], path: Path, post_fn, pane10_fn,
                 sleep_fn, steer_fn, redispatch_fn, warn_fn=None) -> None:
    """单个确认停摆任务的处置（轻推/改派/留痕）。从 tick 进入。"""
    pc = int(st.get("push_count") or 0)
    last_push = float(st.get("last_push_epoch") or 0)
    now_iso = datetime.fromtimestamp(now_epoch).astimezone().isoformat(timespec="seconds")

    def _rec(action: str, **kw):
        _append_record(path, {"task_id": tid, "callee": callee, "action": action,
                              "at_epoch": now_epoch, "at": now_iso, **kw})

    if pc >= _MAX_PUSHES:
        # 三次接不上：改派；无空闲执行者或改派失败后 10 分钟内不重试
        if now_epoch - float(st.get("redispatch_epoch") or 0) < _REDEPATCH_RETRY_SEC:
            return
        rd = redispatch_fn(tid, task, now_epoch)
        outcome = (rd or {}).get("outcome")
        if outcome == "redispatched":
            _rec("redispatched", new_callee=rd.get("new_callee"),
                 new_task_id=rd.get("new_task_id"))
            post_fn(f"停摆改派：{tid} 轻推 {_MAX_PUSHES} 次未接上，已取消并改派 "
                    f"{rd.get('new_callee')}（新任务 {rd.get('new_task_id')}）")
        elif outcome == "redispatch-failed":
            _rec("redispatch-failed", stage=rd.get("stage"), error=rd.get("error"),
                 new_callee=rd.get("new_callee"))
            st["redispatch_epoch"] = now_epoch
            post_fn(f"停摆改派失败：{tid}（阶段 {rd.get('stage')}：{rd.get('error')}）"
                    f"——任务去向见台账更正说明，待主控处置")
        else:
            _rec("redispatch-skip-no-idle")
            st["redispatch_epoch"] = now_epoch
            post_fn(f"停摆待处置：{tid} 轻推 {_MAX_PUSHES} 次未接上，但找不到空闲执行者，"
                    f"未取消未改派，待主控处置")
        return
    if pc > 0 and now_epoch - last_push < _PUSH_INTERVALS_SEC[pc - 1]:
        return  # 递增间隔未到
    msg = _RESUME_MSG.format(tid=tid)
    ok = steer_fn(callee, msg)
    _rec("push", n=pc + 1, steer_ok=ok)
    if warn_fn is not None:
        try:
            warn_fn(tid, f"停摆自动轻推 第{pc + 1}次（steer_ok={ok}）")
        except Exception as e:  # noqa: BLE001 — 台账警告失败不影响处置主链
            logger.warning("[stall-watch] 台账留痕失败(不阻断): %s", e)
    post_fn(f"停摆自动轻推：{tid} 执行者 {callee} 第 {pc + 1}/{_MAX_PUSHES} 次（steer_ok={ok}）")


_RESUME_MIN_AGE_SEC = 180.0   # 处置层最小年龄：创建/最近回调满 3 分钟（丙：不吃提示层 20 分钟门槛；
                              # 3 分钟依据：正常任务头 3 分钟内必有动作或回调，11-25 主控手工轻推也是两分钟内）
_TICK_LOCK = threading.Lock()  # 单飞：上一个 tick 没结束就跳过本次（甲）


def trigger_async(**tick_kwargs) -> "threading.Thread":
    """异步触发一轮 tick（daemon 线程，单飞由 tick 内部的非阻塞锁保证）。

    record_poll 在事件循环线程上被调，只能走这里——tick 里有 25 秒复核与子进程，
    同步执行会卡住整个消息总线（2611230959 甲）。
    """
    def _run():
        try:
            stall_watch_tick(**tick_kwargs)
        except Exception as e:  # noqa: BLE001 — 后台线程的错必须留日志，不许静默
            logger.warning("[stall-watch] 后台 tick 异常: %s", e)
    th = threading.Thread(target=_run, name="stall-watch-tick", daemon=True)
    th.start()
    return th


# ────────────────────────── 有副作用：tick ──────────────────────────

def _events_url() -> str:
    env = os.environ.get("P02_EVENTS_URL", "").strip()
    if env:
        return env
    try:
        from nexusos_xdr.p02_hardblock import _xdr_port
        return f"http://127.0.0.1:{_xdr_port()}/session/event"
    except Exception:
        return "http://127.0.0.1:19998/session/event"


def _post_notice(detail: str) -> bool:
    """往消息总线发一条 notice。成功 True；发不出去 False（不影响任何事）。"""
    try:
        body = json.dumps({"agent": "xdr", "type": "notice", "detail": detail},
                          ensure_ascii=False).encode()
        req = urllib.request.Request(_events_url(), data=body,
                                     headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=3).close()
        return True
    except Exception as e:  # noqa: BLE001 — 观测件
        logger.warning("[stall-watch] notice 发送失败(不阻断): %s", e)
        return False


def _load_remind_state(path: Path) -> Dict[str, float]:
    """从记录文件读「每个任务上次提醒时间」；读不到返回空 dict。"""
    state: Dict[str, float] = {}
    try:
        if path.exists():
            for line in path.read_text(encoding="utf-8", errors="replace").splitlines()[-200:]:
                try:
                    rec = json.loads(line)
                    tid, at = rec.get("task_id"), rec.get("remind_epoch")
                    if tid and isinstance(at, (int, float)):
                        prev = state.get(str(tid))
                        if prev is None or at > prev:
                            state[str(tid)] = float(at)
                except json.JSONDecodeError:
                    continue
    except Exception:  # noqa: BLE001
        pass
    return state


def _append_record(path: Path, rec: Dict[str, Any]) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    except Exception as e:  # noqa: BLE001 — 记录文件写不进去不影响主链
        logger.warning("[stall-watch] 记录文件写入失败(不阻断): %s", e)


def stall_watch_tick(now_epoch: Optional[float] = None,
                     state_path: Optional[Path] = None,
                     remind_state: Optional[Dict[str, float]] = None,
                     post=None, tasks=None, pane=None,
                     threshold_min: float = 20,
                     pane10=None, sleep_fn=None, steer=None,
                     redispatch=None, warn=None) -> List[Dict[str, Any]]:
    """单飞入口（2611230959 甲）：非阻塞锁拿不到=上一个 tick 还在跑，本次跳过。

    tick 里有 25 秒复核与最长 60 秒的 steer 子进程，绝不允许两个 tick 并发；
    调用方（心跳守护线程 / record_poll 异步触发）都走这里。
    """
    if not _TICK_LOCK.acquire(blocking=False):
        logger.info("[stall-watch] 上一个 tick 未结束，本次跳过（单飞）")
        return []
    try:
        return _stall_watch_tick_impl(
            now_epoch=now_epoch, state_path=state_path, remind_state=remind_state,
            post=post, tasks=tasks, pane=pane, threshold_min=threshold_min,
            pane10=pane10, sleep_fn=sleep_fn, steer=steer, redispatch=redispatch,
            warn=warn)
    except Exception as e:  # noqa: BLE001 — 处置层任何一步抛错记警告，不许静默
        logger.warning("[stall-watch] tick 异常: %s", e)
        return []
    finally:
        _TICK_LOCK.release()


def _stall_watch_tick_impl(now_epoch: Optional[float] = None,
                     state_path: Optional[Path] = None,
                     remind_state: Optional[Dict[str, float]] = None,
                     post=None, tasks=None, pane=None,
                     threshold_min: float = 20,
                     pane10=None, sleep_fn=None, steer=None,
                     redispatch=None, warn=None) -> List[Dict[str, Any]]:
    """巡检一次。返回本次实际发出提示的停摆任务列表（测试用）。

    可注入项（全部可选，默认走真实世界）：
      now_epoch     : 当前时间（假时钟）
      state_path    : 记录文件路径（测试指向临时文件）
      remind_state  : 「任务→上次提醒时间」字典；给了就不读写文件（内存去抖）
      post          : 发事件函数 detail->bool（测试替身）
      tasks         : 任务列表（测试替身；缺省 list_recent）
      pane          : 取面板函数 session->str（测试替身；提示层的末两行展示用）
      pane10        : 取面板末 10 行函数（2611230953 停摆判据用；缺省 pane_tail n=10）
      sleep_fn      : 复核等待函数（缺省 time.sleep；测试注入假时钟）
      steer         : 轻推函数 (session, msg)->bool（缺省 _steer_push 走 steer.sh）
      redispatch    : 改派函数 (tid, task, now_epoch)->dict（缺省 _stall_redispatch）
      warn          : 台账留痕函数 (tid, text)（缺省 task_state.append_warning）
    """
    global _TICK_LAST
    now_epoch = time.time() if now_epoch is None else float(now_epoch)
    if now_epoch - _TICK_LAST < _TICK_INTERVAL_SEC:
        return []
    _TICK_LAST = now_epoch

    path = state_path if state_path is not None else STALL_FILE
    own_state = remind_state is None
    if own_state:
        remind_state = _load_remind_state(path)
    post_fn = post if post is not None else _post_notice
    pane_fn = pane if pane is not None else pane_tail
    # 2611230953: 停摆判据用末 10 行（报错行+活动迹象都在这个窗口里）
    pane10_fn = pane10 if pane10 is not None else (lambda s: pane_tail(s, n=10))
    sleep_fn = sleep_fn if sleep_fn is not None else time.sleep
    steer_fn = steer if steer is not None else _steer_push
    redispatch_fn = redispatch if redispatch is not None else _stall_redispatch
    if warn is None:
        try:
            from nexusos_xdr import task_state as _tsw  # 局部导入：避免循环依赖
            warn = _tsw.append_warning
        except Exception:  # noqa: BLE001 — 台账留痕拿不到就只写 jsonl
            warn = None

    if tasks is None:
        try:
            from nexusos_xdr import task_state as _ts  # 局部导入：避免循环依赖
            tasks = _ts.list_recent(limit=5000) or []
        except Exception:  # noqa: BLE001
            tasks = []

    # 2611230953: 处置状态（轻推次数/相位）从同一记录文件重建
    stall_state = _load_stall_state(path)
    auto_resume = auto_resume_enabled()
    processed_callees: set = set()  # 同一执行者同一 tick 只处理一个停摆任务

    now_dt = datetime.fromtimestamp(now_epoch, tz=timezone.utc)
    fired: List[Dict[str, Any]] = []
    for s in stalled_tasks(tasks, now_dt, threshold_min=threshold_min):
        tid, callee, minutes = s["task_id"], s["callee"], s["minutes"]
        last = remind_state.get(tid)
        # 提示层冷却只gate「提示」，不 gate 自动处置（2611230953：轻推有自己的递增间隔）
        if last is None or now_epoch - last >= _REMIND_COOLDOWN_SEC:
            remind_state[tid] = now_epoch
            tail = pane_fn(callee) if callee else ""
            detail = (f"疑似停摆：{tid} 执行者 {callee} 已 {minutes} 分钟没有回调"
                      + (f"；面板末行：{tail}" if tail else "；面板末行：（取不到）"))
            post_fn(detail)
            if own_state:
                _append_record(path, {"task_id": tid, "callee": callee, "minutes": minutes,
                                      "remind_epoch": now_epoch,
                                      "at": datetime.fromtimestamp(now_epoch).astimezone().isoformat(timespec="seconds"),
                                      "pane_tail": tail})
            fired.append({"task_id": tid, "callee": callee, "minutes": minutes, "detail": detail})

    # === 处置层（2611230953 建，2611230959 丙修正：不吃提示层 20 分钟门槛）===
    # 遍历全部在跑任务，用自己的最小年龄（_RESUME_MIN_AGE_SEC）；挂起中的不碰。
    # 开关关掉 = 回到只提示。
    if auto_resume:
        for t in tasks or []:
            if str((t or {}).get("status") or "").upper() != "RUNNING":
                continue
            su = t.get("suspend_until")
            if su and now_epoch < float(su or 0):
                continue  # 挂起中（等窗口/外部事件）的任务不碰
            last_act = last_callback_ts(t)
            if last_act is None:
                continue
            if (now_dt - last_act).total_seconds() < _RESUME_MIN_AGE_SEC:
                continue  # 刚派出/刚有动作，不碰
            tid = str(t.get("task_id") or "")
            callee = str(t.get("callee") or t.get("to") or "")
            if not callee or callee in processed_callees:
                continue
            if _last_callback_status(t) == "need_decision":
                continue  # 在等主控回答的任务不碰
            st = stall_state.get(tid) or {"push_count": 0, "last_push_epoch": 0.0,
                                          "phase": "", "redispatch_epoch": 0.0}
            if st.get("phase") == "redispatched":
                continue  # 已改派过的任务不再处置（原任务随后会被取消终态，双保险）
            text1 = pane10_fn(callee)
            if not pane_error_idle(text1):
                if st.get("phase") == "pushing":
                    # 面板有活动迹象/报错消失 = 接上了：当前轮次清零，累计次数不清
                    st["phase"] = "resumed"
                    _append_record(path, {"task_id": tid, "callee": callee, "action": "resumed",
                                          "at_epoch": now_epoch,
                                          "at": datetime.fromtimestamp(now_epoch).astimezone().isoformat(timespec="seconds")})
                continue
            sleep_fn(_RECHECK_SEC)
            if not pane_error_idle(pane10_fn(callee)):
                continue  # 25 秒复核已有活动迹象：只是两次工具调用之间的空窗
            processed_callees.add(callee)
            _auto_resume(tid, callee, t, now_epoch, st, path,
                         post_fn, pane10_fn, sleep_fn, steer_fn, redispatch_fn, warn)
            stall_state[tid] = st
    return fired


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    out = stall_watch_tick()
    print(json.dumps(out, ensure_ascii=False, indent=2))
