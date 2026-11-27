"""needdecision_notify — need_decision 即时通知桥（Task909，按 904 设计稿 A 案）。

链路：callback handler 非终态分支（仅 need_decision）+ p02 P0-2 复核回调 →
fire() 后台线程 → maybe_push() 判定+组装 → notify_push.push() → 既有降级链（677）。

防轰炸四级（904 §3）：
  ① 真待决才推——handler 只对 need_decision 调本模块（progress/started 不进）
  ② 同任务去重窗——同 task_id 窗口内重复不推（默认 30min，NEXUS_ND_DEDUP_SEC）
  ③ 日限——L1 即时推 ≤N 条/日（默认 20，NEXUS_ND_DAILY_LIMIT），超限降级 L2 合并窗
  ④ 静默时段——默认穿透（卡点=生产线停，L1 深夜也推）；NEXUS_ND_SILENT=l2 可降晨报

2611192240 分级热修（用户 22:1x 投诉「这样的邮件能看？」三缺陷）：
  - gate_blocked 簿记类（p02 P0-2 链）= 主控运营事务，非用户决定事项——
    永不进即时档，降 L2 转日报缓存（⏳挂起区池行自然可见）
  - 真待决文案人话化：标题=卡点一句话（弃「need_decision（xx）」内部格式）；
    动作真填（选项透传/决策队列指引），禁「无需动作+尽快」自相矛盾

红线（904 R5）：通知层异常绝不外逃——maybe_push/fire 全 catch + logger.warning，
callback 热路径与通知完全解耦（fire-and-forget daemon 线程）。
"""
from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
from pathlib import Path

from nexusos_xdr import notify_push
from nexusos_common import subpath  # 1960：锚点单源（design §2.1 行6）

logger = logging.getLogger("nexusos_xdr.needdecision_notify")

# NEXUS_XDR_NOTIFY_DIR 目录级锚点（行5b/6 共用，design §2.1）
_STATE_DIR = Path(os.environ["NEXUS_XDR_NOTIFY_DIR"]) if os.environ.get("NEXUS_XDR_NOTIFY_DIR") else subpath("xdr/notify")
STATE_FILE_DEFAULT = _STATE_DIR / "nd_push_state.json"

# 904 §2 选项透传：「1. xxx」「2）xxx」「选项A xxx」形态行（编号+分隔：标点或空格）
_OPT_LINE = re.compile(r"^\s*(?:选项\s*)?(?:[0-9]+|[A-Ca-c])[\s\.、．）):：]\s*\S")

# 2611192240：gate_blocked 簿记类识别——口径=p02_hardblock._nd_fire 的固定
# 首行前缀「gate_blocked 待复核: REJECT=...」（p02_hardblock.py:423 单源）。
_GATE_BOOKKEEP_PREFIX = "gate_blocked"
_GATE_CODE_RE = re.compile(r"G\d+")


def _is_gate_bookkeeping(blocker: str) -> bool:
    return (blocker or "").lstrip().startswith(_GATE_BOOKKEEP_PREFIX)


def _gate_bookkeeping_view(task_id: str, blocker: str, source: str) -> tuple:
    """gate_blocked 簿记 → (title, body) 人话翻译层（2611192240 §B）。

    禁出现 REJECT 等即时档触发词——原文拼接会让 notify_push.RED_RE 再误判
    进即时档（本次垃圾邮件根因之二）。
    """
    codes = sorted(set(_GATE_CODE_RE.findall(blocker)))
    what = (f"任务 {task_id} 完成时门禁拦了 {'/'.join(codes) or '若干'} 项硬性检查，"
            "产出已转复核通道（池行已登记，owner 认领即闭环）")
    title = f"门禁拦截待复核（{source or 'gate'}）"
    return title, notify_push.format_body(
        blocker=what, background="主控运营事务，无需用户动作，详见日报⏳挂起区")


def _dedup_sec() -> int:
    try:
        return int(os.environ.get("NEXUS_ND_DEDUP_SEC", "1800"))
    except ValueError:
        return 1800


def _daily_limit() -> int:
    try:
        return int(os.environ.get("NEXUS_ND_DAILY_LIMIT", "20"))
    except ValueError:
        return 20


def _silent_mode() -> str:
    # passthrough（默认）= L1 静默穿透；l2 = 静默时段降级合并窗（904 ④）
    return os.environ.get("NEXUS_ND_SILENT", "passthrough").strip().lower()


def _load_state(path: Path) -> dict:
    try:
        return json.loads(path.read_text("utf-8"))
    except Exception:
        return {}


def _save_state(state: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=1), "utf-8")
    tmp.replace(path)


def _extract_options(summary: str, limit: int = 5) -> list:
    """904 §2：summary 中编号选项行透传（render_options 编号渲染），无则空表
    （render_message 走「见 Console 决策队列」默认行）。"""
    opts: list = []
    for line in (summary or "").splitlines():
        if _OPT_LINE.match(line):
            opts.append(line.strip())
            if len(opts) >= limit:
                break
    return opts


def maybe_push(task_id: str, summary: str, source: str = "",
               state_path=None) -> dict:
    """去重/日限判定 + 模板组装 + notify_push.push。永不 raise（904 R5 双保险）。

    2611192240：gate_blocked 簿记类降 L2 转日报（永不即时档）；真待决走
    人话文案（标题=卡点一句话，动作真填）。
    Returns: {"status": sent|queued|queued_silent|recorded|deduped|error,
              "level": L1|L2, "bookkeeping"?: True, ...}
    """
    path = Path(state_path) if state_path else STATE_FILE_DEFAULT
    now = time.time()
    try:
        state = _load_state(path)
        today = time.strftime("%Y-%m-%d")
        if state.get("day") != today:            # 日界：计数与任务窗自然重置
            state = {"day": today, "l1_count": 0, "tasks": {}}
        tasks = {k: v for k, v in (state.get("tasks") or {}).items()
                 if now - float(v) <= _dedup_sec()}
        if task_id in tasks:                      # 防轰炸②：同任务去重窗
            state["tasks"] = tasks
            return {"status": "deduped", "task_id": task_id}
        blocker = (((summary or "").strip().splitlines() or ["（无摘要）"])[0]
                   .strip() or "（无摘要）")
        if _is_gate_bookkeeping(blocker):
            # 2611192240 §A：簿记类=主控运营事务——降 L2 转日报缓存，
            # 人话翻译层确保不再命中即时档触发词
            title, body = _gate_bookkeeping_view(task_id, blocker, source)
            result = notify_push.push("L2", title, body)
            tasks[task_id] = now
            state["tasks"] = tasks
            _save_state(state, path)
            result["level"] = "L2"
            result["bookkeeping"] = True
            return result
        level = "L1" if int(state.get("l1_count", 0) or 0) < _daily_limit() else "L2"
        if level == "L1" and _silent_mode() == "l2" and notify_push._in_silence():
            level = "L2"                          # 防轰炸④：可配降级
        # 2611192240 §B/§C：真待决人话文案——标题=卡点一句话（弃内部格式）；
        # 动作真填（选项透传优先，无选项给决策队列指引），禁「无需动作」
        opts = _extract_options(summary or "")
        title = blocker[:40]
        body = notify_push.format_body(blocker=blocker[:120],
                                       background=summary or "")
        evidence = {"what": blocker[:120],
                    "urgency": "尽快" if level == "L1" else "不急"}
        if not opts:
            evidence["action"] = f"到决策队列裁决（来源 {source or '小弟'}）"
        result = notify_push.push(level, title, body, options=opts,
                                  decision_required=(level == "L1"),  # 2611192105：真待决显式标即时档
                                  evidence=evidence)
        tasks[task_id] = now
        state["tasks"] = tasks
        if level == "L1":
            state["l1_count"] = int(state.get("l1_count", 0) or 0) + 1
        _save_state(state, path)
        result["level"] = level
        return result
    except Exception as e:                        # 通知层异常绝不进 callback 热路径
        logger.warning(f"[Task909] maybe_push 异常（已吞）: {type(e).__name__}: {e}")
        return {"status": "error", "error": f"{type(e).__name__}: {e}"}


def fire(task_id: str, summary: str, source: str = "") -> None:
    """fire-and-forget 入口（handler 钩子调这个）：daemon 线程承载网络退避
    （deliver 最坏 21min），callback 响应零等待（904 §1 同步阻塞红线）。"""
    def _run():
        try:
            r = maybe_push(task_id, summary, source)
            logger.info(f"[Task909] need_decision 通知 task={task_id} "
                        f"→ {r.get('status')}/{r.get('level', '-')}")
            # Task1011 §二：通知留痕 events——观测层可见「已推送」事实，
            # 与 wait_callback 信号（主控感知路）互为印证。留痕失败不影
            # 响通知（fail-open）；本改动待 xdr 下次重启生效（不热加载）。
            if r.get("status") != "error":
                try:
                    from nexusos_xdr import task_state as _ts
                    from nexusos_xdr.session_state import append_event as _ev
                    _caller = ""
                    try:
                        _caller = str((_ts.load_state(task_id) or {}).get("caller") or "")
                    except Exception:
                        pass
                    _ev(source or "executor", "need_decision_notified",
                        f"need_decision 已推送用户 task={task_id}",
                        target=_caller)
                except Exception:
                    pass
        except Exception as e:                    # 双保险
            logger.warning(f"[Task909] fire 线程异常（已吞）: {e}")

    threading.Thread(target=_run, name="nd-notify", daemon=True).start()
