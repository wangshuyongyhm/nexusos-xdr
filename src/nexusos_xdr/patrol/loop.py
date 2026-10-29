#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""nexusos_xdr.patrol.loop — patrol 监控域（Task609 #57 从 dispatchd.py 迁入）。

物理迁入（Step A 原样）+ 可变状态收敛 PatrolState（Step B）：
    _check_output_completion_triple / _scan_task_dir_latest_mtime
    / _timeout_grace_decision / _start_patrol_thread（含闭包群）
    + parse_cost_from_pane（纯函数随迁）+ _post_webhook_with_retry（纯函数随迁）

架构（T604 试点三要素复用）：
    - 依赖方向：dispatchd → patrol 单向。本模块禁 import dispatchd；
      纯函数（_find_task_root）随迁本模块；有状态/跨域成员（PipePaneContext/
      _capture_apply_candidates/_consume_barrier_checkpoint/_active_patrols/
      trigger_auto_relocate）经 _start_patrol_thread 的 runtime 注入（模块级
      _deps 单例，bind_deps() 由 dispatchd import 本模块后立即调用）。
    - shim：dispatchd.py re-export 本模块公开名（外部引用/测试 patch 不变）。
"""
import json
import logging
import os
import re
import subprocess
import threading
import time
import urllib.request
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional

if TYPE_CHECKING:
    from nexusos_xdr.agents.base import AgentBase

from nexusos_xdr import task_state
from nexusos_common import subpath  # 1960：锚点单源（design §2.1 行10）
from nexusos_xdr.patrol.state import PatrolState

logger = logging.getLogger("nexusos-xdr.patrol")

# 常量随迁（原 dispatchd 模块级）
DEFAULT_TIMEOUT = 3600  # T674: 1800→3600（与 dispatchd/task_state 同值，构建/研究型任务实测 40-90min）
# Task627: T609 迁入时值漂移（误写 200）→ 回真值 25。证据链：git 4227d00 起
# dispatchd:309 真源=25 未变过；dispatch()/start_patrol_thread 两条路径均显式传 25
# （main.py:126 / dispatchd:604）；test_dispatchd_loop_caps.py:148 断言 ==25。
# 签名默认参数 import 期求值无法走 _dep()，与 dispatch/main.py"同值注释"法一致。
DEFAULT_MAX_STEPS = 25
DEFAULT_COST_BUDGET_USD = 2.0
PATROL_INTERVAL = 30
# T1574 B6: 挂起墙钟硬顶（秒，env NEXUS_SUSPEND_WALL_CAP 可调——未来有更长
# 设计内挂起时无需改码）。默认 48h：覆盖已知全部设计内挂起场景（如 14:00 快照
# 窗），只拦「反复续期致线程寿命无界」的失控形态。
SUSPEND_WALL_CAP = int(os.environ.get("NEXUS_SUSPEND_WALL_CAP", str(48 * 3600)))


# === Task609: runtime 注入位（依赖方向 dispatchd → patrol 单向）===
# dispatchd 域的有状态成员经 bind_deps 注入；import 本模块后、启动线程前绑定一次。
_deps = None


def bind_deps(dispatchd_module) -> None:
    """绑定 dispatchd 域依赖（PipePaneContext/_capture_apply_candidates/
    _consume_barrier_checkpoint/_active_patrols/trigger_auto_relocate/
    ENABLE_APPLY_CAPTURE 开关读取）。

    契约：dispatchd import 本模块后立即调用（模块加载尾部）；未绑定时 patrol
    线程启动会 RuntimeError（fail-fast 防静默半瘫）。幂等（重复绑定覆盖）。
    """
    global _deps
    _deps = dispatchd_module


def _dep(name: str):
    if _deps is None:
        raise RuntimeError(
            "nexusos_xdr.patrol 未 bind_deps——dispatchd import 后需立即调用 bind_deps(module)"
        )
    return getattr(_deps, name)


# === Cost 解析正则（多 CLI 格式，fail-open）===
# Claude Code: `Cost: $1.23` 或 `cost: $1.23` 或 `Total cost: $1.23`
_COST_CLAUDE = re.compile(r"(?:total\s+)?cost\s*[:：]\s*\$?\s*([\d]+(?:\.[\d]{1,4})?)", re.IGNORECASE)
# opencode/Claude Code 变体: `Tokens: input=1234 output=567` → 按 $3/M 粗算
# （仅当无显式 cost 行时用 token 反推；避免双重计入）
_TOKENS_LINE = re.compile(
    r"tokens?\s*[:：]\s*input\s*[=：]?\s*(\d+)\s*(?:tokens?)?\s*[,/]?\s*output\s*[=：]?\s*(\d+)",
    re.IGNORECASE,
)
# token 反推成本单价（$/M tokens，按 Claude Sonnet 区间粗估）
_TOKENS_TO_USD_PER_M = 3.0




# === Task 1849: cost 读数背离检测（pane 快照噪声根修）===
# 10-19 1842 误杀实录：omp pane 快照第 400 轮出现 "$59.00" UI 文本（前 399 轮
# 恒 $0.00，真实花费 3.54 元），parse_cost_from_pane 把它当可信累计花费 ratchet
# 进 cost_so_far → cost cap 触发 → 60s 宽限强杀健康任务。pane 是 UI 噪声源
# （TUI 状态栏/会话统计），不是计费台账——杀单决策必须过滤跳变读数。
# 原则：宁可漏杀不可误杀，漏杀有 WARNING 告警人工兜底。
COST_DIVERGENCE_FACTOR = 3.0       # 读数 > 可信值×3 且超 $1 绝对增量 = 疑似背离
COST_DIVERGENCE_ABS_USD = 1.0
# 背离读数连续 N 轮「比上轮背离读数更高」才升级为可信——真实暴涨的读数在
# 爬升（59→61→63），UI 噪声跳变后恒定（59→59→59）。恒定背离永不升级（只告警）。
COST_DIVERGENCE_RISING_ROUNDS = 2


def _cost_reading_is_divergent(trusted: float, reading: float) -> bool:
    """Task 1849: cost 读数相对可信累计值是否构成跳变背离（疑似 pane 噪声）。"""
    return reading > trusted * COST_DIVERGENCE_FACTOR + COST_DIVERGENCE_ABS_USD


def _cost_apply_reading(pstate: "PatrolState", parsed_cost: float) -> str:
    """Task 1849: 对单轮 cost 读数做背离去噪决策（就地更新 pstate）。

    Returns:
        "accepted": 正常读数（或背离后恢复），已 ratchet 进 cost_so_far
        "divergent": 疑似坏读数（跳变背离且未达升级阈值），cost_so_far 不动
        "escalated": 背离读数连续上升达 RISING_ROUNDS，判定真实暴涨，已接受
    """
    if _cost_reading_is_divergent(pstate.cost_so_far, parsed_cost):
        pstate.cost_divergent_count += 1
        # rising 边要求 ≥$1 实质增量——UI 噪声的 ±小数抖动不算上升
        if parsed_cost >= pstate.cost_last_divergent_reading + COST_DIVERGENCE_ABS_USD:
            pstate.cost_divergent_rising += 1
        else:
            pstate.cost_divergent_rising = 0
        if pstate.cost_divergent_rising >= COST_DIVERGENCE_RISING_ROUNDS:
            pstate.cost_so_far = max(pstate.cost_so_far, parsed_cost)
            pstate.cost_divergent_count = 0
            pstate.cost_divergent_rising = 0
            pstate.cost_last_divergent_reading = 0.0
            return "escalated"
        pstate.cost_last_divergent_reading = parsed_cost
        return "divergent"
    # 正常读数（含背离后恢复）：ratchet + 清零背离态
    pstate.cost_so_far = max(pstate.cost_so_far, parsed_cost)
    pstate.cost_divergent_count = 0
    pstate.cost_divergent_rising = 0
    pstate.cost_last_divergent_reading = 0.0
    return "accepted"


def parse_cost_from_pane(pane_text: str) -> Optional[float]:
    """Task 297 P0-2: 从 tmux pane 提取**累计**成本（美元）。

    Fail-open 设计：
      - 解析不到 → 返 None（调用方不参与判断，不误判为 $0）
      - 解析到多行 → 取**最大值**（patrol 抓的是当前快照，CLI 通常只显示累计 cost）
      - 既有 cost 行又有 tokens 行 → 优先 cost 行（tokens 是反推，双源会重复计入）

    Args:
        pane_text: tmux capture-pane 输出
    Returns:
        累计成本（USD，float），或 None（解析失败）
    """
    if not pane_text:
        return None
    # 优先级 1: 显式 cost 行
    costs = []
    for m in _COST_CLAUDE.finditer(pane_text):
        try:
            costs.append(float(m.group(1)))
        except ValueError:
            continue
    if costs:
        return max(costs)  # 累计值取快照内最大
    # 优先级 2: tokens 行反推（仅当无 cost 行时）
    token_total = 0
    for m in _TOKENS_LINE.finditer(pane_text):
        try:
            inp = int(m.group(1))
            out = int(m.group(2))
            token_total += inp + out
        except (ValueError, IndexError):
            continue
    if token_total > 0:
        return round(token_total / 1_000_000 * _TOKENS_TO_USD_PER_M, 4)
    return None


def _post_webhook_with_retry(notify_url: str, payload: bytes, attempts: int = 3,
                              timeout: float = 5, retry_sleep: float = 1.0) -> str:
    """场景5 webhook POST + 重试（Task596 从 _notify_caller 闭包抽出，行为零变化：
    1 首发 + N-1 重试；非 200 视为失败；成功返回 None，全败返回 last_err 描述。
    抽出目的：notify 重试路径可用故障注入服务器做真实演练（池 #52）。"""
    import urllib.request
    last_err = None
    for attempt in range(attempts):
        try:
            req = urllib.request.Request(notify_url, data=payload,
                                         headers={"Content-Type": "application/json"}, method="POST")
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                if resp.status == 200:
                    return None
                last_err = f"HTTP {resp.status}"
        except Exception as e:
            last_err = str(e)
        if attempt < attempts - 1:
            time.sleep(retry_sleep)
    return last_err

# Task 280 P1-9: 全局 patrol 线程 registry（task_id -> stop_event）
# 让 cancel_task / shutdown 能统一 stop patrol，避免孤儿线程空转到 timeout


def _find_task_root(task_path: str) -> Optional[Path]:
    """从 task_path（input/task.md）推导任务根目录（含 output/ 的目录）。

    Task 282 P2-6: task_path 可能是任务根/input/task.md/其他——回退 parent。
    """
    p = Path(task_path).parent
    for candidate in [p, p.parent]:
        if (candidate / "output").is_dir() or (candidate / "input").is_dir():
            return candidate
    return p


def _check_output_completion_triple(
    agent: "AgentBase",
    task_id: str,
    task_path: str,
    dispatch_start_time: float,
    pstate,  # PatrolState（Task639 🟡6: 原单元素 list 桥接协议废除——真源唯一化，
    #         reset/累加直接读写 pstate.idle_confirmation_count，无镜像同步面）
    done_threshold: int,
    output_stable_sec: int,
) -> bool:
    """场景1: 产出文件 + agent 持续 idle + 文件稳定 → 判 done（四重确认 + popup 检测）。

    独立为模块级函数便于单元测试。patrol_loop 的闭包只负责传参。

    四重确认（Task 274 三重 + Task 303 第四重）+ popup 检测（08040834）：
    1. output/ 有 size≥50 + mtime 晚于 dispatch_start_time 的文件
    2. agent.is_idle() 连续 True done_threshold 轮（默认 3 轮 ~90 秒）
    2.5. check_popup() 不命中（08040834 新增，防审批弹窗假 idle）
    3. output 最新文件 mtime 停止更新 output_stable_sec 秒（默认 60s）
    4. **task_dir 全目录**（output/+process/+根目录）最近 output_stable_sec 秒无新写入

    任何一条件不满足 → 重置 pstate.idle_confirmation_count → 不判 done。

    历史：
    - 原版: output + 单次 idle → done（Task 273 实锤误判：kimi 思考间隙被误判）
    - Task 274: 三重确认，防 kimi 思考间隙被误判
    - Task 303 (2026-07-21): 加第四重——task_dir 全目录稳定，防 AC 写中间脚本
      (fix_phase2_1.py 在 process/ 或根目录) 时被误判。原版只看 output/，
      AC Phase 1 写 report.md §1 后改 .py 脚本，patrol 看 output/ 稳定+idle
      就误判 DONE，但 AC 实际还有 5 项未完成。
    - 08040834: 加 Check 2.5——popup 阻塞检测。ds2 写完 report 后卡在 done 回调的
      task_shell_start 审批弹窗，四重全过误判 DONE，停 patrol 后 ds2 永远卡着。
    """
    if not task_path:
        pstate.idle_confirmation_count = 0
        return False
    task_dir = _find_task_root(task_path)
    if task_dir is None:
        pstate.idle_confirmation_count = 0
        return False
    output_dir = task_dir / "output"
    if not output_dir.is_dir():
        pstate.idle_confirmation_count = 0
        return False

    # === Check 1: 有 fresh output ===
    latest_mtime = 0.0
    has_fresh_output = False
    try:
        for f in output_dir.iterdir():
            if not f.is_file():
                continue
            st = f.stat()
            if st.st_size >= 50 and st.st_mtime >= dispatch_start_time:
                has_fresh_output = True
                if st.st_mtime > latest_mtime:
                    latest_mtime = st.st_mtime
    except Exception:
        pstate.idle_confirmation_count = 0
        return False

    if not has_fresh_output:
        pstate.idle_confirmation_count = 0
        return False

    # === Check 2: agent idle ===
    if not agent.is_idle():
        pstate.idle_confirmation_count = 0
        return False

    # === Check 2.5 (08040834 NEW): popup 阻塞检测 ===
    # Agent 卡在审批弹窗时，is_idle() 可能为 True（无主动输出，看似空闲），
    # 但实际被阻塞——此时不应判 done。
    # 08040834 会话 ds2 实锤：写完 report 后卡在 done 回调的 task_shell_start 审批，
    # patrol 四重全过误判 DONE，停 patrol 线程，ds2 永远卡着（20 分钟未完成）。
    # check_popup 会发 rescue_keys（自动处理），命中则 reset idle count 继续等。
    try:
        if agent.check_popup():
            pstate.idle_confirmation_count = 0
            logger.info(f"[patrol] {task_id} 检测到审批弹窗，已发救援键，不判 done")
            return False
    except Exception as e:
        logger.debug(f"[patrol] {task_id} check_popup 异常（fail-open 跳过）: {e}")

    # === Check 3 (Task 274): output/ 文件稳定 ===
    now = time.time()
    file_age_sec = now - latest_mtime

    if file_age_sec < output_stable_sec:
        # output/ 文件最近还在写，重置
        pstate.idle_confirmation_count = 0
        logger.debug(
            f"[patrol] {task_id} output 还在写"
            f"（{file_age_sec:.0f}s<{output_stable_sec}s），等"
        )
        return False

    # === Check 4 (Task 303 NEW): task_dir 全目录稳定 ===
    # 防 AC 写中间脚本（fix_phase2_1.py 在 process/ 或根目录）时被误判
    # 扫 task_dir 下所有文件（递归一层，含 output/process/根目录），
    # 任一文件在 output_stable_sec 内被写过 → 小弟还在干别的，不能判 done
    try:
        latest_task_mtime = _scan_task_dir_latest_mtime(task_dir, dispatch_start_time)
    except Exception as e:
        logger.debug(f"[patrol] {task_id} task_dir 扫描异常（fail-open 跳过 Check 4）: {e}")
        latest_task_mtime = latest_mtime  # fail-open：退化到 Check 3 行为

    task_file_age_sec = now - latest_task_mtime
    if task_file_age_sec < output_stable_sec:
        # task_dir 某文件最近还在写（如 .py 脚本），小弟还在干别的，重置
        pstate.idle_confirmation_count = 0
        logger.debug(
            f"[patrol] {task_id} task_dir 还在写"
            f"（{task_file_age_sec:.0f}s<{output_stable_sec}s，"
            f"latest_task_mtime={latest_task_mtime:.0f}），等"
        )
        return False

    # 累计 idle 确认
    pstate.idle_confirmation_count += 1
    logger.info(
        f"[patrol] {task_id} idle 确认 {pstate.idle_confirmation_count}/{done_threshold}"
        f"（output 稳定 {file_age_sec:.0f}s，task_dir 稳定 {task_file_age_sec:.0f}s）"
    )

    if pstate.idle_confirmation_count < done_threshold:
        return False

    # 四重全过 → 真 done
    logger.info(f"[patrol] {task_id} 四重确认全过，判 DONE")
    # 重置以便下次（如宽限检测）能再次判定
    pstate.idle_confirmation_count = 0
    return True


def _scan_task_dir_latest_mtime(task_dir: Path, dispatch_start_time: float) -> float:
    """Task 303: 扫 task_dir 下小弟产出文件的最近 mtime（递归三层）。

    用于 patrol output_detection 的第四重确认——防止小弟在 process/ 或根目录
    写中间脚本（如 fix_phase2_1.py）时被误判 done。

    **跳过 input/ 目录**——input/ 是任务书/dispatched 副本，不是小弟产出，
    扫了会把任务书本身（创建时间接近 dispatch_start_time）误识为"还在写"。

    Args:
        task_dir: task 根目录（含 output/ 或 input/）
        dispatch_start_time: dispatch 开始时间，早于此的文件忽略

    Returns:
        最新 mtime（float, epoch seconds）。无文件返 0.0。
    """
    latest = 0.0
    try:
        for root, dirs, files in os.walk(task_dir):
            # 跳过 input/ 目录（任务书，不是小弟产出）
            depth_parts = Path(root).relative_to(task_dir).parts
            if depth_parts and depth_parts[0] == "input":
                dirs[:] = []
                continue
            # 防深度爆炸：限制 3 层
            if len(depth_parts) >= 3:
                dirs[:] = []
                continue
            for fname in files:
                fpath = Path(root) / fname
                try:
                    st = fpath.stat()
                    if st.st_mtime >= dispatch_start_time and st.st_mtime > latest:
                        latest = st.st_mtime
                except OSError:
                    continue
    except Exception:
        pass
    return latest


def _timeout_grace_decision(elapsed: float, timeout: int, grace_sec: int) -> str:
    """Task587: 超时判定后的二次确认窗口决策（模块级纯函数便于单测）。

    patrol 在 elapsed > timeout 后不立即终态化 TIMEOUT，而是进入 grace_sec
    二次确认窗口：窗口内到达的真 done（回调置 DONE，由循环顶部终态检测
    捕获；或 §2 输出检测命中）直接正常终态化；窗口过后才标 TIMEOUT。

    Returns:
        "not_due": 尚未到超时阈值
        "wait": 窗口内，继续监控不标 TIMEOUT
        "timeout": 窗口已过，应标记 TIMEOUT
    """
    if elapsed <= timeout:
        return "not_due"
    if elapsed <= timeout + grace_sec:
        return "wait"
    return "timeout"


def _suspend_in_window(state: Dict[str, Any], now: Optional[float] = None) -> bool:
    """state 的 suspend_until 是否仍在挂起窗口内（Task1590 P3-2 单一真源）。

    B2 超时临界重读与写前最终复核共用同一判定——两处口径必须一字不差，
    否则「重读说没挂起、写前说挂起」的口径差本身就是新竞态。坏值
    （None/非数字）按「不在窗」处理，与原 B2 内联 float 容错语义一致。
    """
    _susp = (state or {}).get("suspend_until")
    if not _susp:
        return False
    try:
        return (now if now is not None else time.time()) < float(_susp)
    except (TypeError, ValueError):
        return False


# ============================================================================
# Task1589 (池#238): PENDING 投递静默安全网——全局巡检，告警-only
# ============================================================================
# 这是投递确认安全网——小弟没接单（无 started、无任何回调）时任务会永远
# 躺在 PENDING，这里 10 分钟就喊人（事件流 alert + task_state warnings）。
# 只告警不重投不标终态：首次投递可能实际已达、重投=双执行风险（池#238
# 已排除重投方向），处置权留给主控（steer 或重派）。
#
# 为什么是全局巡检而不是塞进下面的 patrol_loop：1568 实录（PENDING 9.4h
# 无人发现）里 per-task patrol 线程本身就没随 dispatch 起来——检查挂在
# 它身上等于没人看着"没人看门的人"。这里独立线程扫 list_active() 全量，
# 服务进程活着就在岗；懒启动=每次 dispatch 拉起 ensure（幂等）。
PENDING_SILENCE_MINUTES = int(os.environ.get("NEXUS_PENDING_SILENCE_MINUTES", "10"))
# Task1703 件二（池#238 重投/TUI 回执半边）：dispatch 后仍无 started 无回调
# 的自动重投阈值（秒）。取 180s：小弟正常读任务书后立即 started，3 分钟
# 零回调=投递未被消化的强信号（收尾态 TUI 吞文本场景）；且早于 1589 告警
# 阈值（10min）——重投成功则告警半边自然不触发（衔接不重复报）。
DELIVERY_RESEND_SEC = int(os.environ.get("NEXUS_DELIVERY_RESEND_SEC", "180"))

_pending_silence_thread: Optional["threading.Thread"] = None


def _pending_silence_seconds(state: Dict[str, Any]) -> Optional[float]:
    """PENDING 任务已静默秒数；不满足判定前提返回 None（不算静默单）。

    静默三前提：PENDING + 从未 started + 0 回调（主控侧看不到小弟任何
    动静）。suspend_until 挂起窗口内的任务是设计内挂起（1543 的 14:00
    快照窗类），与超时判定同口径豁免，不算静默。
    """
    if state.get("status") != "PENDING":
        return None
    if state.get("started") is not None:
        return None
    if state.get("callbacks"):
        return None
    _susp = state.get("suspend_until")
    if _susp:
        try:
            if time.time() < float(_susp):
                return None
        except (TypeError, ValueError):
            pass
    try:
        import datetime as _dt
        _c = _dt.datetime.fromisoformat(str(state.get("created", "")))
        if _c.tzinfo is None:
            _c = _c.replace(tzinfo=_dt.timezone.utc)
        return (_dt.datetime.now(_dt.timezone.utc) - _c).total_seconds()
    except Exception:
        return None


def check_pending_silence_once() -> List[str]:
    """单轮巡检：静默超阈值 → 告警（事件流 alert + warnings 落账），同任务只报一次。

    once_flag 去重（T657 惯例）防 30s 巡检刷屏；返回本轮新告警的 task_id
    列表（巡检线程与测试共用同一判定路径）。
    """
    alerted: List[str] = []
    _threshold = PENDING_SILENCE_MINUTES * 60
    try:
        _active = task_state.list_active()
    except Exception as e:
        logger.debug(f"[pending-silence] list_active 异常（本轮跳过）: {e}")
        return alerted
    for _st in _active:
        _tid = str(_st.get("task_id", ""))
        try:
            _silent = _pending_silence_seconds(_st)
            if _silent is None or _silent < _threshold:
                continue
            if not task_state.once_flag(_tid, "pending_silence_alert"):
                continue  # 已告警过，防巡检刷屏
            _detail = (
                f"[pending-silence] {_tid} PENDING 已静默 {_silent / 60:.0f} 分钟"
                f"无 started 无回调（目标 {_st.get('callee', '?')}）——"
                f"疑似投递未消化，主控可 steer 或重派（Task1589 池#238）"
            )
            task_state.append_warning(_tid, _detail)
            logger.warning(_detail)
            try:
                from nexusos_xdr import session_state as _ss1589
                _ss1589.append_event(
                    agent="xdr", event_type="alert", detail=_detail, target=_tid
                )
            except Exception as _e1589:
                logger.debug(f"[pending-silence] {_tid} alert 事件写入失败（不阻塞）: {_e1589}")
            alerted.append(_tid)
        except Exception as e:
            logger.debug(f"[pending-silence] {_tid} 单任务检查异常（不阻塞）: {e}")
    return alerted


def ensure_pending_silence_patrol() -> None:
    """懒启动全局 PENDING 静默巡检 daemon 线程（幂等；dispatch 首单拉起）。

    间隔 PATROL_INTERVAL（30s）跑一轮 check_pending_silence_once；
    Event.wait 间隔与 patrol_loop 同惯例（可中断等待，非裸 sleep）；
    fail-open：任何轮异常只 log，线程常驻随服务进程。
    """
    global _pending_silence_thread
    if _pending_silence_thread is not None and _pending_silence_thread.is_alive():
        return
    import threading

    _stop_evt = threading.Event()

    def _pending_silence_loop():
        logger.info(
            f"[pending-silence] 全局投递静默巡检启动"
            f"（间隔 {PATROL_INTERVAL}s，阈值 {PENDING_SILENCE_MINUTES} 分钟，Task1589）"
        )
        while not _stop_evt.wait(PATROL_INTERVAL):
            try:
                check_pending_silence_once()
            except Exception as e:
                logger.debug(f"[pending-silence] 巡检轮异常（继续）: {e}")

    _pending_silence_thread = threading.Thread(
        target=_pending_silence_loop, daemon=True, name="pending-silence-patrol"
    )
    _pending_silence_thread.start()


# Task1590 (P3-3): registry 锁——防两个 patrol 同时挂同一任务。字典读写不是
# 原子的：并发双注册都读到 None 时不加锁会双挂（双判定/双 pipe-pane tee）。
_registry_lock = threading.Lock()


def _b7_register(task_id: str, stop_event) -> None:
    """registry check-and-set（持锁，Task1590 P3-3 收口 T1574 B7 残窗）。

    锁内完成 读旧→请求旧退→写新，消除「双读 None→双挂」微秒窗；语义与
    T1574 B7 一字不差：异 event（真双挂）→ set 请求旧线程退出后覆盖；
    同 event（调用方预注册契约形态，dispatch/main.py pop-then-register 及
    同型 helper）→ 幂等无动作，绝不 set（set 会误停自己即将跑的循环）。
    """
    with _registry_lock:
        _old_evt_b7 = _dep("_active_patrols").get(task_id)
        if _old_evt_b7 is not None and _old_evt_b7 is not stop_event:
            logger.warning(
                f"[patrol] {task_id} registry 已有异旧条目，set 请求旧 patrol 退出后覆盖（B7 防双挂）"
            )
            _old_evt_b7.set()
        _dep("_active_patrols")[task_id] = stop_event


def _b7_unregister(task_id: str, stop_event) -> None:
    """registry 清理（持锁）：只清自己的条目。

    身份比较是 T1574 B7 的语义锚（G15）——B7 set 旧线程退出时 registry 已被
    新 patrol 覆盖，无条件 pop 会误删新条目致新 patrol 脱管（cancel 停不到）。
    """
    with _registry_lock:
        _active_b7 = _dep("_active_patrols")
        if _active_b7.get(task_id) is stop_event:
            _active_b7.pop(task_id, None)
            logger.debug(f"[patrol] {task_id} registry 已清理")


def _start_patrol_thread(agent: "AgentBase", task_id: str, stop_event,
                         task_path: str = "", timeout: int = DEFAULT_TIMEOUT,
                         notify_url: str = "",
                         max_steps: int = DEFAULT_MAX_STEPS,
                         cost_budget_usd: float = DEFAULT_COST_BUDGET_USD,
                         barrier_cp_path: str = "") -> None:
    """启动 B 兜底监控线程（设计 §4.2 Step 7 + 场景1/2/4/5 预案）。

    每 PATROL_INTERVAL 秒检查：
    1. 小弟 TUI 弹窗 → 发 rescue_keys（B 兜底）
    2. 三条件输出检测：空闲 + 无busy + report.md存在 → 主动标 DONE（场景1）
    3. stuck 检测：输出连续 N 次无变化 → 只通知不中断（场景2）
    4. 最大存活时间到期 → 退出防泄漏（场景4）
    5. 任务终态 → 通知 caller webhook（场景5）
    6. Task 297 P0-1: step cap——工具调用计数达 max_steps → 注入 reminder
    7. Task 297 P0-2: cost budget——累计成本达 cost_budget_usd → 注入 reminder

    daemon 线程，stop_event.set() 后退出；或存活超过 timeout 自动退出。
    """
    import threading

    # Task1589 (池#238): 懒启动全局 PENDING 静默巡检（dispatch 首单拉起，
    # 幂等 ensure——1568 式"patrol 没起来"场景由这个独立线程兜住）
    try:
        ensure_pending_silence_patrol()
    except Exception as _e1589:
        logger.debug(f"[pending-silence] 懒启动异常（不阻塞 dispatch）: {_e1589}")

    # 场景2: stuck 检测状态（Task609 Step B 收敛进 PatrolState：pstate.last_pane_hash 等）
    # Task 273 P1-2 修正：阈值 6→10（180s→300s，约5分钟，防 oc 思考/大文件处理被误判）
    STUCK_THRESHOLD = 10
    # Task 273 P1-2 修正：STUCK 通知冷却（同 task_id 5 分钟内只通知一次，防刷屏）
    STUCK_NOTIFY_COOLDOWN_SEC = 300  # 5 分钟冷却

    # Task627: T609 迁移遗漏名修复——常量经 _dep() 从 dispatchd 真源解析
    # （bind_deps 单向注入契约；monkeypatch dispatchd 成员即时生效；
    # 587 源级守卫测试要求的裸名调用串 _timeout_grace_decision(elapsed,
    # timeout, TIMEOUT_GRACE_SEC) 保持不变——局部别名满足解析）
    TIMEOUT_GRACE_SEC = _dep("TIMEOUT_GRACE_SEC")
    CAP_GRACE_PERIOD_SEC = _dep("CAP_GRACE_PERIOD_SEC")

    # 测试4 修正: 记录 dispatch 开始时间，只认此后新建/修改的 report.md
    dispatch_start_time = time.time()

    # === Task609 Step B: 十个单元素 list 收敛为 PatrolState 显式对象 ===
    # 原 [0]/[None] 惯用法跨闭包传递可变状态——收敛后 state.xxx 属性读写
    # （同一实例被全部闭包共享，T601 同对象纪律）。语义逐项保留：
    #   idle 确认计数 / stuck 三态 / 死亡轮 / step-cost cap 计数与触发闸。
    # 历史依据：Task 274 三重确认（kimi 思考间隙误判）→ Task 303 第四重
    # （AC 写中间脚本）→ 08040834 popup 检测（ds2 卡审批）。
    pstate = PatrolState()
    DONE_CONFIRMATION_THRESHOLD = 3          # 连续 N 轮 patrol idle 才认 done
    OUTPUT_STABLE_THRESHOLD_SEC = 60         # output 文件 N 秒未变才算稳定

    def _check_output_completion():
        """场景1: 产出文件 + agent 持续 idle + 文件稳定 → 判 done。

        Task 274: 委托到模块级 _check_output_completion_triple 便于测试。
        Task639 🟡6: 直传 pstate（单一真源=PatrolState 实例，无桥接/镜像）。
        """
        _done = _check_output_completion_triple(
            agent=agent,
            task_id=task_id,
            task_path=task_path,
            dispatch_start_time=dispatch_start_time,
            pstate=pstate,
            done_threshold=DONE_CONFIRMATION_THRESHOLD,
            output_stable_sec=OUTPUT_STABLE_THRESHOLD_SEC,
        )
        return _done

    def _notify_caller(status: str, result: dict = None):
        """场景5: 通过 webhook 通知 caller。

        B5 修复(2026-07-29): 原逻辑单次 POST 失败即丢(logger.warning 后无补偿)。
        网络抖动致通知丢失时,caller 永远不知道任务完成。
        修复: ① 加 2 次重试(间隔 1s) ② 全部失败写 task_state 标记 notify_failed,
        主控 GET /task/{id} 可见 ③ 读取并校验响应体(非 200 视为失败)。
        """
        if not notify_url:
            return
        import urllib.request
        import json as _json
        import time as _time
        # body 扁平化兼容 19999 /report 格式（summary 提到顶层，加 source/session）
        r = result or {}
        body = {
            "task_id": task_id,
            "status": status,
            "source": "xdr",
            "session": "",
            "summary": r.get("summary", ""),
            "files": r.get("files", []),
            "result": r,
        }
        payload = _json.dumps(body, ensure_ascii=False).encode()
        # Task596: 重试循环抽出为 _post_webhook_with_retry（可测化，行为零变化）
        last_err = _post_webhook_with_retry(notify_url, payload)
        if last_err is None:
            logger.info(f"[patrol] {task_id} 已通知 caller {notify_url}: {status}")
            return
        # 3 次全败: 写 task_state 标记,主控可见
        logger.error(f"[patrol] {task_id} 通知 caller 失败(3次重试全败): {last_err}")
        try:
            # B5回归修复(agent_f884f44a发现): 原用裸 save_state 绕过 _state_lock,
            # 与并发 update_status/callback 产生 RMW 竞态可能覆盖字段。
            # 改用 update_field 走锁,只写 notify_failed 字段不碰其他。
            task_state.update_field(task_id, "notify_failed",
                                    {"url": notify_url, "status": status, "error": last_err})
        except Exception:
            pass  # 标记失败不阻断(已是最后手段)

    def _mark_done(method: str, result: dict = None) -> bool:
        """标 DONE 并通知 caller（output_detection / 19999_callback 共用）。

        Task476 修复: update_status(DONE) 前先跑 done gate。
        gate 不过 → 不标 DONE（等小弟真实 done 回调走 gate），返回 False。
        fail-open: gate_engine 故障 → 降级原逻辑直接标 DONE。
        返回 True=已标 DONE，False=gate 拦截未标（patrol 继续等）。
        """
        r = result or {"summary": "patrol 输出检测判定完成", "method": method}
        logger.info(f"[patrol] {task_id} 判定疑似 DONE（method={method}），跑 done gate 验证")

        # === Task476: 先跑 done gate（防 patrol 误判绕过 G1-G5）===
        try:
            from nexusos_gate.gate_engine import check as _gate_check  # Task668 ②
            # 组装 gate context（patrol 自己收集，不从 callback body 取）
            # Task639 🟡8: 空 task_path（合法默认值）防御——Path("").parent.parent
            # ="."→G4 查 cwd 相对错位置（误判/误拦）。无 task_path 即无 gate 前置，
            # 对齐 :517 gate 异常 fail-open 降级（log 可见，不静默走错位置）。
            if not task_path:
                raise ValueError("task_path 为空，done gate 无法定位任务根（fail-open 降级标 DONE）")
            _task_root = Path(task_path).parent.parent  # input/ 的 parent = task 根
            _report_path = str(_task_root / "output" / "report.md")
            # 收集 output/ 文件列表（相对路径，gate files_exist 会补 Vault 根）
            _output_dir = _task_root / "output"
            _files = []
            if _output_dir.is_dir():
                from nexusos_xdr._vault_env import VAULT_ROOT as _VR
                _vault_root = _VR
                _files = [
                    str(f.relative_to(_vault_root)) if str(f).startswith(str(_vault_root)) else str(f)
                    for f in _output_dir.iterdir() if f.is_file() and f.stat().st_size > 0
                ]
            _gate_ctx = {
                "task_id": task_id,
                "task_path": task_path,
                "task_path_report": _report_path,
                "files": _files,
                "summary": r.get("summary", ""),
                "experiences": [],  # patrol 不知道小弟写了什么经验
                "source": agent.name,
            }
            _ge_result = _gate_check("done", _gate_ctx, agent=agent.name)
            if not _ge_result["passed"]:
                _fails = [
                    f"{f['id']}: {f.get('detail', '')[:80]}"
                    for f in _ge_result["failures"]
                    if f.get("status") == "fail" and f.get("severity") == "REJECT"
                ]
                logger.warning(
                    f"[Task476][patrol] {task_id} done gate 未过，不标 DONE（等小弟真实回调）：{_fails}"
                )
                return False  # 不标 DONE，patrol 继续等
        except ImportError:
            logger.warning(f"[Task476][patrol] {task_id} gate_engine 不可用（降级原逻辑直接标 DONE）")
        except Exception as _ge:
            logger.warning(f"[Task476][patrol] {task_id} done gate 异常（fail-open 降级）：{_ge}")

        # gate 过了（或 fail-open 降级）→ 正常标 DONE
        logger.info(f"[patrol] {task_id} done gate 通过，标 DONE（method={method}）")
        update_ok = True
        try:
            task_state.update_status(task_id, "DONE", result=r, end_reason="completed")
        except ValueError as e:
            update_ok = False
            logger.info(f"[patrol] {task_id} update_status DONE 跳过(可能 callback 已先 DONE): {e}")
        # T1574 B1: notify 前重读真源——update 失败两成因分流：真源已 DONE（小弟
        # 回调先到）照发 DONE 且用真源 result（顺带治降级信息）；被 CANCELLED/
        # FAILED 抢先则改发真实终态。原硬编码发 DONE=caller 收到与真源矛盾的
        # 通知（W7：cancel 与四重确认同窗交错）。
        if update_ok:
            _notify_caller("DONE", r)
        else:
            try:
                _real = task_state.load_state(task_id) or {}
                _real_status = _real.get("status", "")
                if _real_status in task_state.TERMINAL_STATES and _real_status != "DONE":
                    logger.warning(
                        f"[patrol] {task_id} update 失败且真源已 {_real_status}，"
                        f"改发真实终态（B1 防矛盾通知）"
                    )
                    _notify_caller(_real_status, _real.get("result") or r)
                else:
                    _notify_caller(
                        "DONE",
                        (_real.get("result") if _real_status == "DONE" else None) or r,
                    )
            except Exception as _re_b1:
                logger.warning(f"[patrol] {task_id} notify 前真源重读失败，兜底发 DONE: {_re_b1}")
                _notify_caller("DONE", r)

        if _dep("apply_capture_enabled")():
            try:
                _dep("_capture_apply_candidates")(task_id=task_id, agent_id=agent.name)
                ctx_file = Path(os.environ.get("NEXUS_TMP_DIR") or subpath("tmp")) / f"nexus_current_task_{agent.tmux_session}.txt"
                if ctx_file.exists():
                    ctx_file.unlink()
            except Exception as _e:
                logger.warning(f"[Task324] patrol _mark_done 钩子异常（不阻塞）: {_e}")
        return True

    def patrol_loop():
        logger.info(f"[patrol] {task_id} 监控线程启动（间隔 {PATROL_INTERVAL}s，最长存活 {timeout}s）")
        start_time = time.time()
        # T1574 B6: 墙钟锚——恒不冻结（与 start_time 的可冻结语义相对），线程
        # 真实存活时长线，超时判定前的墙顶清算用。
        wall_start = time.time()
        loop_count = 0
        # Task 280 P1-9: patrol 退出时（终态/超时/异常/stop_event）统一清理 registry
        # Task 298 P0-2: tmux pipe-pane tee 到 process/full-log.md（任何退出路径都关 pipe）
        # 用手动 __enter__/__exit__ 而非 `with` 包裹——等价于 `with _dep("PipePaneContext")(...):`
        # 但避免对 ~180 行 patrol_loop body 做整体缩进（lower risk on a large function）。
        pipe_ctx = _dep("PipePaneContext")(
            tmux_session=agent.tmux_session,
            # 1641 活①(池#102 第12例根治): 原 Path(task_path).parent 把 input/ 当
            # task_dir → full-log 错落 input/process/。改用本模块 _find_task_root
            # （L155, 回退 parent.parent 找含 output//input/ 的任务根）——968/968b/1453 实证。
            task_dir=str(_find_task_root(task_path)) if task_path else "",
        )
        pipe_ctx.__enter__()
        try:
            while not stop_event.wait(PATROL_INTERVAL):
                loop_count += 1
                elapsed = time.time() - start_time
                # 缺陷修复: 整个循环体包 try/except，防未捕获异常杀死 daemon 线程
                try:
                    logger.info(f"[patrol] {task_id} === 第{loop_count}轮 已过{elapsed:.0f}s ===")

                    # 任务已终态则退出
                    state = task_state.load_state(task_id)
                    if state and state.get("status") in task_state.TERMINAL_STATES:
                        logger.info(f"[patrol] {task_id} 已终态 {state['status']}，监控退出")
                        _notify_caller(state["status"], state.get("result"))
                        return

                    # 1. B 兜底：检测弹窗
                    try:
                        if agent.check_popup():
                            logger.info(f"[patrol] {task_id} 检测到弹窗，已发救援键")
                    except Exception as e:
                        logger.debug(f"[patrol] {task_id} check_popup 异常: {e}")

                    # 1.5 S3 (Task315): 进程死亡检测——RUNNING + session 不在 +
                    # 无 done 回调，连续 2 轮确认才合成 interrupted（防误判）。
                    # tmux 服务本身异常时保守不判（红线：非 tmux 执行体不得误判）；
                    # session 活着但静默是 stuck（走 §4 通知路径），不在此合成。
                    try:
                        if not agent.has_session() and _dep("_tmux_server_alive")():
                            cur_s3 = task_state.load_state(task_id) or {}
                            has_done_cb = any(
                                str(c.get("status", "")).lower() in ("done", "completed", "success")
                                for c in cur_s3.get("callbacks", [])
                            )
                            if has_done_cb:
                                pstate.death_rounds = 0  # 有 done 回调的矛盾态不合成（check_I10 告警域）
                            else:
                                pstate.death_rounds += 1
                                if pstate.death_rounds >= 2:
                                    logger.warning(
                                        f"[patrol] {task_id} 执行体 session 死亡（连续 {pstate.death_rounds} 轮确认，无 done 回调），合成 interrupted"
                                    )
                                    task_state.update_status(
                                        task_id, "FAILED",
                                        error=f"执行体 session 死亡（patrol 连续 {pstate.death_rounds} 轮确认，无 done 回调）",
                                        end_reason="interrupted",
                                    )
                                    _notify_caller("FAILED", {"method": "process_death"})
                                    return
                        else:
                            pstate.death_rounds = 0
                    except Exception as e:
                        logger.warning(f"[patrol] {task_id} 进程死亡检测异常（不阻塞）: {e}")
                    # Task 277: 删除 §1.5 查 19999 段（19999 半退役）。
                    # 小弟 done 回调现在走 POST :19998/callback → 直接写 task_state →
                    # 下一轮 patrol 循环 load_state 检测到 DONE 即退出（§2 输出检测也会兜底）。

                    # §1.6 池#238 重投半边（Task1703 件二）：dispatch 后 N 秒仍
                    # 无小弟回调 → 自动重投一次。回执判据=callbacks 空（⚠️不能用
                    # started is None——update_status(RUNNING) 在 send 前就盖
                    # started 时间戳，该字段是 dispatch 侧时刻非小弟回执，恒非
                    # None=死条件）。只重投一次（once_flag）；warning 落账不发
                    # alert（1589 告警半边语义不动——重投 180s 早于告警 10min，
                    # 成功则告警自然不触发，不重复报）。草稿滞留探测
                    # （_input_has_draft）证据随 warning 落账供审计。

                    try:
                        _st238 = state or {}
                        if (
                            _st238.get("status") == "RUNNING"
                            and not _st238.get("callbacks")
                            and elapsed >= DELIVERY_RESEND_SEC
                            and task_state.once_flag(task_id, "delivery_resent")
                        ):
                            _dp238 = str(_st238.get("dispatched_path") or task_path)
                            _draft238 = False
                            try:
                                _draft238 = agent._input_has_draft(_dp238)
                            except Exception:
                                pass
                            _ok238 = agent.send_task(_dp238, task_id)
                            _w238 = (
                                f"[delivery-resend] {task_id} 派发后 {DELIVERY_RESEND_SEC}s "
                                f"无小弟回调（草稿滞留={_draft238}），已自动重投一次："
                                f"{'已发出' if _ok238 else '发送失败'}（池#238 件二 Task1703）"
                            )
                            task_state.append_warning(task_id, _w238)
                            logger.warning(_w238)
                    except Exception as _e238:
                        logger.debug(f"[patrol] {task_id} 重投探测异常（不阻塞）: {_e238}")
                    # 2. 产出文件 + 空闲 → 主动标 DONE（移到超时检查前，防边界竞态）

                    try:
                        if _check_output_completion():
                            if _mark_done("output_detection"):
                                return
                            # gate 没过，不 return，继续 patrol 循环
                    except Exception as e:
                        logger.debug(f"[patrol] {task_id} 输出检测异常: {e}")

                    # Task1543 #217: suspend_until 挂起窗口——窗口期内跳过超时判定
                    # 并冻结 elapsed（每轮重置 start_time=挂起期不消耗 timeout 预算，
                    # 到点恢复=完整预算重新计）。设计内等窗口任务（如 14:00 快照窗）
                    # 不被 patrol 误杀；done 回调/输出检测/弹窗救援不受挂起影响。
                    # 双侧统一以"当前时间 vs suspend_until(epoch 秒)"为准，无清理竞态
                    # （到点后判定自动失效；busy 检查同口径跳过挂起中任务）。
                    try:
                        _susp = (state or {}).get("suspend_until")
                        if _susp:
                            try:
                                _susp_ts = float(_susp)
                            except (TypeError, ValueError):
                                _susp_ts = 0.0
                            if time.time() < _susp_ts:
                                start_time = time.time()  # 冻结 elapsed
                                logger.info(
                                    f"[patrol] {task_id} 挂起窗口内"
                                    f"（suspend_until={_susp_ts:.0f} 未到），跳过超时判定"
                                )
                    except Exception as e:
                        logger.debug(f"[patrol] {task_id} suspend 检查异常（不阻塞）: {e}")

                    # 3. 场景4: 存活超过 timeout → 二次确认窗口（Task587）
                    #    窗口内到达的真 done 直接正常终态化：小弟回调置 DONE 后
                    #    由循环顶部终态检测捕获；输出完成由本循环 §2 输出检测
                    #    命中（每轮都跑，等价旧的一次性"最终宽限检测"）。
                    #    窗口过后才标 TIMEOUT；已 TIMEOUT 的迟到 done 仍走 576
                    #    merge_late_result 补救（两条路径共存）。
                    # T1574 B6: 墙钟硬顶优先于超时判定——elapsed 可被挂起反复冻结
                    # （:631 start_time 重置），反复续期 suspend → patrol 寿命无界
                    # （registry+pipe-pane 慢性泄漏）。wall_start 恒不冻结，双线计时
                    # （elapsed=工作预算 / wall=线程寿命）。超顶强制清算，注明根因。
                    if time.time() - wall_start > SUSPEND_WALL_CAP:
                        try:
                            cur_b6 = task_state.load_state(task_id)
                            if cur_b6 and cur_b6.get("status") not in task_state.TERMINAL_STATES:
                                task_state.update_status(task_id, "TIMEOUT",
                                                         error=f"挂起总时长超墙钟硬顶 {SUSPEND_WALL_CAP}s（suspend_wall_cap_exceeded，B6；elapsed 预算 {elapsed:.0f}s 未尽属挂起冻结）",
                                                         end_reason="timeout")
                                _notify_caller("TIMEOUT", {"method": "suspend_wall_cap_exceeded"})
                        except Exception as e_b6:
                            logger.error(f"[patrol] {task_id} B6 墙顶标记失败: {e_b6}")
                        return
                    if elapsed > timeout:
                        # T1574 B2: 超时临界重读真源核 suspend_until——循环顶部
                        # state 快照（:561 load）可能早于本轮 30s 窗内写入的挂起
                        # （1543 14:00 窗任务临界轮误杀根因，W5）。未到期→冻结
                        # elapsed 本轮不判定（复用 :626-629 float 容错模式）。
                        _skip_b2 = False
                        try:
                            _cur_b2 = task_state.load_state(task_id) or {}
                            # Task1590 (P3-2): 内联 float 判定收口到 _suspend_in_window
                            # （与写前复核同源；坏值=不在窗语义保持不变）
                            if _suspend_in_window(_cur_b2):
                                start_time = time.time()  # 冻结 elapsed
                                _skip_b2 = True
                                logger.info(
                                    f"[patrol] {task_id} 超时临界重读：挂起窗口内"
                                    f"（suspend_until={float(_cur_b2.get('suspend_until')):.0f} 未到），跳过超时判定（B2）"
                                )
                        except Exception as e_b2:
                            logger.debug(f"[patrol] {task_id} B2 suspend 重读异常（不阻塞超时判定）: {e_b2}")
                        if not _skip_b2 and _timeout_grace_decision(elapsed, timeout, TIMEOUT_GRACE_SEC) == "wait":
                            logger.info(
                                f"[patrol] {task_id} 超时但处二次确认窗口"
                                f"（{elapsed:.0f}s ≤ {timeout}+{TIMEOUT_GRACE_SEC}s），继续等真 done"
                            )
                            # 不标 TIMEOUT，进入下一轮循环（顶部终态检测/§2 自然接管）
                        elif not _skip_b2:
                            # 窗口已过，确实超时，标记 TIMEOUT（_skip_b2=True 时两个
                            # 分支都不进——挂起窗口内跳过判定，落循环尾等下一轮）
                            # Task1590 (P3-2): 写前终复核——上方 B2 重读与 update_status
                            # 内部持锁之间仍能钻进新到的 suspend_until（1580 §② 毫秒级
                            # TOCTOU 残窗）。复验在窗 → 冻结 elapsed 等下一轮（B2 重读
                            # 接管），绝不让 TIMEOUT 覆盖挂起；残余窗仅剩本 load 与
                            # update_status 锁内 load 的单指令间隙（ns-µs 级，归零需把
                            # 判定下沉进 task_state 锁内——callback 热路径，本单不碰）。
                            try:
                                cur = task_state.load_state(task_id)
                                if cur and _suspend_in_window(cur):
                                    start_time = time.time()  # 冻结 elapsed
                                    logger.info(
                                        f"[patrol] {task_id} 超时写前终复核：挂起窗口内，本轮不标 TIMEOUT（P3-2）"
                                    )
                                    # 不 return——落循环尾等下一轮，B2 重读接管判定
                                elif cur is None or cur.get("status") in task_state.TERMINAL_STATES:
                                    return  # 已终态（回调抢先）或状态文件丢失，patrol 退出
                                else:
                                    task_state.update_status(task_id, "TIMEOUT",
                                                             error=f"patrol 超时 {timeout}s 无完成信号（含 {TIMEOUT_GRACE_SEC}s 二次确认窗口）",
                                                             end_reason="timeout")
                                    _notify_caller("TIMEOUT", {"method": "patrol_timeout"})
                                    return
                            except Exception as e:
                                logger.error(f"[patrol] {task_id} 超时标记失败: {e}")
                                return

                    # 4. 场景2: stuck 检测（输出无变化）→ 只通知不中断
                    try:
                        pane = agent.capture_pane(lines=10)
                        pane_hash = hash(pane)
                        if pstate.last_pane_hash == pane_hash:
                            pstate.stuck_count += 1
                        else:
                            pstate.stuck_count = 0
                            pstate.last_pane_hash = pane_hash
                        if pstate.stuck_count >= STUCK_THRESHOLD:
                            # Task 273 P1-2 修正：STUCK 通知加 5 分钟冷却
                            # 原版：连续多轮 STUCK 都通知 → 用户被刷屏（Task 273 实测：连续 49 轮报 STUCK）
                            # 现在：同 task_id 冷却期内只通知一次，冷却期后重置计数允许再通知
                            now_mono = time.monotonic()
                            if now_mono - pstate.last_stuck_notify_time >= STUCK_NOTIFY_COOLDOWN_SEC:
                                logger.warning(
                                    f"[patrol] {task_id} 可能 stuck（输出 {STUCK_THRESHOLD*PATROL_INTERVAL}s 无变化），通知 caller"
                                )
                                _notify_caller("STUCK", {"consecutive_idle_checks": pstate.stuck_count})
                                pstate.last_stuck_notify_time = now_mono
                            else:
                                # 冷却期内：只 log debug 不通知（防刷屏但保留诊断）
                                logger.debug(
                                    f"[patrol] {task_id} stuck 持续但冷却期内（剩余 {STUCK_NOTIFY_COOLDOWN_SEC - (now_mono - pstate.last_stuck_notify_time):.0f}s）"
                                )
                            pstate.stuck_count = 0
                    except Exception:
                        pass

                    # 5. Task 297 P0-1/P0-2: 执行 loop 安全控制（step cap + cost budget）
                    # 设计：抓一次 pane 同时供 step 计数和 cost 解析（一次 capture 两用）
                    # 全程 try/except——任何异常不阻塞 patrol 主流程
                    try:
                        cap_pane = agent.capture_pane(lines=50)

                        # --- P0-1: step cap（工具调用差量计数）---
                        if max_steps and max_steps > 0 and cap_pane:
                            cur_tools = set(_dep("extract_tool_calls_from_pane")(cap_pane))
                            if cur_tools:
                                # 差量：本窗口出现但上一窗口没有的 = 新增调用
                                # （小弟 pane 滚动时老调用会消失，这里只数"新增出现"避免重复）
                                delta = cur_tools - pstate.last_window_tool_calls
                                if delta:
                                    pstate.step_count += len(delta)
                                    pstate.last_window_tool_calls = cur_tools
                                    # 写到 task_state.meta（单一真相源，cancel/查询都能看）
                                    try:
                                        st = task_state.load_state(task_id)
                                        if st:
                                            meta = dict(st.get("meta") or {})
                                            meta["step_count"] = pstate.step_count
                                            meta["cost_so_far"] = round(pstate.cost_so_far, 4)
                                            task_state.update_field(task_id, "meta", meta)
                                    except Exception as _e:
                                        logger.debug(f"[patrol] {task_id} 写 step_count 失败（不阻塞）: {_e}")

                        # --- P0-2: cost budget（pane 解析累计成本，1849 背离去噪）---
                        if cost_budget_usd and cost_budget_usd > 0 and cap_pane:
                            parsed_cost = parse_cost_from_pane(cap_pane)
                            if parsed_cost is not None:
                                # fail-open: 解析失败返 None，不误判为 $0，cost_so_far 不变
                                _was_divergent = pstate.cost_divergent_count > 0
                                _r1849 = _cost_apply_reading(pstate, parsed_cost)
                                if _r1849 == "divergent":
                                    if pstate.cost_divergent_count == 1:
                                        _w1849 = (
                                            f"[cost-divergence] {task_id} 读数 ${parsed_cost:.2f} 相对可信累计 "
                                            f"${pstate.cost_so_far:.2f} 跳变背离（阈值 ×{COST_DIVERGENCE_FACTOR}"
                                            f"+${COST_DIVERGENCE_ABS_USD}），视为疑似坏读数不参与 cap 判定；"
                                            f"连续 {COST_DIVERGENCE_RISING_ROUNDS} 轮上升才升级（Task1849）"
                                        )
                                        logger.warning(_w1849)
                                        try:
                                            task_state.append_warning(task_id, _w1849)
                                        except Exception as _e1849:
                                            logger.debug(f"[patrol] {task_id} 背离告警落账失败（不阻塞）: {_e1849}")
                                elif _r1849 == "escalated":
                                    logger.warning(
                                        f"[patrol] {task_id} cost 背离读数连续上升，判定真实花费，"
                                        f"接受 ${parsed_cost:.2f} 为可信累计（Task1849）"
                                    )
                                elif _was_divergent:
                                    logger.info(
                                        f"[patrol] {task_id} cost 读数恢复正常（${parsed_cost:.2f}），"
                                        f"背离态清零（此前为疑似噪声，Task1849）"
                                    )

                        # --- 注入 reminder（step / cost 任一达 cap，且未注入过）---
                        now_mono = time.monotonic()
                        step_cap_hit = (max_steps > 0 and pstate.step_count >= max_steps)
                        cost_cap_hit = (cost_budget_usd > 0 and pstate.cost_so_far >= cost_budget_usd)

                        if step_cap_hit and pstate.step_cap_fired_at is None:
                            reminder = _dep("build_cap_reminder")(
                                f"已到 step cap ({max_steps}，当前 {pstate.step_count})"
                            )
                            if agent.send_keys([reminder]):
                                pstate.step_cap_fired_at = now_mono
                                logger.warning(
                                    f"[patrol] {task_id} ⚠️ step cap 触发 "
                                    f"({pstate.step_count}/{max_steps})，已注入 reminder，"
                                    f"宽限 {CAP_GRACE_PERIOD_SEC}s"
                                )

                        if cost_cap_hit and pstate.cost_cap_fired_at is None:
                            reminder = _dep("build_cap_reminder")(
                                f"已到 cost budget (${cost_budget_usd}，当前 ${pstate.cost_so_far:.2f})"
                            )
                            if agent.send_keys([reminder]):
                                pstate.cost_cap_fired_at = now_mono
                                logger.warning(
                                    f"[patrol] {task_id} ⚠️ cost cap 触发 "
                                    f"(${pstate.cost_so_far:.2f}/${cost_budget_usd})，已注入 reminder，"
                                    f"宽限 {CAP_GRACE_PERIOD_SEC}s"
                                )

                        # --- grace period 到期：硬杀（走现有 timeout 路径标 FAILED）---
                        # 任一 cap 触发后超过 CAP_GRACE_PERIOD_SEC 仍未 done → 强制结束
                        for cap_name, fired_at in (
                            ("step_cap", pstate.step_cap_fired_at),
                            ("cost_cap", pstate.cost_cap_fired_at),
                        ):
                            if fired_at is not None and (now_mono - fired_at) >= CAP_GRACE_PERIOD_SEC:
                                logger.error(
                                    f"[patrol] {task_id} {cap_name} 宽限到期仍无 done，强制结束"
                                )
                                try:
                                    cur = task_state.load_state(task_id)
                                    if cur and cur.get("status") not in task_state.TERMINAL_STATES:
                                        task_state.update_status(
                                            task_id, "FAILED",
                                            error=f"{cap_name} 触发后宽限 {CAP_GRACE_PERIOD_SEC}s 无 done",
                                            end_reason="cap_exceeded",
                                        )
                                        _notify_caller("FAILED", {"method": cap_name + "_force_kill"})
                                except Exception as _e:
                                    logger.error(f"[patrol] {task_id} {cap_name} 强制结束失败: {_e}")
                                return
                    except Exception as e:
                        logger.debug(f"[patrol] {task_id} Task297 cap 检测异常（不阻塞）: {e}")

                    # 诊断: 本轮无触发，记录关键状态供运维排查
                    _divergent_note = (
                        f", diverge={pstate.cost_divergent_count}轮" if pstate.cost_divergent_count else ""
                    )
                    _diag_extra = (
                        f"steps={pstate.step_count}/{max_steps if max_steps > 0 else '∞'}, "
                        f"cost=${pstate.cost_so_far:.2f}/${cost_budget_usd if cost_budget_usd > 0 else '∞'}"
                        f"{_divergent_note}"
                    )
                    logger.info(f"[patrol] {task_id} 第{loop_count}轮完成: 无触发（idle={agent.is_idle()}, elapsed={elapsed:.0f}s, {_diag_extra}）")

                except Exception as e:
                    logger.exception(f"[patrol] {task_id} 循环体异常（继续监控）: {e}")
        finally:
            # Task 280 P1-9: 无论 patrol 怎么退出（终态/超时/异常/stop_event.set），都清理 registry
            # T1574 B7 配套: 只清自己的条目——B7 set 旧线程退出时，registry 已被
            # 新 patrol 覆盖（条目=新 stop_event），无条件 pop 会误删新条目致新
            # patrol 脱管（cancel 停不到）。比较身份后再清。
            # Task1590 (P3-3): 清理同入锁；「只清自己条目」的身份比较语义不动
            # （T1574 B7 锚）。抽取为 _b7_unregister 与注册侧同锁成对。
            _b7_unregister(task_id, stop_event)
            # Task595 #45: wait=False 模式的清算点——patrol 退出=任务终态/超时，
            # 屏障 checkpoint 标 consumed（副作用的未决窗口已闭合）
            if barrier_cp_path:
                _dep("_consume_barrier_checkpoint")(task_id, barrier_cp_path)
            # Task 298 P0-2: 关 pipe-pane（任何退出路径都执行，防下一个 task log 串入）
            pipe_ctx.__exit__(None, None, None)
            # Task 301 (2026-07-21): 清理 dispatched 副本（从 dispatch() finally 挪到此）
            # 🔴 根因：dispatch() 函数 wait=False 时派完即 return → finally 立即执行 cleanup
            # 但此时任务还在小弟 tmux 里跑！AC 读副本时副本已被删（FileNotFoundError）
            # 正确位置：patrol_loop 的 finally，此时任务已终态，小弟不再读副本
            # （_cleanup_dispatched_copy 幂等：已删则 debug log + return）
            try:
                _dep("_cleanup_dispatched_copy")(task_path, task_id)
            except Exception as _e:
                logger.warning(f"[patrol] {task_id} _cleanup_dispatched_copy 入口异常（不阻塞）: {_e}")

    t = threading.Thread(target=patrol_loop, daemon=True, name=f"patrol-{task_id}")
    # Task 280 P1-9: 注册到全局 registry，让 cancel_task 能统一 stop
    # T1574 B7: 覆盖前查旧条目——恢复路径重挂与原 patrol 并存时旧 stop_event 脱管
    # （cancel 停不掉旧线程只能等超时自灭；双 patrol=双判定/双 pipe-pane tee）。
    # 异 event（真双挂）→ set 请求旧线程退出（其 finally 身份比较后幂等清理）；
    # 同 event（调用方预注册契约形态，dispatch/main.py:778 pop-then-register 及
    # 同型 helper）→ 幂等无动作，绝不 set（set 会误停自己即将跑的循环）。
    # Task1590 (P3-3): check-and-set 收进 _b7_register 持锁临界区（原无锁并发
    # 双注册都读到 None 时可双挂 patrol）；异 event 请求退出/同 event 幂等
    # 语义与 T1574 B7 保持一致。
    _b7_register(task_id, stop_event)
    t.start()
    # 修复: 线程启动后验证存活——daemon 线程启动即崩不会抛主线程
    if not t.is_alive():
        logger.error(f"[patrol] {task_id} 监控线程启动后立即死亡！检查 patrol_loop 异常")
        # Task1590 (P3-3): 原裸 pop 在「注册后瞬间被新注册覆盖」竞态下会误删
        # 新条目，改持锁身份清理（与 finally 同一路径）。
        _b7_unregister(task_id, stop_event)
    else:
        logger.debug(f"[patrol] {task_id} 监控线程已确认存活")
    return t
