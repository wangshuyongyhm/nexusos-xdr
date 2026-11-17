#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
dispatchd.py — nexusos-xdr 调度核心

常驻调度逻辑（被 mcp_server.py 的 MCP 工具和 HTTP 端点共用调用）。

核心职责: 接收一条 dispatch 命令，自动完成 11 步全流程。
- flock 分布式锁防并发竞态（v0.4 锁统一: 入口层只转发，锁在 dispatch() 函数里）
- try/finally 保证锁释放
- cancel 前 checkpoint（v0.4 ds2 修订: Ctrl+C 破坏性）

11 步流程（基于 dispatch.sh 实测 + 设计文档 §4.2）:
  0. flock 非阻塞 → 失败返 409 Conflict
  1. load agents/{to}.py
  2. is_idle 二次确认（持锁后）
  3. 生成 task_id，注册 19999
  4. ensure_no_popup（A 方案）
  5. send_task（agents 模块处理差异）
  6. 追加回调指令
  7. 启动超时计时器
  8. 启动监控线程（弹窗 patrol）
  9. 等 /report done 或超时
  10. done → 释放锁 → auto-relocate + write_knowledge
  11. 通知 caller

作者: ZCode Agent  日期: 2026-07-13
"""

import os
import re
import sys
import time
import uuid
import fcntl
import logging
import subprocess
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

# 同包 import
_SCRIPT_DIR = Path(__file__).resolve().parent
_AUTO_DIR = _SCRIPT_DIR.parent  # 自动化脚本/
# Task 282 P2-3: sys.path 初始化改用 nexus.paths_setup 单源（原 2 行 hack 删）
from nexusos_xdr import _vault_env as _ve  # Task668: 包内自足（干净环境 no-op）
_ve.setup_sys_path()

from nexusos_xdr.agents import get_agent, AGENT_REGISTRY, list_agents, list_agents_with_status
from nexusos_xdr.agents.base import AgentBase
from nexusos_xdr import task_state
from nexusos_xdr._mem_compat import scrubbed_env_or_raise  # Task668 ②/1597: 正源在伴随包 nexusos-mem；容错接入（[mem] extra）
from nexusos_xdr.guards import execute_guard_global, DestructiveOperationError

# Task 295: 接入已写好但没人调的 tools_meta / auto_approval
#   - should_auto_approve / precheck_for_agent: dispatch 时预检（不阻塞）
#   - log_tool_call: done 时记调用统计（失败不影响主流程）
# T674 ②: 改经 nexusos_xdr._vault_deps 解析——flip 后 venv 无 vault sys.path 时
# 自动按 NEXUS_VAULT_ROOT 定位 vault 单源（同机 flip 功能保留）；真干净环境降级。
try:
    from nexusos_xdr._vault_deps import (
        should_auto_approve, precheck_for_agent, log_tool_call, TOOLS_META_READY as _TASK295_APIS_READY,
    )
except Exception as _import_err:  # 优雅降级：tools_meta 缺失/PG 不可用时仍能 dispatch
    _TASK295_APIS_READY = False
    should_auto_approve = precheck_for_agent = log_tool_call = None
    import logging as _logging
    _logging.getLogger("nexusos-xdr.dispatchd").warning(
        f"[Task295] tools_meta/auto_approval import 失败，预检+统计降级关闭: {_import_err}"
    )

logger = logging.getLogger("nexusos-xdr.dispatchd")


def _scan_task_tools(task_text: str):
    """Task 295: 扫任务书提到的工具名。

    匹配两种形态：
      - MCP 调用：mcp__namespace__tool 或 mcp__namespace.tool
      - nexus 工具引用：nexusos_mem.search / nexusos_xdr.dispatch 等
    去重保序返回。
    """
    import re as _re
    if not task_text:
        return []
    seen = set()
    out = []
    # MCP 形态 mcp__a__b / mcp__a.b
    for m in _re.finditer(r"mcp__[a-zA-Z0-9_-]+(?:__|\.)[a-zA-Z0-9_.-]+", task_text):
        if m.group(0) not in seen:
            seen.add(m.group(0))
            out.append(m.group(0))
    # nexus_xxx.yyy 形态
    for m in _re.finditer(r"\bnexus_[a-z0-9]+\.[a-z0-9_.-]+", task_text):
        if m.group(0) not in seen:
            seen.add(m.group(0))
            out.append(m.group(0))
    return out


def _precheck_tools_for_dispatch(task_id: str, callee: str, task_text: str):
    """Task 295 Step 1: dispatch 预检——扫工具 + should_auto_approve + precheck_for_agent。

    纯记录，不阻塞 dispatch。失败优雅降级。结果写到 task_state.meta.precheck_notes。
    """
    if not _TASK295_APIS_READY:
        return
    try:
        tools = _scan_task_tools(task_text)
        if not tools:
            logger.info(f"[Task295][{task_id}] 任务书未提及工具，跳过预检")
            return
        notes = []
        for tool in tools:
            try:
                ok_agent, reason_agent = precheck_for_agent(tool, callee)
            except Exception as e:
                ok_agent, reason_agent = None, f"precheck_for_agent 异常: {e}"
            try:
                auto, reason_auto = should_auto_approve(tool)
            except Exception as e:
                auto, reason_auto = None, f"should_auto_approve 异常: {e}"
            notes.append({
                "tool": tool,
                "agent_ok": ok_agent,
                "agent_reason": reason_agent,
                "auto_approved": auto,
                "auto_reason": reason_auto,
            })
            # 仅当真的不通过时 warning（避免日志噪音）
            if ok_agent is False or auto is False:
                logger.warning(
                    f"[Task295][{task_id}] 预检 {tool} for {callee}: "
                    f"agent_ok={ok_agent} auto={auto} — 不阻塞，仅记录"
                )
        try:
            cur = task_state.load_state(task_id) or {}
            meta = dict(cur.get("meta") or {})
            meta["precheck_notes"] = notes
            task_state.update_field(task_id, "meta", meta)
        except Exception as e:
            logger.warning(f"[Task295][{task_id}] 写 precheck_notes 失败（不阻塞）: {e}")
        logger.info(f"[Task295][{task_id}] 预检完成: {len(tools)} 工具，已写入 meta.precheck_notes")
    except Exception as e:
        logger.warning(f"[Task295][{task_id}] 预检整体异常（不阻塞）: {e}")


def _log_tools_on_done(task_id: str, callee: str, success: bool, duration_sec):
    """Task 295 Step 2: done 时把任务用过的工具 log 到 tool_calls 表。

    从 task_state.meta.precheck_notes（Step 1 写的）提取工具列表，
    每个工具调一次 log_tool_call。失败不阻塞 done 主流程。
    """
    if not _TASK295_APIS_READY:
        return
    try:
        state = task_state.load_state(task_id) or {}
        notes = (state.get("meta") or {}).get("precheck_notes") or []
        if not notes:
            logger.info(f"[Task295][{task_id}] done 时无 precheck_notes，跳过 log_tool_call")
            return
        try:
            dur_ms = int((duration_sec or 0) * 1000)
        except Exception:
            dur_ms = 0
        ok_count = 0
        for note in notes:
            tool = note.get("tool") if isinstance(note, dict) else None
            if not tool:
                continue
            try:
                log_tool_call(tool, callee, success=success, duration_ms=dur_ms)
                ok_count += 1
            except Exception as e:
                # 单条失败不阻塞其他工具的统计
                logger.warning(f"[Task295][{task_id}] log_tool_call({tool}) 失败（不阻塞）: {e}")
        logger.info(
            f"[Task295][{task_id}] done log_tool_call: {ok_count}/{len(notes)} 工具写入 tool_calls"
        )
    except Exception as e:
        logger.warning(f"[Task295][{task_id}] _log_tools_on_done 整体异常（不阻塞）: {e}")


# ============================================================================
# Task 321 (2026-07-27): Phase 1 认知诚实——xdr 钩子自动捕获套用候选
# 设计来源：OC 316 方向 B + Phase 0（Task 319）数据基础
#   - Phase 0 已让 search 的 detail 含 experience_ids + task_id 可写入 agent_activity
#   - 本钩子在 done 时汇总该 task 期间所有 search 记录的 experience_ids，
#     写一条 action='apply_candidates' 记录，供主控补标流程（annotate_apply_attempt）读取
#   - Phase 1 不强制（只捕获候选），uncertainty 判断留给主控（DS 建议）
# ============================================================================
ENABLE_APPLY_CAPTURE = True  # Task 321: Phase 1 自动捕获开关，bug 时可关
ENABLE_PHASE2_WARN = True  # Task 329: Phase 2 WARN 模式开关（软强制，不阻塞 done），4 周 KPI 后切 BLOCK


def _capture_apply_candidates(task_id: str, agent_id: str) -> None:
    """Task 321: 汇总该 task 期间的 search 记录，识别套用候选。

    ⚠️ DEPRECATED (Task581 ④, 2026-08-19): 本写入源已被 apply_attempt（search
    annotate 实时写，Task526 落库 task_id 列）取代——消费方（Task412 通电/
    phase2_warn）均已切新源。保留写入仅为兼容 remind-annotate.sh 等旧读者，
    历史表不删；新代码禁止再读本源，改读 apply_attempt。

    套用候选 = 小弟在 task 期间 search 过的 experience_ids（去重合集）。
    写入 agent_activity(action='apply_candidates')，供主控补标时读。

    设计要点：
    - 失败绝不阻塞 done 主流程（整体 try/except，失败只 log warning）
    - target_id 用 None（task_id 是字符串如 'xdr-ac-XXX'，agent_activity.target_id
      历史是 int，混用类型有风险——task_id 放 task_id 列 + detail.agent）
    - experience_ids 在 PG jsonb 里是 list，->> 返回 text，需 json.loads 还原
    """
    try:
        from nexusos_mem.shared import SharedKnowledge
        import json as _json

        kb = SharedKnowledge(agent_id="xdr-hook")  # 标识钩子来源（非真实 agent）
        cur = kb._get_conn().cursor()

        # 查该 task 的所有 search 记录（Phase 0：search 已写 task_id + experience_ids）
        cur.execute(
            """
            SELECT detail->>'experience_ids' AS ids,
                   detail->>'query'          AS q,
                   created
              FROM agent_activity
             WHERE action = 'search' AND task_id = %s
             ORDER BY created
            """,
            (task_id,),
        )
        searches = cur.fetchall()
        cur.close()

        if not searches:
            # 该 task 没 search（纯执行任务），无套用候选，静默跳过（不写空记录）
            logger.debug(f"[Task321][{task_id}] 无 search 记录，跳过 apply_candidates 捕获")
            return

        # 汇总所有 search 的 experience_ids（去重）
        # 陷阱2：detail->>'experience_ids' 返回 text 形式 "[1, 2, 3]"，json.loads 还原
        all_ids = set()
        queries = []
        for ids_str, q, _created in searches:
            if q:
                queries.append(q)
            if not ids_str:
                continue
            try:
                ids_val = _json.loads(ids_str)  # PG jsonb ->> 后是合法 JSON 字符串
                if isinstance(ids_val, list):
                    all_ids.update(int(x) for x in ids_val if x is not None)
            except (ValueError, TypeError) as _e:
                # 单条解析失败不阻塞其他记录汇总
                logger.debug(f"[Task321][{task_id}] experience_ids 解析失败（跳过该条）: {_e}")

        # 写一条 task_apply_candidates 记录（供主控补标时读）
        kb.log_activity(
            action="apply_candidates",
            target_type="task",
            target_id=None,  # 陷阱1：target_id 历史是 int，task_id 字符串放 task_id 列
            task_id=task_id,
            detail={
                "agent": agent_id,
                "search_count": len(searches),
                "candidate_exp_ids": sorted(all_ids),  # 去重排序，便于主控读取
                "candidate_count": len(all_ids),
                "queries": queries,  # 供主控理解 search 上下文
            },
        )
        logger.info(
            f"[Task321][{task_id}] apply_candidates 捕获: "
            f"{len(searches)} 次 search → {len(all_ids)} 候选经验"
        )
    except Exception as e:
        # 陷阱3：钩子失败绝不影响 done 主流程，只 log warning
        logger.warning(f"[Task321][{task_id}] _capture_apply_candidates 异常（不阻塞）: {e}")


def _get_apply_candidates_for_task(task_id: str) -> list:
    """Task396 Step6: 拉取 task 的 apply_candidates 候选经验列表。
    来源: agent_activity action='apply_candidates', detail.candidate_exp_ids
    """
    try:
        from nexusos_mem.shared import SharedKnowledge
        import json as _json
        kb = SharedKnowledge(agent_id="xdr-hook")
        conn = kb._get_read_conn()
        cur = conn.cursor()
        cur.execute(
            "SELECT detail->'candidate_exp_ids' AS exp_ids "
            "FROM agent_activity "
            "WHERE action = 'apply_candidates' AND task_id = %s "
            "ORDER BY created DESC LIMIT 1",
            (task_id,),
        )
        row = cur.fetchone()
        cur.close()
        if row and row[0]:
            return [{"exp_id": int(eid)} for eid in row[0] if eid]
    except Exception as e:
        logger.warning(f"[Task396] _get_apply_candidates_for_task 异常: {e}")
    return []


# ============================================================================
# Task 297 P0-1/P0-2: 执行 loop 安全控制（step cap / cost budget）
# 参考 Steve Kinney "Anatomy of an Agent Loop" #1/#3
# 设计原则：纯解析函数（无 IO），失败 fail-open（解析不到返 None，不报错不误判）
# ============================================================================

# Task 297 P0-1 默认值（Steve Kinney 文章典型区间 15-25，取上限给小弟足够空间）
DEFAULT_MAX_STEPS = 25
# Task 297 P0-2 默认值（Steve Kinney 文章典型 $1-3，取 $2）
DEFAULT_COST_BUDGET_USD = 2.0
# 触发 cap 后注入 reminder，给小弟 60s 收尾窗口（Steve Kinney "early stopping"）
CAP_GRACE_PERIOD_SEC = 60

# === 工具调用识别正则（多 CLI 格式）===
# Claude Code: `● ToolName(...)` 或 `⏺ ToolName(...)`（实心圆点标记）
_TOOL_CALL_CLAUDE = re.compile(r"[●⏺]\s*([A-Za-z_][A-Za-z0-9_]*)\s*\(")
# opencode: `[tool] ToolName` 或 `[TOOL] ToolName`
_TOOL_CALL_OPENCODE = re.compile(r"\[(?:tool|TOOL)\]\s*([A-Za-z_][A-Za-z0-9_.-]+)")
# kimi/ds 等: `> ToolName(...)` 或 `→ ToolName(...)`
_TOOL_CALL_ARROW = re.compile(r"[>→]\s*([A-Za-z_][A-Za-z0-9_]*)\s*\(")
# 提取到的工具名：调用方自己决定是否新增计数（去重时窗）
_TOOL_CALL_PATTERNS = (_TOOL_CALL_CLAUDE, _TOOL_CALL_OPENCODE, _TOOL_CALL_ARROW)


def extract_tool_calls_from_pane(pane_text: str) -> List[str]:
    """Task 297 P0-1: 从 tmux pane 文本提取工具调用名（多 CLI 格式）。

    与 _scan_task_tools 的区别：
      - _scan_task_tools 扫"任务书提到的工具"（静态文档）
      - 本函数扫"实际发生的工具调用"（动态 tmux 输出）

    Args:
        pane_text: tmux capture-pane 输出（任意行数）
    Returns:
        当前 pane 内出现的工具调用名列表（按出现顺序，**不去重**——
        调用方需要差量计数，每次 patrol 抓的是"本窗口新增的调用"）
    """
    if not pane_text:
        return []
    found: List[str] = []
    for pat in _TOOL_CALL_PATTERNS:
        for m in pat.finditer(pane_text):
            name = m.group(1)
            # 过滤明显非工具的误匹配（如纯英文单词 "If" / "def"）
            if len(name) >= 2 and not name[0].islower() or name in {"If", "For", "While", "Def", "Class"}:
                # 大写开头或已知工具前缀（兜底）；纯小写单字放过让上层判断
                found.append(name)
            elif len(name) >= 3:
                found.append(name)
    return found


def build_cap_reminder(reason: str) -> str:
    """构造注入到小弟输入框的 system-reminder 文本。

    Steve Kinney "early stopping generate"：到 cap 时给一轮综合机会，不硬杀。
    """
    return (
        f"<system-reminder>{reason}。立即用现有信息收尾，"
        f"不要再调用工具。{CAP_GRACE_PERIOD_SEC}s 后仍未 done 将强制结束。</system-reminder>"
    )


# === 常量 ===
LOCK_DIR = Path("/tmp")  # flock 文件目录
# Task 277: 19999 半退役，本文件不再调用 19999。NOTIFY_BASE 仅保留给 _notify_caller
# 的 webhook 用——caller 通过 notify_url 参数传入（如 http://:19998/callback），
# 不再硬编码 19999。
DEFAULT_TIMEOUT = 3600  # 60 分钟（T674：1800 误伤 6/25 单——构建/研究型+Ask 等待常态超 30min，依实测 40-90min 区间调大，池 #23）
# Task587: TIMEOUT 二次确认窗口——超时判定后不立即终态化，窗口内等真 done
# 正常终态化（回调置 DONE），窗口过后才标 TIMEOUT（迟到 done 走 576 merge）
TIMEOUT_GRACE_SEC = 120
# P2-2 修正: 15s→5s（19999 同步写入，done 后立即可查，平均省 7.5s）
POLL_INTERVAL = 5
PATROL_INTERVAL = 30


def tmux_server_alive() -> bool:
    """tmux 服务本身是否可用（S3 Task315；T609 搭车① 公开名）。

    区分「session 真死了」和「tmux 服务挂了」：has_session() 在两者下都返回
    False（base.py except 吞错），合成 interrupted 前必须先确认服务可用，
    否则 tmux 故障会误判全部任务为 interrupted（红线：保守不合成）。

    跨模块契约: mcp_server S3 崩溃恢复路径 + patrol/loop 经 _dep 注入读取。
    并发语义: subprocess 短超时（3s），可从任意线程调用。
    幂等性: 纯探测，天然幂等。
    """
    try:
        r = subprocess.run(["tmux", "list-sessions"], capture_output=True, timeout=3,
                           env=scrubbed_env_or_raise())
        return r.returncode == 0
    except Exception:
        return False


# T628 shim 真化：wrapper → 同对象别名（import 形态）。原 wrapper 的 patch 分叉：
# patch.object(dispatchd, "_tmux_server_alive") 只对 patrol _dep() 生效，mcp_server
# 直调公开名不变——两视角行为分叉（假绿风险）。真别名后 patch 任一名全视角生效
# （_dep 属性解析 + 同对象双保险）。测试断言语义不变（is 锁定）。
_tmux_server_alive = tmux_server_alive

AUTO_RELOCATE = _ve.SCRIPTS_DIR / "auto-relocate.sh"


class DispatchError(Exception):
    """dispatch 流程中的错误。"""

    def __init__(self, message: str, status_code: int = 500):
        self.message = message
        self.status_code = status_code
        super().__init__(message)


def _snapshot_dir_md5(directory: Path) -> Dict[str, str]:
    """P1-3: 记录目录下所有文件的 md5（用于 cancel 前后对比）。"""
    import hashlib
    result = {}
    try:
        for p in directory.rglob("*"):
            if p.is_file() and p.stat().st_size < 10 * 1024 * 1024:  # 跳过 >10MB 大文件
                try:
                    h = hashlib.md5(p.read_bytes()).hexdigest()
                    result[str(p.relative_to(directory))] = h
                except Exception:
                    pass
    except Exception as e:
        logger.debug(f"_snapshot_dir_md5({directory}) 失败: {e}")
    return result


def _lock_path(callee: str) -> Path:
    """分布式锁文件路径（按 callee 分锁）。"""
    return LOCK_DIR / f"nexus-xdr-dispatch-{callee}.lock"


def _acquire_lock(callee: str) -> Optional[int]:
    """非阻塞获取 flock。返回 fd 或 None（已被锁）。"""
    lock_file = _lock_path(callee)
    fd = os.open(str(lock_file), os.O_CREAT | os.O_RDWR, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return fd
    except (IOError, OSError):
        os.close(fd)
        return None


def _release_lock(fd: int) -> None:
    """释放 flock。"""
    try:
        fcntl.flock(fd, fcntl.LOCK_UN)
    except Exception:
        pass
    finally:
        os.close(fd)


def _find_task_root(task_path: str) -> Optional[Path]:
    """找到 task 根目录（含 output/ 或 input/ 的目录）。

    task_path 通常在 input/ 子目录下:
      .../258-cognitive-honesty/input/kimi-design-review.md
    task 根目录是:
      .../258-cognitive-honesty/

    修复缺陷: 原来 Path(task_path).parent 得到 input/ 而非根目录，
    导致 output/ 检测路径错误。
    """
    if not task_path:
        return None
    p = Path(task_path).parent
    for candidate in [p, p.parent]:
        if (candidate / "output").is_dir() or (candidate / "input").is_dir():
            return candidate
    return p


# ============================================================================
# patrol 域 — Task609 (#57) 子包化迁出 → nexusos_xdr/patrol/
# 物理实现（4 函数 + 闭包群 + PatrolState 状态收敛）在 nexusos_xdr/patrol/loop.py；
# 此处保留 shim re-export（外部引用/测试 patch 语义不变——同一函数对象）。
# runtime 注入：bind_deps 在文件尾部（import patrol 后立即调用）。
# ============================================================================
from nexusos_xdr.patrol import (  # noqa: E402  Task609 shim
    PatrolState,
    _check_output_completion_triple,
    _scan_task_dir_latest_mtime,
    _timeout_grace_decision,
    _start_patrol_thread,
    parse_cost_from_pane,
    _post_webhook_with_retry,
    bind_deps as _patrol_bind_deps,
)

# Task609: patrol 子包 runtime 注入——import 后立即绑定（本模块即依赖真源；
# patrol 侧 _dep() 每次调用时经本模块属性解析，monkeypatch dispatchd 成员即时生效）
_patrol_bind_deps(sys.modules[__name__])



def _wait_for_callback(task_id: str, timeout: int) -> Dict[str, Any]:
    """等待 done 回调或超时。

    Task 277: 原"双检测"（19999 + task_state）改为单一真相源 task_state。
    小弟 done 回调走 POST :19998/callback → 直接写 task_state DONE → 本函数检测到。

    Returns:
        {"status": "DONE"|"TIMEOUT", "result": {...}}
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        # 检查 task_state（cancel / callback 都会改状态）
        state = task_state.load_state(task_id)
        if state:
            s = state.get("status")
            if s in task_state.TERMINAL_STATES:
                # Task576 例③: 带 done_late 标志返回——TIMEOUT 后真 done 迟到
                # 合并（merge_late_result）时等待者可感知 result 已补
                return {
                    "status": s,
                    "result": state.get("result"),
                    "error": state.get("error"),
                    "done_late": bool(state.get("done_late")),
                }

        time.sleep(POLL_INTERVAL)

    return {"status": "TIMEOUT"}


# ============================================================================
# Task601 (#57 批次①): 公开接口区——跨模块协作的正式契约
#
# 背景: mcp_server.py 存在 4 处对上述私有成员的运行时反向协作（依赖单向无环，
# 但调用打在私有名上=语义撒谎+巨石拆分硬障碍）。本收敛为自解释公开接口；
# 原私有名保留为同对象别名（T602 才切调用方，本批禁碰 mcp_server.py）。
#
# 🔴 别名纪律: 公开名与私有名必须绑定同一对象（容器 copy=状态分叉=静默行为
# 变化，315 测试不保证罩住）——由 test_dispatchd_public_api.py 的 `is` 断言锁定。
# ============================================================================

# --- ① auto-relocate 触发（mcp_server http_callback DONE 公共路径调用）---

def trigger_auto_relocate(task_dir: str) -> None:
    """触发 auto-relocate 外迁任务产出大文件（done 清算的公共步骤）。

    跨模块契约（谁调/何时调）:
        mcp_server.http_callback —— DONE 公共主路径（Task496：覆盖 wait 两模式
        + 小弟主动回调）在 update_status 成功后调用；dispatchd patrol/_wait 路径
        经 `_trigger_auto_relocate` 同对象别名走同一逻辑。

    并发语义:
        可从任意线程调用（FastAPI worker 线程 / patrol daemon 线程并发到达）。
        内部 subprocess.run 超时 60s 阻塞调用方线程——调用方应在线程池上下文
        （anyio.to_thread）中调用，勿在事件循环直调。

    幂等性:
        非幂等但有界——auto-relocate.sh 自身按文件大小阈值迁移，重复调用对已
        迁移文件是 no-op；失败只 log 不抛（fail-open，绝不阻塞 done 清算）。

    Args:
        task_dir: 任务根目录（含 output/ 的目录，auto-relocate.sh 的作用对象）
    """
    _trigger_auto_relocate_impl(task_dir)


# --- ② patrol registry（mcp_server 服务重启恢复路径读取）---

# Task 280 P1-9: 全局 patrol 线程 registry（task_id -> stop_event）
# 让 cancel_task / shutdown 能统一 stop patrol，避免孤儿线程空转到 timeout。
# 🔴 Task609 事故复盘恢复：本定义曾因删除 _post_webhook_with_retry 块时结束
# 谓词漏匹配本行（不以 def/class 开头）被误删 → 生产 xdr 崩溃循环 6 次
# （NameError: _active_patrols）。真源在此（patrol/loop 经 _dep() 解析）。
_active_patrols: Dict[str, "threading.Event"] = {}


def get_active_patrols() -> Dict[str, "threading.Event"]:
    """返回 patrol 线程 registry 的**同对象引用**（task_id → stop_event）。

    跨模块契约:
        mcp_server 服务重启恢复逻辑（Task 280 P1-14）——重挂任务前检查
        `tid in get_active_patrols()` 判断 patrol 是否仍在监控，避免双 patrol。

    并发语义:
        🔴 返回的是共享可变 dict 本体（非 copy）——读侧（`in` 判定/遍历）线程
        安全由 GIL 单操作保证；**写侧（增删 key）只允许 dispatchd 内部**
        （_start_patrol_thread 注册 / patrol_loop finally 清理），外部写=契约
        违约。copy 一份返回会造状态分叉（判定失效→双 patrol），故同对象。

    幂等性:
        纯读访问，天然幂等。
    """
    return _active_patrols


# --- ③ patrol 启动（mcp_server 服务重启恢复路径调用）---

def start_patrol_thread(agent: "AgentBase", task_id: str,
                        stop_event: "threading.Event",
                        task_path: str = "", timeout: int = DEFAULT_TIMEOUT,
                        notify_url: str = "",
                        max_steps: int = DEFAULT_MAX_STEPS,
                        cost_budget_usd: float = DEFAULT_COST_BUDGET_USD,
                        barrier_cp_path: str = "") -> None:
    """启动 patrol 监控 daemon 线程（dispatch 主流程与崩溃恢复共用入口）。

    跨模块契约:
        dispatchd.dispatch() —— 正常派发路径；mcp_server 服务重启恢复
        （Task 280 P1-14）—— 找到 RUNNING 但 patrol 已丢的任务重挂监控。

    并发语义:
        每次调用新建一个 daemon 线程并注册到 registry（task_id → stop_event）。
        🔴 非幂等——对**同一 task_id 重复调用会造双 patrol**（双份 pipe-pane/
        双份终态迁移竞争）：调用方（含 mcp_server 恢复路径）必须先查
        `task_id in get_active_patrols()` 再调本接口。import 期零副作用
        （不自动启动任何线程）。

    幂等性:
        见上——调用方负责防重。

    Args:
        agent: 目标 Agent 适配器（patrol 用它读 pane/idle 状态）
        task_id: 任务 ID（registry key）
        stop_event: 停止信号（cancel_task 统一 stop 用）
        task_path: 任务书路径（pipe-pane 落点推导）
        timeout: patrol 最长存活秒数（防泄漏）
        notify_url: 终态 webhook（场景5）
        max_steps / cost_budget_usd: Task 297 loop 安全控制
        barrier_cp_path: Task595 屏障 checkpoint 路径（终态 finally 清算用）
    """
    _start_patrol_thread(agent, task_id, stop_event,
                         task_path=task_path, timeout=timeout,
                         notify_url=notify_url, max_steps=max_steps,
                         cost_budget_usd=cost_budget_usd,
                         barrier_cp_path=barrier_cp_path)


# --- ④ 行为开关（mcp_server 钩子读取 + 测试 monkeypatch 目标）---
# 🔴 设计裁决（Task601 对抗式审查结论）：bool 不可变，常量别名（公开名=老名值
# 拷贝）在 monkeypatch 后必分叉（patch 老名→公开名仍旧值=假绿）；模块级
# __setattr__ 钩子不被 Python 模块类型调用（PEP 562 仅 __getattr__），拦截写
# 不可行。故开关的公开契约是 **getter 函数**——单一读取路径，读模块当前值：
# 测试 patch 老名（ENABLE_APPLY_CAPTURE=False）→ getter 立即反映；
# patch getter 本身同样生效。函数类"同对象"的等价口径=可调用等价（任务书
# 验收 4 预留），常量形态不提供（防分叉诱惑）。

def apply_capture_enabled() -> bool:
    """认知诚实 Phase 1（apply_candidates 自动捕获）开关的读取接口。

    跨模块契约:
        mcp_server http_callback / mcp 工具 done 钩子（Task326 双保险）与
        patrol _mark_done 读取；T602 切换后统一走本接口。

    并发语义:
        纯读 bool（GIL 原子）；monkeypatch（pytest）改模块属性即时生效——
        老路径 `monkeypatch.setattr(dispatchd, "ENABLE_APPLY_CAPTURE", False)`
        与 patch 本函数等效（单一真源=模块属性，无别名分叉面）。

    幂等性:
        纯读，天然幂等。
    """
    return bool(globals()["ENABLE_APPLY_CAPTURE"])


def phase2_warn_enabled() -> bool:
    """认知诚实 Phase 2 WARN 模式开关的读取接口。

    跨模块契约:
        mcp_server http_callback Phase2 检查段（有候选未补标 → WARN+记录，
        软强制不阻塞 done；4 周 KPI 后评估切 BLOCK）。

    并发语义 / 幂等性:
        同 apply_capture_enabled（纯读模块属性，patch 即时生效）。
    """
    return bool(globals()["ENABLE_PHASE2_WARN"])


def _trigger_auto_relocate_impl(task_dir: str) -> None:
    """trigger_auto_relocate 的实现体（原 _trigger_auto_relocate 改名承接）。"""
    if not AUTO_RELOCATE.exists():
        logger.debug(f"auto-relocate.sh 不存在，跳过: {AUTO_RELOCATE}")
        return
    try:
        _r = subprocess.run(
            ["bash", str(AUTO_RELOCATE), task_dir],
            timeout=60, capture_output=True, text=True,
            env=scrubbed_env_or_raise(),
        )
        if _r.returncode != 0:
            logger.warning(f"auto-relocate 非0退出 rc={_r.returncode}: {_r.stderr[:200]} | dir={task_dir}")
        else:
            logger.info(f"auto-relocate 完成: {task_dir} | {_r.stdout[:120]}")
    except Exception as e:
        logger.warning(f"auto-relocate 异常（不阻塞）: {e} | dir={task_dir}")

# T628 shim 真化：wrapper → 同对象别名。原 wrapper 调 impl 的分叉：patch 公开名
# trigger_auto_relocate 时 patrol/_dep 与 callback（都解析公开名）生效，但 patch 私有
# 名 _trigger_auto_relocate 时无人可见（死名）——注释宣称「同对象别名」名不符实。
# 真别名后两名为同一函数对象，patch 任一名全生效。
_trigger_auto_relocate = trigger_auto_relocate


def _cleanup_dispatched_copy(task_path: str, task_id: str) -> None:
    """Task 298 P0-1: 清理 dispatched 副本（任何 final_status 都要清）。

    🔴 根因（诊断）：原版只在 final_status=="DONE" 时调 _trigger_auto_relocate，
    TIMEOUT/FAILED/CANCELLED 永远不清副本 → 1 天内堆积 12 个 task.dispatched.*.md。

    修复：本函数在 dispatch() finally 块里调用，保证所有终态（含异常退出）都清。

    Args:
        task_path: 原任务书路径（副本与其同目录，命名 task.dispatched.{task_id}.md）
        task_id: 任务 ID（副本文件名含 task_id）
    """
    try:
        parent = Path(task_path).parent
        # 副本命名约定（见 dispatch() L901）：task.dispatched.<task_id>.md
        dispatched_path = parent / f"task.dispatched.{task_id}.md"
        if not dispatched_path.exists():
            logger.debug(f"[dispatch] {task_id} 无 dispatched 副本（无需清理）: {dispatched_path}")
            return
        dispatched_path.unlink()
        logger.info(f"[dispatch] {task_id} 已清理 dispatched 副本: {dispatched_path.name}")
    except Exception as e:
        # 清副本失败不能阻塞 dispatch 返回（任务本身可能已成功）
        logger.warning(f"[dispatch] {task_id} 清理 dispatched 副本失败（不阻塞）: {e}")


# ============================================================================
# Task 298 P0-2: tmux pipe-pane 接入（process/full-log.md 自动生成）
# ============================================================================
class PipePaneContext:
    """Task 298 P0-2: tmux pipe-pane 上下文管理器。

    🔴 根因（诊断）：dispatchd 完全没 pipe-pane 配置（grep 命中 0 处），
    process/full-log.md 全靠小弟手工写 → 89% 违反铁律。

    修复：dispatch 启动 patrol 时 `with PipePaneContext(...)`，启动 pipe-pane
    把 tmux pane 输出实时 tee 到 process/full-log.md；patrol 退出时（任何路径）
    关 pipe，防止下一个 task 的 log 串到上一个文件。

    设计：
    - `__enter__`: tmux pipe-pane -t <session> -o 'cat >> <path>'
    - `__exit__`:  tmux pipe-pane -t <session> ''（空命令=关闭 pipe）
    - fail-open: 任何 tmux 调用失败只 log warning，不阻塞 dispatch
    - 可注入 tmux_runner 用于测试（默认走真实 subprocess）

    用法（patrol 线程内）：
        with PipePaneContext(tmux_session='agent-a', task_dir='/path/to/task-XXX/'):
            # patrol loop ...
    """

    def __init__(self, tmux_session: str, task_dir: str,
                 *, filename: str = "full-log.md",
                 tmux_runner=None):
        self.tmux_session = tmux_session
        self.task_dir = task_dir
        self.filename = filename
        # tmux_runner(args: List[str]) -> int（返 returncode）；默认走真实 subprocess
        self._tmux = tmux_runner or self._default_tmux
        self.log_path: Optional[Path] = None
        self._pipe_started = False

    @staticmethod
    def _default_tmux(args: List[str]) -> int:
        """默认 tmux 执行器——真实 subprocess，fail-open。"""
        try:
            r = subprocess.run(["tmux"] + args, capture_output=True, timeout=5,
                               env=scrubbed_env_or_raise())
            return r.returncode
        except Exception as e:
            logger.warning(f"[pipe-pane] tmux 调用异常（fail-open）: {e}")
            return -1

    # Task 300: ANSI strip 命令模板（perl 实时过滤，比 sed 鲁棒——处理 CSI/OSC/cursor 序列）
    # 三类 ANSI 序列：
    #   1. CSI: ESC [ ... letter（SGR 颜色 / 光标移动 / 清屏，最常见）
    #   2. OSC: ESC ] ... BEL 或 ST（窗口标题 / 终端查询应答）
    #   3. 其他: ESC = / ESC >（键盘模式）等单字符 ESC 序列
    # Task 495: 追加单行截断 + CR→换行——tmux spinner 帧（⠦..\r⠧..\r⠇..\r）
    #   的 \r 不被 \n 切分，显现为 13K-3.2M 病态长行，撑爆 Obsidian Renderer
    #   V8 字符串操作 → SIGTRAP / IndexedDB 损坏（08120840 事故）。
    # 🔴 顺序铁律（Task 495 Level 3 集成验证）：line cap 必须在 \r→\n 之前。
    #   PTY 输出 \r\n，若先 s/\r/\n/g 会在 $_ 内部注入 \n，而 s/^(.{2000}).*$/$1/
    #   无 /m 修饰时 $ 只匹配串尾/尾随换行——内部 \n 致 $ 定位失败，cap 失效（实测
    #   5000 字符行未被截断）。cap 先行时 $_ 仍是原始单行（\r 是普通字符，. 匹配），
    #   $ 正确定位尾随 \n，截断生效后再 \r→\n 拆帧。
    # perl -pe 逐行处理；pipe-pane 是流式输出，perl 实时 strip 比 post-process 更可靠
    #
    # 🔴 转义注意（Task 300 踩坑）：raw string r"\e\[" 是 4 字符（\ e \ [），
    # shell 原样传给 perl，perl 解释为 "ESC + 字面["。**不要写 r"\e\\["**——那会变成
    # "ESC + 字面\ + 字面["，不匹配真实 ANSI 序列。
    _ANSI_STRIP_CMD = (
        r"perl -pe '"
        r"s/\e\[[0-9;?]*[a-zA-Z]//g;"       # CSI 序列（含 ? 私有序列）
        r"s/\e\][0-9];.*?\a//g;"            # OSC 序列（ESC ] 0;title BEL）
        r"s/\e[=>]//g;"                     # 单字符 ESC 序列（ESC = / ESC >）
        r"s/\e\[[0-9;]*m//g;"               # SGR 颜色序列（兜底）
        r"s/^(.{2000}).*$/$1/;"             # 单行硬截断 2000 字符（必须在 \r→\n 前：PTY \r\n 环境下先 \r→\n 会注入内部 \n 致 $ 定位失败）
        r"s/\r/\n/g;"                       # CR→换行：拆 spinner 帧拼接（治病态长行根因；08120840 Obsidian 崩溃事故）
        r"'"
    )

    def _build_pipe_command(self) -> str:
        """Task 300: 构造 pipe-pane -o 后接的 shell 命令（含 ANSI strip）。

        设计：`{strip_cmd} >> '{log_path}'`
          - strip_cmd: perl 实时过滤 ANSI 序列（保留可读文本）
          - >> log_path: 追加到 full-log.md
          - 单引号包裹 log_path 防路径含空格/特殊字符

        可注入：测试可覆盖此方法验证命令构造。
        """
        return f"{self._ANSI_STRIP_CMD} >> '{self.log_path}'"

    def __enter__(self) -> "PipePaneContext":
        # process/ 目录不存在要 mkdir
        try:
            proc_dir = Path(self.task_dir) / "process"
            proc_dir.mkdir(parents=True, exist_ok=True)
            self.log_path = proc_dir / self.filename
            # Task 300: 把裸 cat 换成 perl strip 命令（实时过滤 ANSI）
            # 原: tmux pipe-pane -t <session> -o 'cat >> <path>'
            # 现: tmux pipe-pane -t <session> -o 'perl -pe ... >> <path>'
            cat_cmd = self._build_pipe_command()
            rc = self._tmux(["pipe-pane", "-t", f"={self.tmux_session}:", "-o", cat_cmd])
            if rc == 0:
                self._pipe_started = True
                logger.info(f"[pipe-pane] {self.tmux_session} → {self.log_path.name} 启动（含 ANSI strip）")
            else:
                logger.warning(
                    f"[pipe-pane] {self.tmux_session} 启动失败 rc={rc}（fail-open，继续 dispatch）"
                )
        except Exception as e:
            logger.warning(f"[pipe-pane] __enter__ 异常（fail-open）: {e}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> False:
        # 关 pipe-pane（任何 patrol 退出路径都执行：正常/异常/stop_event）
        if self._pipe_started:
            try:
                # tmux pipe-pane -t <session> '' （空命令=关闭）
                self._tmux(["pipe-pane", "-t", f"={self.tmux_session}:", ""])
                logger.info(f"[pipe-pane] {self.tmux_session} 已关闭")
            except Exception as e:
                logger.warning(f"[pipe-pane] {self.tmux_session} 关闭失败（不阻塞）: {e}")
            finally:
                self._pipe_started = False
        return False  # 不吞异常，让外层处理


# ============================================================================
# dispatch 主流程域 — Task621 (#57 收官批) 子包化迁出 → nexusos_xdr/dispatch/
# 物理实现（dispatch 主函数 + check_status/cancel_task/request_approval/
# list_supported_agents + 屏障三件套）在 nexusos_xdr/dispatch/main.py；
# 此处 shim re-export + bind_deps（import 后立即调用）。
# ============================================================================
from nexusos_xdr.dispatch import (  # noqa: E402  Task621 shim
    _loop_goal_context,
    _write_dispatch_barrier_checkpoint,
    _consume_barrier_checkpoint,
    dispatch as _dispatch_impl,
    check_status as _check_status_impl,
    cancel_task as _cancel_task_impl,
    request_approval as _request_approval_impl,
    list_supported_agents as _list_supported_agents_impl,
    bind_deps as _dispatch_bind_deps,
)

# Task621: bind 顺序——dispatch 依赖本模块成员，先绑定再用
_dispatch_bind_deps(__import__(sys.modules[__name__.__package__].__name__, fromlist=["_"]) if False else sys.modules[__name__])

# 公开 API（原 dispatchd.dispatch 调用方零变化；老名保持）
dispatch = _dispatch_impl
check_status = _check_status_impl
cancel_task = _cancel_task_impl
request_approval = _request_approval_impl
list_supported_agents = _list_supported_agents_impl
