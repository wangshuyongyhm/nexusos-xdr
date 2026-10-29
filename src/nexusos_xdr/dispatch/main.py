#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""nexusos_xdr.dispatch.main — dispatch 主流程域（Task621 #57 收官批，自 dispatchd.py 迁入）。

物理迁入（行为零变化——T609 纪律）：dispatch 主函数（全体系心脏）+ check_status
/ cancel_task / request_approval / list_supported_agents。

架构：bind_deps 注入（T609 模式——禁 import dispatchd 防循环）；patrol 正向
import；flock/锁语义零变化（陷阱 2）。段内函数定义（屏障三件套/goal 上下文）
随段迁入为本模块顶层函数。
"""
import json
import logging
import os
import re
import subprocess
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any, Dict, Optional

from nexusos_xdr import task_state
from nexusos_common import subpath  # 1960：锚点单源（design §2.1 行10）
from nexusos_xdr._mem_compat import scrubbed_env_or_raise  # Task627/1597: cancel 快照 md5 用；容错接入（[mem] extra，缺包时报安装指引）
from nexusos_xdr.guards import execute_guard_global, DestructiveOperationError  # Task627: request_approval 守卫（叶子模块无环）
from nexusos_xdr.patrol import _start_patrol_thread as start_patrol_thread

logger = logging.getLogger("nexusos-xdr.dispatchd")  # 保持宿主 logger 名（观测兼容）

_deps = None


def _atomic_write_text(path: Path, text: str) -> None:
    """原子写任务书副本（Task1594 池#234 批次2 D1）。

    原 write_text 是 truncate-rewrite——写窗内小弟读副本 / 注入流程二次读改写
    会撞见半截内容（1563 C3+ 撕裂同机制，副本含中文任务书是撕裂敏感形态）。
    同目录 tmp + fsync + os.replace：tmp 与目标同目录=同卷结构保证，rename
    原子让半截对读者不可见（1563 C4 双盘 100 轮 100% 可靠）。
    失败清理 tmp 后重抛——副本写失败走调用方既有 fallback（原路径继续派发）。
    """
    _fd, _tmp = tempfile.mkstemp(dir=str(path.parent), prefix=".tmp-aw-", suffix=".md")
    try:
        with os.fdopen(_fd, "w", encoding="utf-8") as _f:
            _f.write(text)
            _f.flush()
            os.fsync(_f.fileno())
        os.replace(_tmp, str(path))
    except Exception:
        try:
            os.unlink(_tmp)
        except OSError:
            pass
        raise

# Task639 🟡8: 副本注入模板抽常量（原两处 f-string 嵌套 + {{}} 转义，脆弱难读；
# 常量 + .format 单层替换，占位符见字即得）。to=目标小弟，cred=search 凭据 ID。
_SEARCH_CMD_BLOCK_TMPL = """

## 🔴 NEXUS 强制：执行前必查 MEM（带凭据，直接复制运行）

> **遇事第一动作：搜 MEM 知识库**（B4 禁凭记忆干）。以下命令已带你的凭据，直接复制改关键词即可：

```bash
# 搜 MEM（HTTP 200 = 凭据有效）
curl -s -H "X-Credential: {cred}" \\
  "http://127.0.0.1:11537/search?q=你的关键词&top_k=5&agent_id={to}"
```

**认知诚实**：搜到的经验如果套用了，自动记录到闭环（带 agent_id 触发 annotate）。
**done 前**：把高价值发现写回 MEM：
```bash
curl -s -X POST http://127.0.0.1:11537/knowledge -H "Content-Type: application/json" -d '{{"agent_id":"{to}","title":"标题","content":"内容","knowledge_type":"experience","tags":["关键词"]}}'
```
"""

_SEARCH_CMD_BLOCK_ANON = """

## ⚠️ NEXUS 提示：search_cred 未签发（fail-open 降级）

> search 凭据未签发，请用 anonymous 模式查 MEM（不记认知闭环）：
> `curl -s "http://127.0.0.1:11537/search?q=关键词&top_k=5"`
"""


def bind_deps(dispatchd_module) -> None:
    """绑定 dispatchd 域依赖（锁/错误/预检/清理/wait/_capture/常量）。

    契约：dispatchd import 本包后立即调用（幂等覆盖）；未绑定 fail-fast。
    """
    global _deps
    _deps = dispatchd_module


def _dep(name: str):
    if _deps is None:
        raise RuntimeError("nexusos_xdr.dispatch 未 bind_deps——dispatchd import 后需立即调用")
    return getattr(_deps, name)


# === Task595 #45: dispatch 副作用前 checkpoint 屏障 ===
# send_keys 投递不可逆——投递前强制留 checkpoint（崩溃后可精确恢复「派了没派」）。
# goal 来源：LOOP 会话上下文（环境变量 NEXUS_LOOP_GOAL / /tmp/nexus_loop_goal.txt），
# 非 LOOP 场景（交互式单派）无 goal——用 task_path 摘要做身份（屏障仍生效）。

def _loop_goal_context() -> str:
    """读当前 LOOP goal（env → /tmp 文件 → 空）。读不到不阻塞（降级 task_path 身份）。"""
    goal = os.environ.get("NEXUS_LOOP_GOAL", "")
    if not goal:
        try:
            gp = Path(os.environ.get("NEXUS_TMP_DIR") or subpath("tmp")) / "nexus_loop_goal.txt"
            if gp.exists():
                goal = gp.read_text(encoding="utf-8").strip()
        except Exception:
            goal = ""
    return goal


def _write_dispatch_barrier_checkpoint(task_id: str, task_path: str, caller: str, to: str) -> str | None:
    """send_task 前写屏障 checkpoint。失败重试 1 次仍失败 → fail-closed _dep('DispatchError')。

    1976 O1：`nexus.checkpoint` 属 vault 侧私包（OSS/fresh 环境无此物）——缺包时
    默认宽松降级（WARN + 跳过屏障，返 None）；NEXUS_STRICT_CHECKPOINT=1 恢复
    fail-closed（严格语义下缺包即拒派）。vault 生产侧 import 恒成功，行为零变化。

    Returns: checkpoint 文件路径；宽松降级时 None
    Raises: _dep('DispatchError')（503 CHECKPOINT_BARRIER_FAILED——canonical 错误码）
    """
    try:
        from nexus.checkpoint import write_checkpoint, goal_revision_hash  # noqa: F401
    except ImportError as e:
        if os.environ.get("NEXUS_STRICT_CHECKPOINT", "").strip().lower() in ("1", "true", "yes"):
            raise _dep('DispatchError')(
                f"CHECKPOINT_BARRIER_FAILED: strict 模式缺 nexus.checkpoint（{e}）",
                status_code=503,
            )
        logger.warning(
            f"[Task595][{task_id}] nexus.checkpoint 不可用（非 vault 环境）——"
            f"屏障降级跳过（设 NEXUS_STRICT_CHECKPOINT=1 可恢复严格拒派）"
        )
        return None
    goal = _loop_goal_context() or f"single-dispatch:{Path(task_path).parent.parent.name}"
    for attempt in (1, 2):
        try:
            cp_path = write_checkpoint(
                state="S5",
                session_id=f"dispatch-barrier-{task_id}",
                goal=goal,
                queue=[f"dispatched:{task_id}→{to}（未回调=已派未清）"],
                # Task1682-d13: 屏障场景的两层补全——在途=本次投递，待决=等回调
                inflight_summary=f"dispatch {task_id} → {to} 投递中（caller={caller}）",
                pending_events=[f"await-callback:{task_id}"],
                recovery_guide=(
                    f"屏障 checkpoint（Task595 #45）：task_id={task_id} caller={caller} → {to}\\n"
                    f"若该 task 在 task_state 无终态=派发后崩溃，恢复动作：\\n"
                    f"1. GET /task/{task_id} 查状态；无记录=send_keys 前崩溃（未派，安全重派）\\n"
                    f"2. RUNNING 但 pane 已死=派了未执行（cancel 后重派）\\n"
                    f"3. 终态但未验收=仅补验收，勿重派"
                ),
            )
            logger.info(f"[Task595][{task_id}] 屏障 checkpoint 已写: {cp_path}")
            return cp_path
        except Exception as e:
            if attempt == 1:
                logger.warning(f"[Task595][{task_id}] checkpoint 写入失败（重试 1 次）: {e}")
                continue
            # canonical 错误码，不静默降级（fail-closed）
            raise _dep('DispatchError')(
                f"CHECKPOINT_BARRIER_FAILED: 副作用前 checkpoint 写入两次失败，拒绝派发 {task_id}（{e}）",
                status_code=503,
            )
    # 不可达（循环内必 return 或 raise）
    raise _dep('DispatchError')("CHECKPOINT_BARRIER_FAILED: unreachable", status_code=503)


def _consume_barrier_checkpoint(task_id: str, barrier_cp_path: str) -> None:
    """Task595 #45: dispatch 清算（终态后）后把屏障 checkpoint 标 consumed。

    实现：terminal 路径重写 consumed 标记（checkpoint 文件为不可变 append 流，
    在同目录写 consumed 标记文件 <name>.consumed——恢复侧 read_latest 后查伴生标记）。
    """
    try:
        p = Path(barrier_cp_path)
        marker = p.with_name(p.name + ".consumed")
        marker.write_text(f"task_id={task_id} consumed_at={time.strftime('%Y-%m-%dT%H:%M:%S')}\n",
                          encoding="utf-8")
        logger.info(f"[Task595][{task_id}] 屏障 checkpoint 已标 consumed: {marker.name}")
    except Exception as e:
        logger.warning(f"[Task595][{task_id}] 标 consumed 失败（不阻塞清算）: {e}")


def _presence_autoregister(agent_name: str, task_id: str, task_path: str = "") -> None:
    """投递成功后：服务端代注册目标 Agent 到 presence（池 #74 治本，Task637）。

    设计:
      - 直调 session_state.register_agent（同进程同锁，不走 HTTP 自环）
      - fail-open: 任何异常只 log warning，绝不阻断投递主流程
      - 幂等: register_agent 全量覆盖写（小弟后续自觉 init 自然覆盖，语义正确）
      - session_id 用 task_id（xdr-<agent>-<ts>-<hex> 自带归属维度；presence 的
        op-log 冲突检测键是 (agent,target)，session_id 仅事件归属维度——不污染）
      - 事件 type=auto_register（Task543 router_register 先例：专用 type 防
        gate_grace 观测器把代注册当 session_register 信号→假报 init gate 漏调）
    """
    try:
        from nexusos_xdr import session_state as _ss
        # 简述取任务目录名（task/637-xxx/input/task.md → 637-xxx），取不到退 task_id
        _short = task_id
        if task_path:
            try:
                _short = Path(task_path).parent.parent.name
            except Exception:
                pass
        _ss.register_agent(
            agent=agent_name,
            role="executor",
            session_id=task_id,
            current_task=f"{_short}（dispatch 代注册）",
            status="working",
        )
        _ss.append_event(
            agent_name, "auto_register",
            detail=f"dispatch 代注册: task={task_id}", target=f"task:{task_id}",
        )
    except Exception as _e:
        logger.warning(f"[{agent_name}] presence 代注册失败（不阻断投递）: {_e}")



def _nn_clash_warning(task_path: str) -> "str | None":
    """Task991: 任务编号撞号检测（不拦只提示）——task/ 下同 NN 前缀不同目录名
    （09010832 实锤：两个 989-* 并存=两主控编号空间未协调）。只 ls 目录名
    一层不递归；task 目录从 task_path 上两级推导（勿硬编码路径）。"""
    try:
        task_dir = Path(task_path).resolve().parent.parent
        nn = task_dir.name.split("-", 1)[0]
        if not nn.isdigit():
            return None
        siblings = sorted(d.name for d in task_dir.parent.iterdir()
                          if d.is_dir() and d.name != task_dir.name
                          and d.name.split("-", 1)[0] == nn)
        if siblings:
            return (f"任务编号撞号：{task_dir.name} 与 {', '.join(siblings)} "
                    f"同为 NN={nn}（多主控编号空间未协调，注意任务书引用歧义）")
    except Exception:
        pass
    return None


def _parse_suspend_until(val) -> Optional[float]:
    """Task1543: suspend_until 归一化——epoch 秒（int/float/数字串）或 ISO 8601（含 Z/+08:00）
    → UTC epoch 秒 float。None/非法 → None（不挂起，行为与旧版完全一致）。
    陷阱1 对策：统一 UTC epoch 秒内部表示，ISO 仅入口兼容。"""
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        pass
    try:
        from datetime import datetime, timezone
        s = str(val).strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except Exception:
        return None


def _read_task_budget_usd(task_path: str) -> Optional[float]:
    """Task 1849: 从任务书 frontmatter 读 budget_usd 声明（无/坏值返 None）。

    背景：patrol cost 帽默认 $2.0（DEFAULT_COST_BUDGET_USD），任务书声明的
    预算顶（如 1842/1847 的「15 元」≈$2.1）此前没有通道传入 patrol——
    超帽判定与声明预算脱节。主控在任务书 frontmatter 写 budget_usd: <USD>
    即生效；不写保持调用方默认。坏值（非数字/<=0）fail-open 返 None 走
    默认帽，不阻塞 dispatch。
    """
    try:
        with open(task_path, "r", encoding="utf-8") as f:
            head = [next(f, "") for _ in range(30)]
        if not head or head[0].strip() != "---":
            return None
        for line in head[1:]:
            s = line.strip()
            if s == "---":
                break
            if s.startswith("budget_usd:"):
                val = float(s.split(":", 1)[1].strip())
                return val if val > 0 else None
    except Exception:
        return None
    return None


def dispatch(
    to: str,
    task_path: str,
    caller: str,
    timeout: int = 3600,   # DEFAULT_TIMEOUT 同值（import 期求值；T674 1800→3600）
    wait: bool = True,
    notify_url: str = "",
    max_steps: int = 25,   # DEFAULT_MAX_STEPS 同值（import 期求值；dispatchd 常量真源）
    cost_budget_usd: float = 2.0,   # DEFAULT_COST_BUDGET_USD 同值
    force: bool = False,   # Task991: 越过忙碌硬检查（用户显式并派场景）
    suspend_until: Optional[float] = None,   # Task1543 #217: 挂起窗口（epoch 秒；期内 patrol 跳过超时判定）
) -> Dict[str, Any]:
    """派任务给另一个 Agent。11 步完整流程。

    这是唯一的 dispatch 入口（v0.4 锁统一）。
    MCP 工具和 HTTP 端点都调这个函数，锁在这里实现一次。

    Args:
        to: 目标 Agent 名（oc/omp/kimi/ds/ds2/ac）
        task_path: 任务书文件绝对路径
        caller: 调用方 Agent 名（审批归属）
        timeout: 超时秒数
        wait: True=同步等待完成，False=只派发立即返回 task_id
        max_steps: Task 297 P0-1 执行 loop step cap（默认 25）。
            patrol 检测到工具调用达到此数 → 注入 reminder 让小弟收尾，
            60s 后仍未 done 才走现有 timeout 逻辑。<=0 表示不限制。
        cost_budget_usd: Task 297 P0-2 cost budget（默认 $2.0）。
            patrol 解析 tmux 输出累计成本，达到此值注入 reminder。
            <=0 表示不限制。
    Returns:
        {"task_id": "...", "status": "...", ...}
    Raises:
        _dep('DispatchError'): 流程错误（404 未知 Agent / 409 冲突 / 等）
    """
    # === Step 1: 加载 agent 模块 ===
    agent = _dep('get_agent')(to)
    if agent is None:
        # 区分"未知 Agent" vs "退役 Agent" 给主控更准确报错
        all_statuses = _dep('list_agents_with_status')()
        if to in all_statuses:
            raise _dep('DispatchError')(
                f"Agent '{to}' 状态={all_statuses[to]}（非 active，不能被 dispatch）。"
                f"如需启用，改 nexusos_xdr/agents/__init__.py 的 status 为 active。"
                f"当前 active agents: {_dep('list_agents')()}",
                status_code=404,
            )
        raise _dep('DispatchError')(
            f"未知 Agent: {to}（已注册: {_dep('list_agents')(active_only=False)}）",
            status_code=404,
        )

    # 检查任务书存在（空路径/非文件/非法路径 → 拒绝）
    # 测试3补充: is_file() 对超长路径/null字节会抛 OSError，需 try/except
    if not task_path:
        raise _dep('DispatchError')("task_path 为空", status_code=400)
    try:
        if not Path(task_path).is_file():
            raise _dep('DispatchError')(f"任务书不存在或不是文件: {task_path}", status_code=400)
    except (OSError, ValueError) as e:
        raise _dep('DispatchError')(f"task_path 非法: {e}", status_code=400)

    # === Task 1849: 任务书 frontmatter 预算声明优先（budget_usd）===
    # frontmatter 是任务书作者的显式预算声明，优先于调用方默认值（API 层
    # 目前无 budget 传参通道，恒默认 $2.0）。与声明脱节的帽会误杀健康任务。
    _fm_budget = _read_task_budget_usd(task_path)
    if _fm_budget is not None:
        logger.info(f"[dispatch] 任务书 frontmatter budget_usd={_fm_budget} 覆盖默认 cost 帽（Task1849）")
        cost_budget_usd = _fm_budget

    # === Step 0: flock 非阻塞 ===
    fd = _dep('_acquire_lock')(to)
    if fd is None:
        raise _dep('DispatchError')(
            f"Agent {to} 已有任务在跑（锁冲突）",
            status_code=409,
        )

    # === Step 0.5: 忙碌硬检查（Task991）===
    # 为什么 flock 之外还要查：flock 防"提交瞬间"竞态（两 dispatch 同时抢同一
    # agent 的秒级窗口）；本检查防"时段"竞态——wait=False 异步派发后锁即释放，
    # 同一 agent 可再被派第二个任务（09010832 实锤：kimi 双主控各派一个
    # RUNNING，双任务书进同一 TUI 输入流）。口径=task_state.list_active
    # （RUNNING+PENDING 非终态，与 patrol 一致）；force=true 显式越过（合法
    # 并派场景出口，409 payload 写明用法）。
    if not force:
        # Task1543 #217: 挂起中（suspend_until 未到）的任务不算 busy——设计内等窗口
        # 任务不应拦新派单（同口径当前时间 vs epoch，与 patrol 一致，无清理竞态）
        _now_busy = time.time()
        _busy = [t for t in task_state.list_active() if t.get("callee") == to
                 and not (t.get("suspend_until") and _now_busy < float(t.get("suspend_until") or 0))]
        if _busy:
            # 本检查在主 try 之外（finally 管不到）——抛出前必须手动放锁，
            # 否则每次 busy 拒都泄漏一把 flock（991 测试场景连环 409 实锤）
            _dep('_release_lock')(fd)
            raise _dep('DispatchError')(
                json.dumps({
                    "busy": True,
                    "error": (f"Agent {to} 已有 {len(_busy)} 个在途任务——等 done 或换 agent；"
                              f"确认要并派则在请求体加 \"force\": true"),
                    "existing": [{"task_id": t.get("task_id"), "status": t.get("status"),
                                  "task_path": t.get("task_path"),
                                  "caller": t.get("caller", "?")} for t in _busy],
                    "hint": "Task991 忙碌硬检查（loop-auto-advance §14 小弟 409 落地）",
                }, ensure_ascii=False),
                status_code=409,
            )
    task_id = f"xdr-{to}-{int(time.time())}-{uuid.uuid4().hex[:6]}"
    # 1646: 高风险标记（dispatch-orchestrator §5 审读机器化的标记半边）——
    # 任务书内容命中风险词表 → task_state 记 high_risk=true（ack 侧强制 audit_mem）。
    # 词表可维护（本行注释处）；读文件失败不标记（fail-open 标记位, 后续自报错）。
    _RISK_WORDS = ("生产上线", "上线", "发布", "批量", "数据操作", "公网", "端口", "凭据", "权限")
    _high_risk = False
    try:
        with open(task_path, encoding="utf-8", errors="replace") as _f:
            _taskmd = _f.read()
        _high_risk = any(w in _taskmd for w in _RISK_WORDS)
    except OSError:
        pass
    # Task576 例②: 此日志只表"开始派发"不代表已注册（注册确认日志在 create_task
    # 写后读校验之后）。原 [dispatch] 标签会被误读为"已注册"——0818 Task568
    # 进程重启窗口内日志已出、注册未落盘，60 次 GET /task 404。
    logger.info(f"[dispatch-begin] {caller} → {to}, task_id={task_id}, task={task_path}")

    try:
        # === Step 2: session 存在性 + is_idle 二次确认 ===
        # 区分"session不存在"(503，可重启) vs "session忙碌"(409，等)
        if not agent.has_session():
            raise _dep('DispatchError')(
                f"Agent {to} session 不存在（需重启小弟: start_tool.sh start {to}）",
                status_code=503,
            )
        if not agent.is_idle():
            raise _dep('DispatchError')(
                f"Agent {to} session 不空闲（可能正在执行任务）",
                status_code=409,
            )

        # === Step 3: 注册前置（Task576 例②）===
        # Task 277: 删除 _register_to_19999 调用（19999 半退役，xdr 不再注册到 19999）
        # 🔴 Task576: create_task 从副本写入之后（原 :1462）前移到此处——
        # session 检查后、副本写入/MEM 注入（~70 行慢操作，含网络 I/O）之前。
        # 消灭"日志已出、注册未落盘"的不可见窗口；此后任何异常走 except Exception
        # 标 FAILED（不留孤儿 PENDING）。dispatched_path 副本创建后 update_field 补记。
        task_state.create_task(
            task_id=task_id,
            caller=caller,
            callee=to,
            task_path_file=task_path,  # 保留原路径（用于 _find_task_root）
            timeout=timeout,
            extra={
                "agent_type": agent.agent_type,
                "notify_url": notify_url,
                # 1646: 仅命中词表时写字段（普通/历史任务无此字段=ack 侧自然零打扰）
                **({"high_risk": True} if _high_risk else {}),
            },
        )
        # Task1543 #217: 挂起窗口落顶层字段（patrol/busy 双消费；epoch 秒归一化）
        _susp_norm = _parse_suspend_until(suspend_until)
        if _susp_norm is not None:
            if _susp_norm <= time.time():
                raise _dep('DispatchError')(
                    f"suspend_until={suspend_until} 已是过去时间（epoch={_susp_norm:.0f} ≤ now）——挂起窗口必须指向未来",
                    status_code=400,
                )
            task_state.update_field(task_id, "suspend_until", _susp_norm)
        # Task576 例②: 写后读校验——create 后立即 load，None=存储层故障，fail-fast
        if task_state.load_state(task_id) is None:
            raise _dep('DispatchError')(
                f"task_state 写后读校验失败: {task_id}（create_task 后 load 为 None，存储层异常）",
                status_code=500,
            )
        logger.info(f"[dispatch] {task_id} 注册成功（task_state 已落盘并写后读校验）")

        # === Task 279 P1-6: 创建任务书副本，task_id 替换（方案 5D）===
        # 原任务书里 hardcode 的 task_id（如 279）只用于人类可读性
        # 副本里替换为 xdr-ac-XXX，小弟读副本时拿到的就是真实 task_state 主键
        # 副本与原任务书同目录（保留 _find_task_root 兼容性）
        # Task639 🟡5: 预初始化——read_text 失败走 except 后，:338 预检仍引用
        # dispatched_text/original_text，未初始化会 NameError 掩盖原始错误
        dispatched_text = original_text = ""
        dispatched_path = Path(task_path).parent / f"task.dispatched.{task_id}.md"
        try:
            original_text = Path(task_path).read_text(encoding='utf-8')
            # 替换任务书里 hardcode 的 task_id（多种格式，保留原格式只换值）
            # 匹配: `**task_id**`: `279` / "task_id": "279" / task_id=`279`（backtick，Task532 P-d1）
            patterns = [
                (r'(`?\*\*task_id\*\*[`:：]\s*`?)([a-zA-Z0-9_-]+)(`?\s*\n)', r'\g<1>' + task_id + r'\g<3>'),
                (r'("task_id"\s*:\s*")([a-zA-Z0-9_-]+)(")', r'\g<1>' + task_id + r'\g<3>'),
                (r'(task_id[`=：]\s*`)([a-zA-Z0-9_-]+)(`)', r'\g<1>' + task_id + r'\g<3>'),
            ]
            dispatched_text = original_text
            for pat, repl in patterns:
                dispatched_text = re.sub(pat, repl, dispatched_text)

            # 如果原任务书没有显式 task_id 标记，在文件开头追加一行（让小弟能读到真实 task_id）
            if dispatched_text == original_text:
                dispatched_text = f"> 🔧 dispatched xdr task_id: `{task_id}`（回调时用这个 task_id，不要用任务书正文里 hardcode 的值）\n\n" + original_text

            _atomic_write_text(dispatched_path, dispatched_text)  # Task1594 原子写
            logger.info(f"[dispatch] {task_id} 任务书副本: {dispatched_path}")

            # === 08040834: 动态注入——从 MEM 拉相关经验注入到副本 ===
            # 在副本写入后、gate 前。fail-open（注入失败不阻塞 dispatch）。
            # 让小弟自动获得相关上下文，不靠主控自觉写 references.md。
            try:
                from nexusos_xdr.exp_injector import inject_experiences
                original_text_for_inject = Path(task_path).read_text(encoding='utf-8')
                injected_text = inject_experiences(
                    dispatched_text=dispatched_text,
                    task_id=task_id,
                    target_agent=to,
                    task_text=original_text_for_inject,
                    task_path=task_path,  # Task583: UK 假设腐烂提醒扫描用
                )
                if injected_text != dispatched_text:
                    _atomic_write_text(dispatched_path, injected_text)  # Task1594 原子写
                    dispatched_text = injected_text  # 更新变量，后续步骤用注入后的
                    logger.info(f"[dispatch] {task_id} 副本已注入 MEM 经验")
            except Exception as _inj_e:
                logger.warning(f"[dispatch] {task_id} 动态注入失败（fail-open）: {_inj_e}")
        except Exception as e:
            logger.warning(f"[dispatch] {task_id} 创建任务书副本失败（继续，用原路径）: {e}")
            dispatched_path = Path(task_path)  # fallback 用原路径

        # Task576 例②: create_task 已前移到 session 检查后（见 Step 3）。
        # 副本路径补记顶层字段（原存 meta.dispatched_path 无消费方；顶层更可见，
        # update_field 走 _state_lock 防 RMW 竞态）。
        try:
            task_state.update_field(task_id, "dispatched_path", str(dispatched_path))
        except ValueError as e:
            logger.warning(f"[dispatch] {task_id} update_field dispatched_path 失败: {e}")
        # 缺陷5: notify_url 存到 state 顶层（服务重启恢复用）
        # Task 280 P1-19: 改用 task_state.update_field 公开接口，不直改 state dict 绕过状态机
        if notify_url:
            try:
                task_state.update_field(task_id, "notify_url", notify_url)
            except ValueError as e:
                logger.warning(f"[dispatch] {task_id} update_field notify_url 失败: {e}")

        # === Task 295 Step 1: dispatch 预检（should_auto_approve + precheck_for_agent）===
        # 纯记录，不阻塞——失败优雅降级
        try:
            _dep('_precheck_tools_for_dispatch')(task_id, to, dispatched_text or original_text)
        except Exception as e:
            logger.warning(f"[Task295][{task_id}] 预检入口异常（不阻塞）: {e}")

        # === Task393 P1: dispatch gate 自动触发（gate_passed 写入凭据 prerequisites）===
        # 第4轮设计2：dispatch() 内路由内触发，Agent 绕不过
        # gate 失败不阻塞 dispatch（fail-open），但凭据 prerequisites 标 gate_passed=False
        _dispatch_gate_passed = "unknown"  # 默认 unknown（gate 不可用时）
        try:
            from nexusos_gate.gate_engine import check as _gate_check_fn  # Task668 ②
            # Task530 A4: task_dir 传任务根（对照 :1774 正确写法）。
            # task_path 是 input/task.md 时 parent 是 input/ → D1/D2 查
            # {task_dir}/input/task.md 变 input/input/task.md 必不存在。
            _td_a4 = Path(task_path).parent  # input/ 或任务根
            if _td_a4.name == "input":
                _td_a4 = _td_a4.parent  # 任务根（含 output/）
            _disp_gate_result = _gate_check_fn("dispatch", {
                "task_id": task_id,
                "task_dir": str(_td_a4),
                "target_agent": to,
            }, agent=caller)
            _dispatch_gate_passed = _disp_gate_result.get("passed", False)
            _gate_warns = [f"{r.get('id','?')}" for r in _disp_gate_result.get("warns", [])]
            if not _dispatch_gate_passed:
                logger.warning(f"[Task393] {task_id} dispatch gate REJECT: "
                               f"{[r.get('id') for r in _disp_gate_result.get('hard_fails', [])]}")
            elif _gate_warns:
                logger.info(f"[Task393] {task_id} dispatch gate WARN: {_gate_warns}")
            else:
                logger.info(f"[Task393] {task_id} dispatch gate passed")
        except ImportError:
            logger.debug(f"[Task393] gate_engine 不可用, dispatch gate 跳过")
        except Exception as _dge:
            logger.debug(f"[Task393] dispatch gate 异常（fail-open）: {_dge}")

        # === Task392 P0+P1: 签发 dispatch 凭据（gate 结果写入 prerequisites）===
        # 凭据 = 行为合法发生的必要条件证明。dispatch 签发 → callback 校验 → 双向信任。
        _cred_id = None  # Task441: 提升到外层作用域，副本注入需要用
        _search_cred_id = None
        try:
            from nexusos_xdr.credential import issue_credential
            _cred_id = issue_credential(
                cred_type="dispatch_cred",
                bearer=to,
                prerequisites={
                    "gate_passed": _dispatch_gate_passed,  # P1: 写入 gate 真实结果
                    "task_spec_complete": True,
                    "precheck_passed": True,
                },
                task_id=task_id,  # Task563: 凭据绑定任务（apply_attempt 归因反查用）
            )
            if _cred_id:
                task_state.update_field(task_id, "credential_id", _cred_id)
                logger.info(f"[Task393][{task_id}] dispatch 凭据已签发(gate={_dispatch_gate_passed}): {_cred_id}")
            else:
                logger.warning(f"[Task393][{task_id}] dispatch 凭据签发失败（fail-open 继续）")

            # === Task441: 同时签发 search_cred（让小弟 search 能带凭据）===
            # 设计：fail-open，签发失败不阻塞 dispatch。关联 dispatch_cred 作为 prerequisite。
            try:
                _search_cred_id = issue_credential(
                    cred_type="search_cred",
                    bearer=to,
                    prerequisites={
                        "gate_passed": _dispatch_gate_passed,
                        "task_spec_complete": True,
                        "dispatch_cred": _cred_id,  # 关联 dispatch_cred（契约链）
                    },
                    task_id=task_id,  # Task563: 小弟 search 凭据绑任务（api 反查兜底）
                )
                if _search_cred_id:
                    task_state.update_field(task_id, "search_credential_id", _search_cred_id)
                    logger.info(f"[Task441][{task_id}] search 凭据已签发: {_search_cred_id}")
                else:
                    logger.warning(f"[Task441][{task_id}] search 凭据签发失败（fail-open）")
            except Exception as _sc_e:
                logger.warning(f"[Task441][{task_id}] search 凭据签发异常（fail-open）: {_sc_e}")

            # === Task441: 凭据注入到副本 frontmatter（让小弟能读到 credential_id + search_credential_id）===
            # 副本已在 L1335 写入。这里在签发凭据后回写 frontmatter。
            # 借鉴 exp_injector 的"副本写入后再次重写"模式（dispatchd.py:1351）。
            try:
                if _cred_id or _search_cred_id:
                    _cred_lines = []
                    if _cred_id:
                        _cred_lines.append(f"credential_id: {_cred_id}")
                    if _search_cred_id:
                        _cred_lines.append(f"search_credential_id: {_search_cred_id}")
                    _cred_block = "\n".join(_cred_lines)

                    _cur_text = dispatched_path.read_text(encoding='utf-8')
                    if _cur_text.startswith("---\n"):
                        _fm_end = _cur_text.find("\n---", 4)
                        if _fm_end > 0:
                            _new_text = (
                                _cur_text[:_fm_end] + "\n" + _cred_block +
                                _cur_text[_fm_end:]
                            )
                        else:
                            # frontmatter 无结束标记，包裹一层
                            _new_text = f"---\n{_cred_block}\n---\n\n" + _cur_text
                    else:
                        # 无 frontmatter，创建一个
                        _new_text = f"---\n{_cred_block}\n---\n\n" + _cur_text
                    _atomic_write_text(dispatched_path, _new_text)  # Task1594 原子写
                    logger.info(f"[Task441][{task_id}] 凭据已注入副本 frontmatter: "
                                f"cred={bool(_cred_id)} search_cred={bool(_search_cred_id)}")
            except Exception as _ci_e:
                logger.warning(f"[Task441][{task_id}] 凭据注入副本异常（fail-open，小弟需手动校验）: {_ci_e}")
            # === Task473: 副本正文注入 search 命令块（修复规则-现实断裂）===
            # 凭据已在 frontmatter，但小弟不解析 frontmatter。把命令直接塞正文。
            try:
                if _search_cred_id:
                    _search_cmd_block = _SEARCH_CMD_BLOCK_TMPL.format(
                        cred=_search_cred_id, to=to)
                else:
                    _search_cmd_block = _SEARCH_CMD_BLOCK_ANON
                _cur_text2 = dispatched_path.read_text(encoding='utf-8')
                # 注入位置：frontmatter 结束（\n---\n）之后，正文最前面
                if _cur_text2.startswith("---\n"):
                    _fm_end2 = _cur_text2.find("\n---", 4)
                    if _fm_end2 > 0:
                        _inject_pos = _fm_end2 + 4  # \n--- 之后
                        _new_text2 = _cur_text2[:_inject_pos] + _search_cmd_block + _cur_text2[_inject_pos:]
                    else:
                        _new_text2 = _cur_text2 + _search_cmd_block
                else:
                    _new_text2 = _search_cmd_block + _cur_text2
                _atomic_write_text(dispatched_path, _new_text2)  # Task1594 原子写
                logger.info(f"[Task473][{task_id}] search 命令块已注入副本正文 (cred={'有' if _search_cred_id else '无'})")
            except Exception as _si_e:
                logger.warning(f"[Task473][{task_id}] search 命令块注入异常（fail-open）: {_si_e}")
        except Exception as e:
            logger.warning(f"[Task393][{task_id}] 凭据签发异常（fail-open）: {e}")

        # === Step 4: ensure_no_popup（A 方案）===
        try:
            agent.ensure_no_popup()
        except Exception as e:
            logger.warning(f"[{to}] ensure_no_popup 异常（继续）: {e}")

        # === Task595 #45: 副作用前 checkpoint 屏障（send_task 不可逆，先留恢复点）===
        # 写失败=fail-closed 拒绝派发（canonical 错误码，不静默降级）；
        # 瞬时失败重试 1 次（陷阱：别误伤正常流）。done 清算后标 consumed。
        _barrier_cp_path = _write_dispatch_barrier_checkpoint(
            task_id=task_id, task_path=task_path, caller=caller, to=to,
        )
        task_state.update_field(task_id, "barrier_checkpoint", _barrier_cp_path)

        # === Step 5+6: send_task（含回调指令）===
        # Task 279 P1-6: send_task 传副本路径（task.dispatched.<task_id>.md），
        # 小弟读副本时拿到的 task_id 已是 xdr-ac-XXX 格式，与 task_state 主键一致
        task_state.update_status(task_id, "RUNNING")

        # 🔴 Task 325 试运行修复：写 task_id 上下文文件（dispatch 主流程，所有 Agent 必经）
        # 不能放 base.py send_task（AC 重写了 send_task 不调 super，会绕过）
        # 放 dispatch 主流程 send_task 调用前，确保所有 Agent 都生效
        try:
            ctx_file = Path(os.environ.get("NEXUS_TMP_DIR") or subpath("tmp")) / f"nexus_current_task_{to}.txt"
            ctx_file.write_text(task_id, encoding="utf-8")
            logger.info(f"[Task325][{task_id}] 写入 task 上下文文件: {ctx_file}")
        except Exception as _e:
            logger.warning(f"[Task325] 写 task 上下文文件失败（不阻塞）: {_e}")

        if not agent.send_task(str(dispatched_path), task_id):
            task_state.update_status(task_id, "FAILED", error="send_task 返回失败", end_reason="error")
            raise _dep('DispatchError')(f"send_task 到 {to} 失败", status_code=502)

        logger.info(f"[dispatch] {task_id} 已派发给 {to}，等待回调...")

        # === Task637 池#74: presence 服务端代注册（投递成功即在线，不依赖小弟自觉 init）===
        _presence_autoregister(to, task_id, task_path)

        # === P1-2: 启动 B 兜底监控线程（设计 §4.2 Step 7）===
        # 无论 wait 与否都启动——wait=False 时 patrol 是唯一弹窗兜底
        # 场景1/2/4/5: patrol 同时负责输出检测、stuck通知、超时退出、webhook通知
        import threading
        patrol_stop = threading.Event()
        start_patrol_thread(agent, task_id, patrol_stop,
                             task_path=task_path, timeout=timeout, notify_url=notify_url,
                             max_steps=max_steps, cost_budget_usd=cost_budget_usd,
                             barrier_cp_path=_barrier_cp_path)

        # === 非 wait 模式: 立即返回（patrol 线程继续跑）===
        if not wait:
            _r991 = {"task_id": task_id, "status": "RUNNING", "callee": to,
                     "message": f"已派发，异步执行中。查状态: GET /task/{task_id}"}
            _w991 = _nn_clash_warning(task_path)
            if _w991:
                _r991["warning"] = _w991
            return _r991

        # === Step 7-9: 等待回调 ===
        result = _dep('_wait_for_callback')(task_id, timeout)
        patrol_stop.set()  # 通知 patrol 线程退出

        # === Task595 #45: wait 模式清算——屏障 checkpoint 标 consumed ===
        # （wait=False 模式的清算是 patrol_loop 终态 finally / http_callback 公共 DONE 路径）
        _consume_barrier_checkpoint(task_id, _barrier_cp_path)

        # === Step 10: 处理结果 ===
        final_status = result["status"]
        if final_status == "DONE":
            # Task524 P1-2: http_callback 可能已将状态设为 DONE（终态不可迁移的
            # 二次 update 会抛 ValueError → _dep('DispatchError') 500）。try/except 容忍
            # 二次迁移（callback 已设过说明事实正确），对齐 TIMEOUT/FAILED 分支模式。
            try:
                task_state.update_status(
                    task_id, "DONE",
                    result=result.get("result") or {"summary": "done"},
                    end_reason="completed",
                )
            except ValueError:
                logger.debug(f"[dispatch] {task_id} 状态已是 DONE（callback 预设），跳过二次迁移")
            # Task496: auto-relocate 触发已移到 http_callback（mcp_server.py），
            # 覆盖两种 wait 模式 + 小弟主动 done 的公共主路径。此处删冗余调用。
        elif final_status == "TIMEOUT":
            # Task 298 修复后新发现的 bug：patrol 内部已将状态设为 TIMEOUT，
            # 主流程这里再 update 会触发 task_state "已是终态不能迁移" ValueError。
            # try/except 容忍二次迁移（patrol 已设过说明事实正确）。
            try:
                task_state.update_status(task_id, "TIMEOUT", error=f"超时 {timeout}s", end_reason="timeout")
            except ValueError as _e:
                logger.debug(f"[dispatch] {task_id} 状态已是 TIMEOUT（patrol 预设），跳过二次迁移")
        elif final_status in ("FAILED", "CANCELLED"):
            # Task 280 P1-11: load_state 可能返 None（任务文件被删/损坏），加 None 检查
            cur_state = task_state.load_state(task_id)
            if cur_state and cur_state.get("status") != "CANCELLED":
                try:
                    task_state.update_status(task_id, "FAILED", error=result.get("error", "failed"), end_reason="error")
                except ValueError as _e:
                    logger.debug(f"[dispatch] {task_id} 状态已是终态，跳过迁移")

        # === Step 11: 返回最终结果 ===
        # Task 280 P1-11: load_state None 检查（任务文件被删/损坏时不崩）
        final_state_obj = task_state.load_state(task_id)

        # === Task 295 Step 2: done 时记 tool_calls（失败不阻塞）===
        # 覆盖所有终态：DONE=success=True，FAILED/TIMEOUT/CANCELLED=success=False。
        # 只在终态 log（不在 started/progress 时 log，避免噪音）。
        try:
            _dep('_log_tools_on_done')(
                task_id=task_id,
                callee=to,
                success=(final_status == "DONE"),
                duration_sec=(final_state_obj or {}).get("duration_sec"),
            )
        except Exception as e:
            logger.warning(f"[Task295][{task_id}] log_tool_call 入口异常（不阻塞）: {e}")

        # === Task 321 Step 3: Phase 1 xdr 钩子——自动捕获套用候选 ===
        # 覆盖所有终态（DONE/TIMEOUT/FAILED/CANCELLED），因为 search 记录在任何终态都可能有。
        # 陷阱5：开关在调用前判断（避免无谓函数调用开销 + 函数内部已无开关，职责单一）。
        # _capture_apply_candidates 内部已 try/except，外层无需再包（fail-open）。
        if _dep('apply_capture_enabled')():
            _dep('_capture_apply_candidates')(task_id=task_id, agent_id=to)

        # Task 322 试运行修复：清理小弟 task_id 上下文文件（防下次同 agent 关联到旧 task）
        try:
            ctx_file = Path(os.environ.get("NEXUS_TMP_DIR") or subpath("tmp")) / f"nexus_current_task_{to}.txt"
            if ctx_file.exists():
                ctx_file.unlink()
        except Exception as _e:
            logger.debug(f"[Task322] 清理 task 上下文文件异常（不阻塞）: {_e}")

        _r991 = {
            "task_id": task_id,
            "status": final_status,
            "callee": to,
            "caller": caller,
            "result": result.get("result"),
            "error": result.get("error"),
            "duration": final_state_obj.get("duration_sec") if final_state_obj else None,
        }
        _w991 = _nn_clash_warning(task_path)
        if _w991:
            _r991["warning"] = _w991
        return _r991

    except _dep('DispatchError'):
        raise
    except Exception as e:
        logger.exception(f"[dispatch] {task_id} 异常")
        try:
            task_state.update_status(task_id, "FAILED", error=str(e), end_reason="error")
        except Exception:
            pass
        raise _dep('DispatchError')(f"dispatch 内部错误: {e}", status_code=500)
    finally:
        # 锁必须释放（v0.4 task lifecycle: try/finally 保证）
        _dep('_release_lock')(fd)
        logger.debug(f"[dispatch] {task_id} 锁已释放")
        # Task 301 (2026-07-21): _cleanup_dispatched_copy 已挪到 patrol_loop 的 finally 块。
        # 🔴 原 Task 298 P0-1 在这里调 cleanup 是 bug：dispatch() wait=False 时派完即 return，
        # finally 立即执行 cleanup，但此时任务还在小弟 tmux 里跑！AC 读副本时副本已被删。
        # 正确位置：patrol_loop finally（任务终态后小弟不再读副本时才清）。
        # dispatch() finally 不再清，留 patrol finally 唯一负责。


def check_status(task_id: str) -> Dict[str, Any]:
    """查任务状态。

    Task 277: 删除 19999 fallback，task_state 是唯一真相源。
    """
    state = task_state.load_state(task_id)
    if state is None:
        raise _dep('DispatchError')(f"任务不存在: {task_id}", status_code=404)
    return {"task_id": task_id, "source": "xdr-state", **state}


def cancel_task(task_id: str) -> Dict[str, Any]:
    """取消任务。

    v0.4 ds2 修订: Ctrl+C 可能中断写操作致文件损坏。
    流程:
    1. 写 checkpoint（capture-pane 当前状态）
    2. send-keys Ctrl+C
    3. 等待 2s 让小弟清理
    4. 验证 task 目录文件完整性（md5 对比，如有 checkpoint）
    5. 更新状态 CANCELLED + 释放锁
    """
    state = task_state.load_state(task_id)
    if state is None:
        raise _dep('DispatchError')(f"任务不存在: {task_id}", status_code=404)

    if state["status"] in task_state.TERMINAL_STATES:
        raise _dep('DispatchError')(f"任务已终态: {state['status']}", status_code=409)

    # Task 280 P1-9: 先 stop patrol 线程（避免孤儿线程继续空转）
    patrol_stop_evt = _dep('get_active_patrols')().pop(task_id, None)
    if patrol_stop_evt is not None:
        patrol_stop_evt.set()
        logger.info(f"[cancel] {task_id} 已通知 patrol 线程退出")

    callee = state.get("callee")
    agent = _dep('get_agent')(callee) if callee else None

    # P1-3 Step 1.5: Ctrl+C 前记录 task 目录文件 md5（v0.4 ds2 修订）
    # P1-NEW-2(ac) 修正: 目录范围从 input/ 改为真实任务根（含 output/）
    task_path_file = state.get("task_path", "")
    before_md5 = {}
    if task_path_file:
        task_dir_input = Path(task_path_file).parent  # input/
        # 真实任务根 = 去 /input 后缀（dispatch.sh:163 的逻辑）
        if task_dir_input.name == "input":
            task_dir = task_dir_input.parent  # 含 output/ 的任务根
        else:
            task_dir = task_dir_input
        if task_dir.exists():
            before_md5 = _dep('_snapshot_dir_md5')(task_dir)

    # 1. checkpoint
    if agent:
        pane_text = agent.capture_pane(lines=50)
        task_state.save_checkpoint(task_id, pane_text)

        # 2. Ctrl+C
        try:
            subprocess.run(
                ["tmux", "send-keys", "-t", f"={agent.tmux_session}:", "C-c"],
                timeout=5, capture_output=True,
                env=scrubbed_env_or_raise(),
            )
        except Exception as e:
            logger.warning(f"cancel Ctrl+C 失败: {e}")

    # 3. 等待清理（Task639 🟡9: 轮询 md5 稳定替代硬 sleep 2s——连续两次目录
    # 快照一致=小弟写操作已停止；上限 10×0.3s=3s。无 task_dir 快照条件时保
    # 留原固定 2s 等待。）
    if before_md5 and task_dir and task_dir.exists():
        _last_snap = _dep('_snapshot_dir_md5')(task_dir)
        for _poll in range(10):
            time.sleep(0.3)
            _cur_snap = _dep('_snapshot_dir_md5')(task_dir)
            if _cur_snap == _last_snap:
                break
            _last_snap = _cur_snap
    else:
        time.sleep(2)

    # P1-3 Step 4: 文件完整性验证（md5 对比，范围=真实任务根含 output/）
    # 修复(2026-07-29): 原版只检测"before有after也有但md5变了",漏掉两类:
    # ① 文件被删(md5_after is None 短路跳过) ② Ctrl+C 后新增的文件完全不检查。
    corrupted = []
    if before_md5 and task_dir and task_dir.exists():
        after_md5 = _dep('_snapshot_dir_md5')(task_dir)
        for name, md5_before in before_md5.items():
            md5_after = after_md5.get(name)
            if md5_after is None:
                # 文件被 Ctrl+C 中断删除(原版漏检)
                corrupted.append(f"{name}(deleted)")
            elif md5_after != md5_before:
                corrupted.append(f"{name}(modified)")
        # 检测新增文件(after 有 before 没,可能是中断时写了一半)
        for name in after_md5:
            if name not in before_md5:
                corrupted.append(f"{name}(new_unexpected)")

    # 5. 更新状态
    if corrupted:
        # v0.4 Step 5: 文件损坏 → 标记 CANCELLED（detail 在 error 字段）
        # Task 281 P2-24: 修正注释 CANCELLED_CORRUPTED → CANCELLED（task_state TERMINAL_STATES 只有 CANCELLED）
        task_state.update_status(
            task_id, "CANCELLED",
            error=f"用户取消；检测到文件损坏: {corrupted}",
            end_reason="aborted",
        )
        logger.warning(f"[cancel] {task_id} 文件损坏: {corrupted}")
        return {
            "task_id": task_id,
            "status": "CANCELLED",
            "corrupted_files": corrupted,
            "message": f"已取消，但检测到 {len(corrupted)} 个文件可能损坏（Ctrl+C 中断写操作）: {corrupted}",
        }

    task_state.update_status(task_id, "CANCELLED", error="用户取消", end_reason="aborted")

    logger.info(f"[cancel] {task_id} 已取消，checkpoint 已保存，文件完整性正常")
    return {
        "task_id": task_id,
        "status": "CANCELLED",
        "message": "已发送 Ctrl+C，checkpoint 已保存，文件完整性验证通过。如需审查中断点，查看 state.checkpoint.pane_tail。",
    }


def request_approval(
    operation: str,
    reason: str,
    caller: str,
    callee: str = "user",
) -> Dict[str, Any]:
    """请求审批（由调用方 Agent 处理）。

    v0.4 审批路由: 不再 forward 给 Hermes，由 caller 的审批策略决定。
    """
    # 先过 guards（v0.4: api_server 也要过 guards）
    try:
        _, warnings = execute_guard_global(operation)
    except DestructiveOperationError as e:
        return {"approved": False, "reason": str(e), "by": "guards"}

    # 获取 caller 的审批策略
    caller_agent = _dep('get_agent')(caller)
    policy = caller_agent.approval_policy if caller_agent else "manual"

    if policy == "auto_approve_all":
        return {"approved": True, "by": caller, "policy": policy, "warnings": warnings}
    elif policy == "auto_approve_except":
        if warnings:
            return {"approved": False, "reason": "危险操作需人工确认", "warnings": warnings, "by": caller}
        return {"approved": True, "by": caller, "policy": policy}
    else:  # manual
        return {
            "approved": None,
            "status": "pending",
            "waiting_on": caller,
            "operation": operation,
            "reason": reason,
            "warnings": warnings,
        }


def list_supported_agents() -> list:
    """列出所有支持的 Agent（仅 active 状态，退役的隐藏）。"""
    return _dep('list_agents')(active_only=True)
