#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""nexusos_xdr.callback.outcome_autofill — done 回调 outcome 自动回填钩子（Task1450）。

业务目标（task/1450 §0）：done 回调 gate 完成后，自动把本任务「真套用未回填」
的 annotate attempt 回填 outcome，把人工遗漏型 pending（1447 实证：有效回填仅
475/2828）在管道层消灭。feature flag NEXUS_OUTCOME_AUTOFILL 默认 off，灰度由
主控决定。

回填规则（保守，宁缺勿滥——任务书陷阱2）：
    - tests.passed>0 且 failed==0 → 任务成功，套用判 success
    - 其余（failed>0 / 缺 tests）→ 不回填保持 pending 留人工——「套用经验与
      失败域相关」无法自动判定，任务成功≠套用的每条经验都有效，宁可不填

真套用过滤（G10 口径同源，不自创第二口径）：
    - 只回填 is_real_apply（nexusos_mem.apply_honesty）态2 命中的 attempt：
      pending + attempt.created 起 24h 内 report 引用 MEM#<target_id>
    - 查证式命中（态3：只查过没用过）不回填——填 success=数据造假
    - scope 限定本 task_id（G10 的 agent 当日跨 task 欠账不代填：其他任务的
      成败与本任务 tests 无关联，归各自 done 回调/人工）

幂等与防死循环（MEM#70646 防再踩）：
    - 只在 done 回调 bg-gate 线程尾巴执行（不进回调热路径——Task492 前车之鉴）
    - SQL 双守卫：outcome IN (NULL,'pending') 且无非-pending apply_outcome 行
      ——已回填（列或行任一表示）天然跳过，重复 done 不重复计 ref_count
      （record_apply_outcome 重复调用会多计，见 shared.py:1356 注）
    - 通电（Task412）写的 pending 占位 apply_outcome 行不阻断回填（行过滤只排
      非 pending result）

调用方：callback/handler.py `_bg_gate_check` 尾巴（flag 分支内 lazy import）。
"""

import logging
import os

# 陷阱2 合规（同 handler.py）：logger 名保持 nexusos-xdr.mcp——M8 按格式解析
# error.log，新模块不改 logger 名，避免观测层解析断链。
logger = logging.getLogger("nexusos-xdr.mcp")

# 单任务最多回填条数（防极端批量写放大；与 Task412 通电 [:10] 同思路）
MAX_AUTOFILL = 20


def _decide_result(body: dict) -> "str | None":
    """从回调 body 的 tests 推断回填 result。

    业务规则：passed>0 且 failed==0 → 'success'；其余 None（不确定，留人工）。
    tests 缺失/结构异常一律 None——没有测试证据就不替人下结论。
    """
    tests = body.get("tests") if isinstance(body, dict) else None
    if not isinstance(tests, dict):
        return None
    try:
        passed = int(tests.get("passed", 0) or 0)
        failed = int(tests.get("failed", 0) or 0)
    except (TypeError, ValueError):
        return None
    if passed > 0 and failed == 0:
        return "success"
    return None


def _pending_attempts(task_id: str) -> list:
    """查本 task 待回填 attempt（幂等双守卫都在 SQL 里）。

    守卫1（列口径）：outcome IS NULL 或 'pending'——已回填真值的行为不在集内。
    守卫2（行口径）：不存在 result 非 pending 的 apply_outcome 行关联本 attempt
        ——列回写失败（shared.py 两处 try/except pass 兜底路径）时防重复回填
        多计 ref_count。通电写的 pending 占位行不算（result='pending' 不排除）。
    守卫3（行口径·1453 补，Task1450 移交缺陷）：手动 POST /apply_outcome 合法路径
        可只带 knowledge_id 不带 apply_attempt_id（gate G10 提示口径），守卫2 的
        attempt_id 关联匹配不到这类行→已手动回填的 attempt 仍被判 pending 重复
        回填。此处按「同 agent+同经验+attempt 创建之后+result 非 pending」维度
        排除——created>=a.created 防历史任务对同一经验的旧回填误伤新 attempt。
    """
    import psycopg2

    conn = psycopg2.connect("dbname=nexus_mem")
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT a.id, a.target_id, a.created
            FROM agent_activity a
            WHERE a.action='apply_attempt' AND a.task_id=%s
              AND (a.outcome IS NULL OR a.outcome='pending')
              AND NOT EXISTS (
                  SELECT 1 FROM agent_activity o
                  WHERE o.action='apply_outcome'
                    AND o.detail->>'apply_attempt_id'=a.id::text
                    AND o.detail->>'result' != 'pending')
              AND NOT EXISTS (
                  SELECT 1 FROM agent_activity o
                  WHERE o.action='apply_outcome'
                    AND o.target_id = a.target_id
                    AND o.agent_id = a.agent_id
                    AND o.created >= a.created
                    AND o.detail->>'result' != 'pending')
            ORDER BY a.id
            """,
            (task_id,),
        )
        rows = [
            {"id": r[0], "target_id": r[1], "created": r[2]}
            for r in cur.fetchall()
        ]
        cur.close()
        return rows
    finally:
        conn.close()


def _task_is_done(task_id: str) -> bool:
    """单查任务是否已到终态 DONE（回填前提=终态，任务书 §5.2）。

    不做等待轮询：bg-gate 线程跑完 gate 检查是秒级耗时，主路径
    update_status DONE 是毫秒级落盘——gate 尾巴执行时 DONE 早已可查。
    极小概率竞态（gate 快于主路径）→ 本轮跳过，attempt 留 pending 由
    G10/人工兜底（WARN 记录语义本就存在）。不为极小概率加轮询休眠
    ——code gate C2 禁新增阻塞式等待调用，且此处等待本无必要。
    """
    from nexusos_xdr import task_state

    try:
        state = task_state.load_state(task_id) or {}
        return str(state.get("status", "")).upper() == "DONE"
    except Exception:
        return False


def autofill_for_task(task_id: str, body: dict) -> dict:
    """done 回调自动回填入口（幂等；任何失败降级 skip 不抛出语义）。

    Returns: {"task_id", "filled", "candidates", "skipped"}——skipped 非空时
    给出人可读原因（日志观测用）。
    """
    ret = {"task_id": task_id, "filled": 0, "candidates": 0, "skipped": ""}
    if os.environ.get("NEXUS_OUTCOME_AUTOFILL", "0") != "1":
        ret["skipped"] = "flag off"
        return ret
    result = _decide_result(body)
    if result is None:
        ret["skipped"] = "tests 非全过或缺失（保守不回填，留人工）"
        return ret
    if not _task_is_done(task_id):
        ret["skipped"] = "task 未到终态 DONE（单查未命中，本轮放弃）"
        return ret
    try:
        attempts = _pending_attempts(task_id)
    except Exception as e:
        ret["skipped"] = f"attempt 查询失败: {type(e).__name__}: {e}"
        return ret
    if not attempts:
        ret["skipped"] = "无 pending attempt"
        return ret
    try:
        from nexusos_mem.apply_honesty import is_real_apply
    except ImportError:
        # 口径真源不可用——宁可不填也不裸填（第二口径=自创判定，违反单一真源）
        ret["skipped"] = "is_real_apply 不可用（口径真源缺失，宁可不填）"
        return ret
    real = [a for a in attempts if a.get("target_id") and is_real_apply(a)]
    ret["candidates"] = len(real)
    if not real:
        ret["skipped"] = (
            f"{len(attempts)} 条 pending 均无产物引用（查证式命中，不回填）"
        )
        return ret
    try:
        from nexusos_mem.shared import SharedKnowledge
        # agent_activity.agent_id varchar(32)——超长截断防 INSERT 报错
        agent_id = (str(body.get("source") or body.get("session") or "unknown"))[:32]
        kb = SharedKnowledge(agent_id=agent_id)
    except Exception as e:
        ret["skipped"] = f"SharedKnowledge 初始化失败: {type(e).__name__}: {e}"
        return ret
    tests = body.get("tests") or {}
    evidence = (
        f"autofill by done-callback task_id={task_id}: "
        f"tests {tests.get('passed', '?')} 全过 + report 引用判真套用（Task1450）"
    )
    for attempt in real[:MAX_AUTOFILL]:
        try:
            kb.record_apply_outcome(
                apply_attempt_id=attempt["id"],
                exp_id=attempt["target_id"],
                result=result,
                evidence=evidence,
            )
            ret["filled"] += 1
        except Exception as e:
            # 单条失败不中断整批（其余 attempt 不陪葬）
            logger.warning(
                "[Task1450][autofill] %s attempt=%s 回填失败(继续): %s",
                task_id, attempt["id"], e,
            )
    return ret
