#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""nexusos_xdr.callback.handler — done 回调域（Task618 #57 主线批，自 mcp_server.py 迁入）。

物理迁入（行为零变化——函数体原样搬家，T609 纪律：只搬家不整形容）：
    http_callback 主链 + _bg_gate_check 后台线程 + 清算逻辑（auto-relocate/
    experiences 写 MEM/apply_candidates 钩子/Phase2 WARN）。

架构（T604 三要素复用）：
    - register_routes(mcp) 显式注册 /callback POST（mcp_server 启动处调用）。
    - 依赖方向：callback → dispatchd/task_state/spill 正向 import ✅（无环）。
    - Task492 线程语义保持：bg-gate daemon 线程启动时机/参数一个字符未动。

陷阱 2 合规：logger 名保持 nexusos-xdr.mcp（M8 按格式解析 error.log——
迁移不改 logger 名，避免观测层解析断链）。
"""
import logging
import os
import re
import sys
import threading
import psycopg2

import anyio
from starlette.requests import Request
from starlette.responses import JSONResponse

from nexusos_xdr import dispatchd
from nexusos_common import subpath  # 1960：锚点单源（design §2.1 行10）
from nexusos_xdr import spill  # Task311 S1: callback 大 summary 外溢
from nexusos_xdr import task_state
from nexusos_xdr._vault_env import VAULT_ROOT  # Task668: 包内自足（干净环境可解析）

logger = logging.getLogger("nexusos-xdr.mcp")  # 陷阱2: 保持原 logger 名（M8 解析）

# Task639 X1: 包路径 import（经 nexusos_xdr/__init__ 的 __path__ append 恒可达），
# 不再依赖"mcp_server 先 import"的 sys.path 副作用——裸名 import 在子包被单独
# 首载时走降级分支静默跳过 gate（fail-open，诚实体系问题）。包真损时
# ImportError 响亮失败（fail-closed），不再存在不可达的降级路径。
from nexusos_gate.gate_engine import check as gate_engine_check  # Task668 ②: 正源在 nexusos-gate 包


def register_routes(mcp) -> None:
    """在 mcp 实例上显式注册 callback 域路由（/callback POST）。

    契约：mcp_server 启动处调用一次（mcp.run 前）；🔴 非幂等（重复调用
    双注册），调用方保证单次。并发语义：仅注册期执行。
    """
    mcp.custom_route("/callback", methods=["POST"])(http_callback)


async def http_callback(request: Request) -> JSONResponse:
    """小弟 done 回调入口（Task 277 P1-1：替代 19999 /report，走 task_state 单一真相源）。

    Body（兼容 19999 /report 格式，零改动迁移）:
        {
            "task_id": "...",
            "source": "agent-a",  # 执行方 tool 名
            "session": "agent-a",
            "status": "done" | "failed" | "progress" | "started" | "need_decision",
            "summary": "...",
            "files": ["..."],
            "tests": {"passed": N, "failed": N}
        }

    status 大小写都接受（task_state 用大写 DONE/FAILED，小弟回调常用小写）。
    """
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "无效 JSON"}, status_code=400)

    task_id = body.get("task_id")
    if not task_id:
        return JSONResponse({"error": "缺少 task_id"}, status_code=400)
    # LOOP-4修复(2026-07-31): task_id 格式校验(防路径遍历/特殊字符致500)。
    # 与 task_state._TASK_ID_RE 一致: 只允许字母数字下划线连字符(xdr-oc-1785-abc 格式)。
    # 原版未校验,../../etc/passwd 触发 _validate_task_id ValueError 冒泡成 500。
    if not isinstance(task_id, str) or len(task_id) > 100 or not re.match(r'^[a-zA-Z0-9_\-]+$', task_id):
        return JSONResponse({"error": "task_id 格式非法（只允许字母数字下划线连字符，≤100字符）"}, status_code=400)
    # source/session 基本校验(只防异常输入,不限制字符集——它们进参数化SQL/JSON不进路径)
    source = str(body.get("source", "") or "")
    session = str(body.get("session", "") or "")
    if len(source) > 50 or len(session) > 50:
        return JSONResponse({"error": "source/session 过长"}, status_code=400)

    # 大小写兼容
    status = str(body.get("status", "")).lower()
    summary = body.get("summary", "")
    files = body.get("files", [])

    # === Task311 S1: callback 大 summary 自动外溢（spill-to-disk）===
    # 单点截获（309 §2.1）：done result(:1119)/FAILED error(:1378)/callbacks[]
    # （append_callback 存 body）/done gate/_notify_caller 转发全部消费截断后文本。
    # spill.maybe_spill 内部 fail-open（任何异常 warning+原文），回调必 200。
    if "summary" in body:
        summary = spill.maybe_spill(summary, task_id=task_id, agent=source)
        body["summary"] = summary

    # 写 task_state（单一真相源）
    state = await anyio.to_thread.run_sync(lambda: task_state.load_state(task_id))
    if state is None:
        return JSONResponse({"error": f"任务不存在: {task_id}"}, status_code=404)

    if status in ("done", "completed", "success"):
        # === Task393 P1: 凭据校验 fail-closed（无凭据新task→真403）===
        # P0 是 fail-open warn 观察期。P1 升级为 fail-closed 强制。
        # 紧急开关：CRED_FAIL_OPEN=1 临时退回 fail-open（防凭据 bug 阻断所有 task）
        # 老 task 兼容：credential_id NULL + created 7 天内 → warn 放行（超 7 天 403）
        # Task532 P-d4 设计妥协标注：此处只校验 task 名下凭据（dispatch 时签发给该
        # task），不校验"请求方"身份——知道 task_id 的进程可 POST done 是已接受的
        # 妥协（回调方是小弟 session 本身，无独立凭据链路；升级需 done_cred 签发点）
        import os as _os_p1
        _cred_fail_open = _os_p1.environ.get("CRED_FAIL_OPEN", "0") == "1"
        _cred_id = state.get("credential_id") if state else None
        _task_created = state.get("created", "") if state else ""
        if _cred_id:
            # 新 task（P1 后 dispatch）：凭据校验
            try:
                from nexusos_xdr.credential import verify_credential
                _valid, _detail = verify_credential(_cred_id, "dispatch_cred")
                if not _valid:
                    if _cred_fail_open:
                        logger.warning(f"[Task393] {task_id} 凭据校验失败(CRED_FAIL_OPEN=1): {_detail}")
                    else:
                        logger.warning(f"[Task393] {task_id} 凭据校验失败→403: {_detail}")
                        return JSONResponse({
                            "ok": False, "rejected": True,
                            "reason": f"凭据校验失败: {_detail}",
                            "msg": "凭据无效或被篡改。请通知主控重新 dispatch。",
                        }, status_code=403)
                else:
                    logger.info(f"[Task393] {task_id} 凭据校验通过: {_detail}")
            except Exception as _ce:
                if _cred_fail_open:
                    logger.warning(f"[Task393] {task_id} 凭据校验异常(CRED_FAIL_OPEN=1): {_ce}")
                else:
                    logger.warning(f"[Task393] {task_id} 凭据校验异常→403: {_ce}")
                    return JSONResponse({
                        "ok": False, "rejected": True,
                        "reason": f"凭据校验异常: {_ce}",
                    }, status_code=403)
        else:
            # 老 task（credential_id NULL）：7 天兼容期判断
            import datetime as _dt
            try:
                _created_dt = _dt.datetime.fromisoformat(_task_created) if _task_created else None
                if _created_dt and _created_dt.tzinfo is None:
                    _created_dt = _created_dt.replace(tzinfo=_dt.timezone.utc)
                _age_days = (_dt.datetime.now(_dt.timezone.utc) - _created_dt).total_seconds() / 86400 if _created_dt else 999
            except Exception:
                _age_days = 999  # 无法解析 created → 按超期处理
            if _age_days <= 7:
                logger.warning(f"[Task393] {task_id} 老task无凭据(age={_age_days:.1f}d≤7d兼容期)→放行")
            elif _cred_fail_open:
                logger.warning(f"[Task393] {task_id} 老task无凭据(age={_age_days:.1f}d>7d但CRED_FAIL_OPEN=1)→放行")
            else:
                logger.warning(f"[Task393] {task_id} 老task无凭据(age={_age_days:.1f}d>7d兼容期)→403")
                return JSONResponse({
                    "ok": False, "rejected": True,
                    "reason": f"老task兼容期已过({ _age_days:.1f}d>7d), 无凭据",
                    "msg": "凭据体系已强制生效。请重新 dispatch 获取凭据。",
                }, status_code=403)

        result = {
            "summary": summary,
            "method": "callback",
            "files": files,
            "source": body.get("source", ""),
            "session": body.get("session", ""),
        }
        if "tests" in body:
            result["tests"] = body["tests"]
        # Task 350: experiences 字段处理（链路③ 知识共享）
        # 有则写 MEM + 计数，无则标记 missing（不阻断，主控 GET /task/{id} 可见）
        experiences = body.get("experiences", [])
        result["experiences_count"] = len(experiences)
        result["experiences_missing"] = len(experiences) == 0
        # T657 修4: files[] 幽灵文件自检（记录不阻断——Task492 口径）。
        # vault 根相对拼接（648 实证 files[] 惯例是 Vault 根相对路径）；
        # 空数组同样记 files_ghost warning。结构化字段 result.files_ghost
        # / files_empty 供 Console 决策队列（647）消费。
        _ghost_files = []
        for _fp657 in files:
            _p657 = str(_fp657)
            _cands657 = [_p657] if os.path.isabs(_p657) else [_p657, str(VAULT_ROOT / _p657)]
            if not any(os.path.exists(_c) for _c in _cands657):
                _ghost_files.append(_p657)
        if _ghost_files:
            result["files_ghost"] = _ghost_files
            try:
                if task_state.once_flag(task_id, "files_ghost"):
                    task_state.append_warning(
                        task_id, f"files_ghost: {_ghost_files[:5]}（共 {len(_ghost_files)} 条不存在）")
            except Exception:
                pass
        elif not files:
            result["files_empty"] = True
            try:
                if task_state.once_flag(task_id, "files_ghost"):
                    task_state.append_warning(task_id, "files_ghost: []（回调 files 空数组）")
            except Exception:
                pass
        # Task492: gate 移出 callback 热路径——后台异步跑，不阻塞 callback 响应。
        # 原 Task365 gate 在 callback 热路径阻塞 5-10s（G5 mem_queried 调 api/embed），
        # 多 callback 并发 → 线程池被阻塞操作占满 → xdr 死锁。
        # 现在：callback 立即标 DONE + 返 ACK，gate 后台 daemon 线程跑，
        # gate 结果记 task_state warnings（主控查 /task/{id} 可见）。
        # trade-off：gate 从"REJECT 阻断 DONE"降级为"WARN 记录不阻断"——
        # callback 死锁比个别谎报没被 gate 拦严重得多。gate 拦谎报移到主控验收。
        def _bg_gate_check(_task_id, _body, _state, _files, _summary):
            """后台 gate 检查，结果记 warnings（不阻断 DONE）。"""
            try:
                # Task639 C4: 线程参数 _state 是 done 时刻快照——gate 排队期间
                # patrol 可能已改写 task_state。重新 load 用最新态（None 时回退快照）。
                _state = task_state.load_state(_task_id) or _state
                _task_path = _state.get("task_path", "")
                _report_path = _task_path.replace("input/task.md", "output/report.md")
                if "input/" in _report_path and "/output/" not in _report_path:
                    import re as _re_bg
                    _report_path = _re_bg.sub(r"input/[^/]+$", "output/report.md", _report_path)
                if _report_path.startswith("task/") or not os.path.isabs(_report_path):
                    _report_full = str(VAULT_ROOT / _report_path.lstrip("/"))
                else:
                    _report_full = _report_path
                _gate_ctx_bg = {
                    "task_id": _task_id,
                    "task_path": _task_path,
                    "task_path_report": _report_full,
                    "files": _files,
                    "summary": _summary,
                    "experiences": _body.get("experiences", []),
                    # Task778: source 注入 gate context——done.yaml G13 模板 {source}
                    # 缺键致 _resolve_template KeyError 静默原串 → /bin/sh bad substitution。
                    "source": _body.get("source", ""),
                }
                _ge = gate_engine_check("done", _gate_ctx_bg,
                                        agent=(_state.get("callee") or _body.get("source", "")))
                if not _ge.get("passed"):
                    _fails = [f"{r['id']}_{r.get('severity','WARN')}: {r.get('detail','')}"
                              for r in _ge.get("failures", []) if r.get("status") == "fail"]
                    logger.warning("[Task492][bg-gate] %s gate 未过(已标DONE,记录warning): %s", _task_id, _fails)
                    # Task531 B4: G10 fail 显著 log——后台 gate 不阻断 done（Task492
                    # 设计），但 outcome 未回填需主控看见（原混在普通 warning 里易漏）
                    _g10_fails = [r for r in _ge.get("failures", [])
                                  if r.get("id") == "G10" and r.get("status") == "fail"]
                    for _g10 in _g10_fails:
                        logger.warning("[G10 OUTCOME] %s 认知诚实未闭环: %s",
                                       _task_id, _g10.get("detail", ""))
                    try:
                        # T657 修1: warning 去重——同组 fail 只 append 一次
                        # （653 实证同一条 gate 未过 warning 出现两次）
                        _sig657 = "gate_warn:" + ",".join(
                            sorted(f.split(":")[0].strip() for f in _fails))
                        if task_state.once_flag(_task_id, _sig657):
                            task_state.append_warning(_task_id, f"gate未过: {'; '.join(_fails[:3])}")
                    except Exception:
                        pass  # append_warning 可能不存在，只 log
                else:
                    logger.info("[Task492][bg-gate] %s gate 通过", _task_id)
                # Task1450: outcome 自动回填——gate 异步尾巴执行（MEM#70646 防
                # 再踩：不进回调热路径、gate 完成后跑、模块内幂等双守卫）。
                # flag 默认 off（灰度由主控定）；off 时零 import 零行为变化。
                if os.environ.get("NEXUS_OUTCOME_AUTOFILL", "0") == "1":
                    try:
                        from nexusos_xdr.callback.outcome_autofill import autofill_for_task
                        _af_r = autofill_for_task(_task_id, _body)
                        logger.info("[Task1450][autofill] %s -> %s", _task_id, _af_r)
                    except Exception as _af_e:
                        logger.warning("[Task1450][autofill] %s 异常(不阻塞): %s", _task_id, _af_e)
            except Exception as _ge_err:
                logger.warning("[Task492][bg-gate] %s gate 后台检查异常(不阻塞): %s", _task_id, _ge_err)
        threading.Thread(target=_bg_gate_check, args=(task_id, body, state, files, summary), daemon=True).start()

        # === Task412: 通电逻辑提前到 update_status 之前 ===
        # 原因：patrol 可能抢先标 DONE → update_status 失败 → _update_failed=True →
        # 后续代码仍执行但 record_apply_outcome 被跳过/异常路径未到。
        # 修复：通电逻辑独立于 update_status 结果，在 update 之前执行。
        try:
            from nexusos_mem.shared import SharedKnowledge as _SK_s6
            # Task531 B1: 直接读 search annotate 写的 apply_attempt（带 task_id，
            # Task526 落库）。原读 _get_apply_candidates_for_task（apply_candidates）
            # 但 Task326 在 callback 流程更晚写入——首次 done 时序倒挂致空转。
            import psycopg2 as _pg_b1
            # Task627: T628 C2 加防御初始化时误删了本行（git dd7dfcd 的 -diff 实证；
            # 缺失致 _conn_b1.cursor() 每 done 必 NameError 被 except 吞——Task531 B1
            # 通电回填静默失效）。按 T618 前身 mcp_server:1193 原文恢复。
            _conn_b1 = _pg_b1.connect("dbname=nexus_mem")
            _rows_b1 = []  # T628 C2: 防御性初始化——fetchall 前异常时兜底空集（异常本就被外层 except 接住，此处让数据流自洽无 NameError 可能）
            try:
                _cur_b1 = _conn_b1.cursor()
                # Task1269 修复：通电只给 outcome IS NULL 的 attempt 补 pending
                # 占位——原无条件覆盖会把已回填的真实 outcome（success/
                # not_applicable）打回 pending，形成「回填→done 回调→auto
                # 覆盖→gate G10 永 fail」死循环（1269 验收实测两轮复现）
                _cur_b1.execute(
                    "SELECT id, target_id FROM agent_activity "
                    "WHERE action='apply_attempt' AND task_id=%s "
                    "AND outcome IS NULL",
                    (task_id,),
                )
                _rows_b1 = _cur_b1.fetchall()
            finally:
                _conn_b1.close()
            _cands_s6 = [{"exp_id": _r[1], "attempt_id": _r[0]} for _r in _rows_b1 if _r[1]]
            if _cands_s6:
                _src_s6 = body.get("source") or body.get("session") or "unknown"
                _kb_s6 = _SK_s6(agent_id=_src_s6)
                _ok_s6 = 0
                for _c in _cands_s6[:10]:
                    try:
                        # attempt 已在 search 时 annotate 过——直接补 pending 占位
                        _kb_s6.record_apply_outcome(
                            apply_attempt_id=_c["attempt_id"],
                            exp_id=_c["exp_id"],
                            result="pending",
                            evidence=f"auto by callback task_id={task_id}",
                        )
                        _ok_s6 += 1
                    except Exception as _ae:
                        logger.warning(f"[Task412] pending 回填 exp={_c.get('exp_id')} 失败: {_ae}")
                logger.info(f"[Task412] {task_id} 通电 pending 回填 {_ok_s6}/{len(_cands_s6)}（读 apply_attempt，B1）")
        except psycopg2.OperationalError as _oe:
            # T628 C2: PG 断连/不可达单独分级——通电（认知闭环占位）整体失效，
            # 属体系级故障必须可见（warning + I8 可观测），非静默 debug。
            logger.warning(
                f"[Task412] {task_id} 通电失败（PG 不可达，认知闭环占位本轮失效）: {_oe}"
            )
        except Exception as _e:
            # T628 C2: 其余异常（查询/回填逻辑）从 debug 提到 warning——通电静默
            # 失效曾致 1053 条 NameError 无可见性（T625 审计），降级语义保留
            # （不阻塞 done），但每类失败必须留 warning 级日志。
            logger.warning(f"[Task412] {task_id} 通电失败（不阻塞 done，已降级）: {type(_e).__name__}: {_e}")

        _update_failed = False
        _merged_late = False
        try:
            await anyio.to_thread.run_sync(lambda: task_state.update_status(task_id, "DONE", result=result, end_reason="completed"))
        except ValueError as e:
            _update_failed = True
            logger.warning(f"[callback] {task_id} update_status DONE 失败: {e}")
            # === Task576 例③: 终态吸收——迟到 done 合并非丢弃 ===
            # Task562 实锤: patrol 先标 TIMEOUT 后真 done 到达，原逻辑只记 WARNING，
            # result 丢失、轮询方永远等不到。现走 merge_late_result 合并进终态记录
            # （status 不变 / result 填充 / end_reason=done_after_timeout / done_late 标志）。
            try:
                _merged = await anyio.to_thread.run_sync(
                    lambda: task_state.merge_late_result(task_id, result=result)
                )
                _merged_late = True
                # T1556 B8: 重复 done（原终态 DONE）走字段级合并保第一次产出，
                # 日志区分重复与真迟到（duplicate_done），不再统一称"迟到"。
                _dup = bool(_merged.get("duplicate_done"))
                logger.info(
                    f"[Task576] {task_id} {'重复' if _dup else '迟到'} done 已合并"
                    f"（原终态 {_merged.get('done_late_from_status')}, "
                    f"end_reason={_merged.get('end_reason')}, "
                    f"done_late=True, duplicate_done={_dup}）"
                )
            except ValueError as _me:
                logger.warning(f"[Task576] {task_id} 迟到 done 合并失败: {_me}")
        await anyio.to_thread.run_sync(lambda: task_state.append_callback(task_id, body))

        # === Task496: auto-relocate 外迁大文件（DONE 公共主路径）===
        # 原 dispatchd.py 只在 wait=True DONE 触发，wait=False/其他终态永不外迁。
        # http_callback 覆盖两种 wait 模式 + 小弟主动 done，是真正的公共 DONE 点。

        # 🔴 不用 Path：本函数后面有局部 from pathlib import Path（:1113），
        #    Python 作用域规则致整个函数的 Path 变局部变量，此处用会 UnboundLocalError。
        # 🔴 两级 dirname：task_path = <root>/input/task.md，任务根 = dirname(dirname())。
        # Task576 例③: 合并成功（_merged_late）也触发外迁——迟到 done 的产出文件
        # 真实存在，不应因先 TIMEOUT 就永不外迁。
        if not _update_failed or _merged_late:
            try:
                _st = task_state.load_state(task_id) or {}
                _tp = _st.get("task_path")
                if _tp:
                    _task_dir = os.path.dirname(os.path.dirname(_tp))
                    await anyio.to_thread.run_sync(lambda _d=_task_dir: dispatchd.trigger_auto_relocate(_d))
            except Exception as _re:
                logger.warning(f"[Task496] {task_id} auto-relocate 触发失败（不阻塞 done）: {_re}")

        # === Task 350: experiences 自动写入 MEM（链路③ 知识共享基础设施）===
        # 有 experiences 则写 MEM（让小弟发的经验真进库，其他 Agent 立即可见）；
        # 无则已在 result 标 experiences_missing=true（不阻断 done，主控可见）。
        # 陷阱1：MEM 写入失败绝不阻断 callback（MEM 挂了不能影响 task 闭环）。
        # 陷阱2：tags 兼容逗号字符串和数组两种格式。knowledge_type 默认 experience。
        if experiences:
            import urllib.request as _urlreq
            import json as _json350
            _write_ok = 0
            _write_fail = 0
            _src = body.get("source") or body.get("session") or "unknown"
            for exp in experiences:
                _tags = exp.get("tags", [])
                if isinstance(_tags, str):
                    _tags = [t.strip() for t in _tags.split(",") if t.strip()]
                _exp_body = _json350.dumps({
                    "agent_id": _src,
                    "title": exp.get("title", "无标题"),
                    "content": exp.get("content", exp.get("detail", "")),
                    "knowledge_type": exp.get("knowledge_type", "experience"),
                    "tags": _tags,
                }).encode()
                _req = _urlreq.Request(
                    "http://127.0.0.1:11537/knowledge",
                    data=_exp_body, method="POST",
                    headers={"Content-Type": "application/json"},
                )
                try:
                    # Task494: urlopen 包在 run_sync lambda 里（AST 可见 run_sync 上下文）
                    _resp_raw = await anyio.to_thread.run_sync(lambda _r=_req: _urlreq.urlopen(_r, timeout=5).read())
                    _resp = _json350.loads(_resp_raw) if _resp_raw else {}
                    if isinstance(_resp, dict) and _resp.get("ok", True) is not False:
                        _write_ok += 1
                    else:
                        _write_fail += 1
                        logger.warning(f"[Task350] {task_id}: experience 写入 MEM 返回非 ok: {_resp_raw[:100]}")
                except Exception as _werr:
                    _write_fail += 1
                    logger.warning(f"[Task350] {task_id}: experience 写入 MEM 异常: {_werr}")
            result["experiences_written"] = _write_ok
            result["experiences_write_failed"] = _write_fail
            logger.info(f"[Task350] {task_id}: experiences 写入 MEM 成功 {_write_ok}/{len(experiences)} (source={_src}, 失败 {_write_fail})")
        else:
            logger.info(f"[Task350] {task_id}: done 无 experiences（experiences_missing=true）")

        # 🔴 Task 326 试运行修复：callback 是小弟 done 的真实主路径
        # 原 patrol _mark_done 钩子只在 patrol 检测到完成时触发，
        # 但小弟主动调 callback 时直接 update_status，不经过 _mark_done。
        # 钩子必须在 http_callback 也触发（双重保险，与 patrol 钩子等价）。
        try:
            from nexusos_xdr.dispatchd import apply_capture_enabled, _capture_apply_candidates
            if apply_capture_enabled():
                callee = body.get("source") or body.get("session") or "unknown"
                _capture_apply_candidates(task_id=task_id, agent_id=callee)
                # 清理上下文文件
                from pathlib import Path
                ctx_file = Path(os.environ.get("NEXUS_TMP_DIR") or subpath("tmp")) / f"nexus_current_task_{callee}.txt"
                if ctx_file.exists():
                    ctx_file.unlink()
        except Exception as _e:
            logger.warning(f"[Task326] callback apply_candidates 钩子异常（不阻塞）: {_e}")

        # === Task 329: Phase 2 WARN 模式（软强制，不阻塞 done）===
        # 触发条件：task 有 apply_candidates（候选>0）但无 apply_attempt（主控未补标）
        # 处置：放行 done，但 logger.warning 提醒 + 写 action='phase2_warn' 记录供统计 WARN 率
        # PASS 分支：无 apply_candidates 的纯执行任务，不输出噪音（默认 PASS）
        # BLOCK 模式不实现（等 4 周 KPI 评估后切 BLOCK，DS 318 建议）
        try:
            from nexusos_xdr.dispatchd import phase2_warn_enabled
            if phase2_warn_enabled():
                from nexusos_mem.shared import SharedKnowledge as _SK_warn
                _kb_warn = _SK_warn(agent_id="phase2-warn")  # 陷阱1：http_callback 无 kb，新建专用实例
                _cur_warn = _kb_warn._get_conn().cursor()
                # 陷阱2：参数化查询，不拼字符串。一次查候选数 + 补标数
                # Task581 ④: 候选源从 apply_candidates（dispatchd done 钩子写，时序晚
                # 于本检查，首次 done 空转）切到 apply_attempt（search annotate 实时写，
                # 与 Task412 通电/Task531 B1 同源）——消除双源漂移。补标判定同步升级：
                # 原「有 apply_attempt 行」在 Task412 通电后恒真（自动写行）失去区分度，
                # 改为「有非 pending 的 apply_outcome 回填」（pending=通电占位非补标）。
                _cur_warn.execute(
                    """
                    SELECT
                        (SELECT COUNT(*) FROM agent_activity
                         WHERE action='apply_attempt' AND task_id=%s) AS cand,
                        (SELECT COUNT(*) FROM agent_activity ao
                         JOIN agent_activity aa
                           ON aa.id::text = ao.detail->>'apply_attempt_id'
                           AND aa.action = 'apply_attempt'
                         WHERE ao.action='apply_outcome'
                           AND aa.task_id=%s
                           AND COALESCE(ao.detail->>'result','') != 'pending') AS annotated
                    """,
                    (task_id, task_id),
                )
                _cand_count, _annotated_count = _cur_warn.fetchone()
                _cur_warn.close()
                # WARN 条件：有套用尝试 + 无一真实 outcome 回填
                if _cand_count > 0 and _annotated_count == 0:
                    logger.warning(
                        f"[Task329][Phase2-WARN] {task_id} 有 {_cand_count} 条 apply_attempt 但无一真实 outcome 回填！"
                        f"请回填（apply_experience_outcome，候选见 apply_attempt 记录，remind-annotate.sh {task_id}）"
                    )
                    # 写 action='phase2_warn' 记录供统计 WARN 率（DS 318: quality_metrics 量化）
                    _kb_warn.log_activity(
                        action="phase2_warn",
                        target_type="task",
                        target_id=None,
                        task_id=task_id,
                        detail={
                            "candidate_count": _cand_count,
                            "annotated_count": _annotated_count,
                            "agent": callee,
                            "source": "apply_attempt",  # Task581 ④: 已切源
                        },
                    )
                # else: 无候选=PASS（纯执行任务，不输出噪音）；已有补标=已完成，也不输出
        except Exception as _e:
            # 陷阱3：WARN 检查绝不阻塞 done，失败只 debug log
            logger.debug(f"[Task329] WARN 检查异常（不阻塞）: {_e}")

        # Task412: 通电逻辑已移到 update_status 之前（L965 区域），此处不再重复执行

        # B3 修复续: update_status 失败时返回真实状态(非假 DONE),让小弟知道
        # Task576 例③: 合并成功时如实返回 DONE_LATE（done 事实已入终态记录）
        if _merged_late:
            return JSONResponse({
                "ok": True, "task_id": task_id, "status": "DONE_LATE",
                "warning": "任务已先终态化(如 TIMEOUT)，迟到 done 已合并: "
                           "result 已填充, end_reason=done_after_timeout, done_late=True。"
                           "主控 GET /task 可见完整产出。",
            })
        if _update_failed:
            _real_state = task_state.load_state(task_id) or {}
            _real_status = _real_state.get("status", "UNKNOWN")
            logger.warning(f"[callback] {task_id} 返回真实状态 {_real_status} (update 失败)")
            return JSONResponse({
                "ok": False, "task_id": task_id, "status": _real_status,
                "warning": f"update_status 失败,任务当前状态为 {_real_status}(可能是 patrol 已先标终态)。callback 已记录。",
            })
        return JSONResponse({"ok": True, "task_id": task_id, "status": "DONE"})

    elif status in ("failed", "error"):
        try:
            await anyio.to_thread.run_sync(lambda: task_state.update_status(task_id, "FAILED", error=summary, end_reason="error"))
        except ValueError as e:
            logger.warning(f"[callback] {task_id} update_status FAILED 失败: {e}")
        await anyio.to_thread.run_sync(lambda: task_state.append_callback(task_id, body))
        return JSONResponse({"ok": True, "task_id": task_id, "status": "FAILED"})

    elif status in ("progress", "started", "need_decision"):
        # 非终态——只 append_callback 不改状态机
        # T628 C1: 来源校验对齐 done 路径的 fail-closed 模式——body.source 必须与
        # task_state.callee 一致（小弟回调链真实流量即如此：dispatch 注册 callee=
        # 小弟 tool 名，回调 source 同名）。伪造者可猜 task_id 但对不上 callee。
        # 校验失败 403（与 search 凭据 403 同语义），不 append（防 callbacks[] 膨胀）。
        _callee = state.get("callee") if state else None
        if _callee and source and source != _callee:
            logger.warning(
                f"[callback] {task_id} 非终态 {status} 来源校验失败: source={source!r} != callee={_callee!r}，拒绝"
            )
            return JSONResponse(
                {"error": f"source 与任务 callee 不符（非终态回调校验，T628）"},
                status_code=403,
            )
        # T628 C1: callbacks[] 长度上限（防膨胀——非终态无自然上限，恶意/失控小弟
        # 可灌爆 state 文件）。超限拒新（历史保留），返回 429 语义（上限可调）。
        _MAX_NONTERMINAL_CALLBACKS = 200
        # T1556 B3: 预判保留作快速路径（旧快照已超限直接 429，省一次锁竞争）；
        # 最终判定在 append_callback 锁内原子复核——两个并发回调同读旧快照
        # 199 双双通过预判的击穿窗口由锁内复核封死。
        _cb_count = len(state.get("callbacks") or []) if state else 0
        if _cb_count >= _MAX_NONTERMINAL_CALLBACKS:
            logger.warning(
                f"[callback] {task_id} callbacks[] 达上限 {_MAX_NONTERMINAL_CALLBACKS}，拒绝追加（防膨胀）"
            )
            return JSONResponse(
                {"error": f"callbacks 超上限 {_MAX_NONTERMINAL_CALLBACKS}（T628 防膨胀）"},
                status_code=429,
            )
        try:
            await anyio.to_thread.run_sync(
                lambda: task_state.append_callback(
                    task_id, body, max_callbacks=_MAX_NONTERMINAL_CALLBACKS
                )
            )
        except task_state.CallbackLimitError as _cle:
            logger.warning(
                f"[callback] {task_id} {_cle}，拒绝追加（锁内复核，T1556 B3）"
            )
            return JSONResponse(
                {"error": f"callbacks 超上限 {_MAX_NONTERMINAL_CALLBACKS}（锁内复核，T1556 B3）"},
                status_code=429,
            )
        # Task909（904 设计 A 案）：need_decision 即时通知——fire-and-forget
        # 后台线程，通知层异常绝不进 callback 热路径（防轰炸①：仅
        # need_decision 推，progress/started 不进；notify_push 降级链 677 继承）。
        if status == "need_decision":
            try:
                from nexusos_xdr import needdecision_notify
                needdecision_notify.fire(task_id, summary or "", source=source or "")
            except Exception:
                logger.warning(f"[Task909] {task_id} 通知触发失败（不阻塞回调）",
                               exc_info=True)
        return JSONResponse({"ok": True, "task_id": task_id, "status": status.upper()})

    else:
        return JSONResponse({"error": f"未知 status: {status}"}, status_code=400)
