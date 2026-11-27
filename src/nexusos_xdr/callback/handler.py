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
from pathlib import Path  # Task 2611230853 件四：自动补齐要列任务目录里的 .md
from nexusos_xdr import spill  # Task311 S1: callback 大 summary 外溢
from nexusos_xdr.task_aliases import task_id_aliases
from nexusos_xdr import task_state
from nexusos_xdr._vault_env import VAULT_ROOT  # Task668: 包内自足（干净环境可解析）

logger = logging.getLogger("nexusos-xdr.mcp")  # 陷阱2: 保持原 logger 名（M8 解析）

# Task639 X1: 包路径 import（经 nexusos_xdr/__init__ 的 __path__ append 恒可达），
# 不再依赖"mcp_server 先 import"的 sys.path 副作用——裸名 import 在子包被单独
# 首载时走降级分支静默跳过 gate（fail-open，诚实体系问题）。包真损时
# ImportError 响亮失败（fail-closed），不再存在不可达的降级路径。
from nexusos_gate.gate_engine import check as gate_engine_check  # Task668 ②: 正源在 nexusos-gate 包


async def http_callback_cred_stats(request: Request) -> JSONResponse:
    """GET /callback_cred_stats — 回调凭据闸门的只读状态（2611231133）。

    返回：当前模式（record/enforce）+ 最近 24 小时按来源（回调的 source 字段）的
    「已核 / 缺 / 错」三类计数。只读：不改开关、不写任何东西。
    可选 ?hours=N（1–168）调整窗口，默认 24。
    """
    try:
        from nexusos_xdr.callback import cred_gate as _cg
        hours = 24
        try:
            _qp = getattr(request, "query_params", None)
            _raw = _qp.get("hours") if _qp is not None else None
            if _raw:
                hours = max(1, min(int(_raw), 168))
        except Exception:  # noqa: BLE001 — 参数坏了按默认窗口
            hours = 24
        return JSONResponse(_cg.stats(hours=hours))
    except Exception as e:  # noqa: BLE001
        logger.warning(f"[2611231133] 状态接口异常: {type(e).__name__}: {e}")
        return JSONResponse({"ok": False, "error": f"状态接口异常: {type(e).__name__}"},
                            status_code=500)


def register_routes(mcp) -> None:
    """在 mcp 实例上显式注册 callback 域路由（/callback POST）。

    契约：mcp_server 启动处调用一次（mcp.run 前）；🔴 非幂等（重复调用
    双注册），调用方保证单次。并发语义：仅注册期执行。
    """
    mcp.custom_route("/callback", methods=["POST"])(http_callback)
    # 2611231133：回调凭据闸门的只读状态接口（模式 + 最近 24h 按来源的三类计数）
    mcp.custom_route("/callback_cred_stats", methods=["GET"])(http_callback_cred_stats)



def _autofix_registration(task_id: str, task_path: str, files: list, status: str) -> list:
    """Task 2611230853 件四：完成门禁前自动补齐登记类内容（产出登记段 / 报告机读块）。

    返回「补了什么」的说明列表，调用方逐条写进 task warnings（auto-fixed: …）。
    任何一步失败只返回说明、不抛——自动补齐不能影响门禁与回调。
    """
    notes = []
    try:
        base = Path(task_path)
        if base.name == "input":
            base = base.parent
        if not base.is_dir():
            return notes
        # ① 产出登记：任务目录下全部 .md（排除 dispatched 副本）都要以笔记链接形式列出
        task_md = base / "input" / "task.md"
        if task_md.is_file():
            mds = [p for p in sorted(base.rglob("*.md"))
                   if not p.name.startswith("task.dispatched.")]
            text = task_md.read_text(encoding="utf-8", errors="replace")
            missing = []
            for p in mds:
                rel = p.relative_to(base).as_posix()
                link = f"[[task/{base.name}/{rel}]]"
                if link in text or rel in text:
                    continue
                missing.append(link)
            if missing:
                lines = text.splitlines()
                head = None
                for i, ln in enumerate(lines):
                    if ln.lstrip().startswith("#") and "产出登记" in ln:
                        head = i
                        break
                if head is None:
                    text = text.rstrip("\n") + "\n\n## 产出登记\n\n" + "\n".join(f"- {m}" for m in missing) + "\n"
                else:
                    end = len(lines)
                    for j in range(head + 1, len(lines)):
                        if lines[j].startswith("#"):
                            end = j
                            break
                    block = [f"- {m}" for m in missing]
                    while end > head + 1 and not lines[end - 1].strip():
                        end -= 1
                    lines[end:end] = block
                    text = "\n".join(lines) + "\n"
                task_md.write_text(text, encoding="utf-8")
                notes.append(f"产出登记补 {len(missing)} 条（{'、'.join(m.split('/')[-1].rstrip(']') for m in missing[:3])}…）")
        # ② 机读块：report.md 末尾没有 yaml 围栏就补一个最小块
        report = base / "output" / "report.md"
        if report.is_file():
            body = report.read_text(encoding="utf-8", errors="replace")
            if "```yaml" not in body[-800:]:
                ev = [f for f in (files or []) if str(f).strip()]
                block = ["", "```yaml", f"status: {status or 'complete'}", "evidence:"]
                block += [f"  - {f}" for f in ev] or ["  - （回调未带 files）"]
                block += ["next_steps:", '  - "（自动生成，待执行者补充）"', "```", ""]
                report.write_text(body.rstrip("\n") + "\n" + "\n".join(block), encoding="utf-8")
                notes.append("机读块补 1 个（status/evidence/next_steps）")
    except Exception as e:  # noqa: BLE001 — 自动补齐失败只记说明，不阻断
        notes.append(f"自动补齐异常（忽略）: {type(e).__name__}: {e}")
    return notes


def _build_gate_context(task_id: str, task_path: str, files: list,
                        summary: str, body: dict) -> dict:
    """bg-gate context 组装单点（Task 2611111775）——callback 载荷全量转发。

    1774 残留边角实证：闭包内联组装漏 evidence——G7/EADC（REJECT 级，
    verification_evidence_anchor.check 消费 context["evidence"]）在重发 done
    场景拿不到显式验证证据 → 降级文本检测 → maybe_clear_on_pass 不触发，
    复核出路断头。本函数是唯一组装点：evidence/files/summary/experiences/
    source 全量转发，与 CLI 直调 gate 等价；旧载荷无 evidence 键时不注入
    （gate 侧 .get 默认 {} 走文本检测——兼容判据不变，禁吞错无兜底分支）。
    """
    import re as _re_ctx
    report_path = (task_path or "").replace("input/task.md", "output/report.md")
    if "input/" in report_path and "/output/" not in report_path:
        report_path = _re_ctx.sub(r"input/[^/]+$", "output/report.md", report_path)
    if report_path.startswith("task/") or not os.path.isabs(report_path):
        report_full = str(VAULT_ROOT / report_path.lstrip("/"))
    else:
        report_full = report_path
    ctx = {
        "task_id": task_id,
        "task_path": task_path,
        "task_path_report": report_full,
        "files": files,
        "summary": summary,
        "experiences": body.get("experiences", []),
        # Task778: source 注入 gate context——done.yaml G13 模板 {source}
        # 缺键致 _resolve_template KeyError 静默原串 → /bin/sh bad substitution。
        "source": body.get("source", ""),
    }
    # 2611111775: evidence 透传（G7/EADC REJECT 级消费；旧载荷无键不注入=兼容）
    _evidence = body.get("evidence")
    if isinstance(_evidence, dict) and _evidence:
        ctx["evidence"] = _evidence
    return ctx


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

    # 2611239006：服务端自己查来历（只记录）；并进回调留痕（body 会存进 callbacks）
    try:
        from nexusos_xdr import proc_origin as _po9006
        _origin9006 = _po9006.observe("callback", str(body.get("source") or ""),
                                     request=request, server_port=19998)
        body["origin"] = _po9006.trace_block(_origin9006)
    except Exception:  # noqa: BLE001 — 埋点绝不影响业务返回
        _origin9006 = {}
    # 大小写兼容
    status = str(body.get("status", "")).lower()
    summary = body.get("summary", "")
    files = body.get("files", [])

    # === 2611192130 J1: callback_arrived —— 回调到达本身落流（MEM#118115 空白②：
    # 此前只有 done/progress 等业务态有事件，「回调到达」无独立痕迹——占位 done
    # 与迟到 callback 无法分辨。校验通过即发（404 前置校验之后），status 原样入流。
    # fire-and-forget：事件失败不影响回调主链（回调可用性 > 观测完整性）。
    try:
        from nexusos_xdr import session_state as _ss_j1
        _ss_j1.emit_system_event(
            source or "xdr", "callback_arrived",
            detail=f"{task_id} callback status={status or '?'} session={session or '?'}",
            target=f"task:{task_id}",
            extra={"callback_status": status, "session": session},
        )
    except Exception:
        pass

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

    # === 2611231133：完成回调必须出示派单凭据（第一层：系统验）===
    # 背景：原来只查「任务自己名下的派单凭据在库里还有效」，不要求调用方拿出来
    # （Task532 P-d4 的妥协：知道 task_id 就能伪造 done）。本段要求调用方出示，
    # 并且必须等于该任务台账里的 credential_id。规则见 references §2.1：
    #   出示且正确 → 照常；出示了但不对 → 两种模式下都拒；
    #   没出示（或台账无凭据可比对）→ 按开关走（record 只记录 / enforce 拒）。
    # 热路径纪律：只读一个小 JSON 文件（每次请求读，改文件即生效、不用重启）+ 追加一行计数，
    # 不在这里连库（现有的凭据有效性校验仍在下面 done 分支里照旧）。
    # === 2611239908：拿不到派单凭据的执行者怎么报不规范派单 ===
    # 死结：约束文档让这种执行者用「需要决策」上报，可统一回调命令没有凭据就拒绝执行、
    # 强制模式下的裸回调也会被拒——真遇到不规范派单反而报不上来。这条通道**只记录**：
    # 状态必须是 need_decision 且带明确标记字段（cred_gate.MISSING_CRED_REPORT_FIELD），
    # 两种模式都接受、只在状态接口单列计数，不改任务状态、不触发自动判定。
    # 标记判定本身失败 → 当普通回调处理（按凭据规则走），不往「放过」一侧倒。
    try:
        from nexusos_xdr.callback import cred_gate as _cg_rep1133
        _miss_report1133 = _cg_rep1133.is_missing_cred_report(body, status)
    except Exception as _rep_e1133:  # noqa: BLE001 — 判定不了按普通回调处理（拒绝侧）
        logger.warning(f"[2611239908] 报告通道判定失败（按普通回调处理）: "
                       f"{type(_rep_e1133).__name__}")
        _miss_report1133 = False

    try:
        from nexusos_xdr.callback import cred_gate as _cg1133
        _presented1133 = (str(request.headers.get("X-Callback-Credential") or "")
                          or str(body.get("callback_credential") or "")).strip()
        _mode1133, _mode_err1133 = _cg1133.read_mode()
        if _miss_report1133:
            # 2611239908：只记录——不比对凭据、不拒、不碰状态机（走下面的 need_decision 分支）
            _cg1133.record_missing_cred_report(task_id, source, status,
                                               detail=f"mode={_mode1133}")
            logger.info(f"[2611239908] {task_id} 无凭据报不规范派单已留痕（mode={_mode1133}）")
        else:
            _ledger_cred1133 = str((state or {}).get("credential_id") or "").strip()
            _outcome1133 = _cg1133.decide(_presented1133, _ledger_cred1133, _mode1133)
            _cg1133.record(_outcome1133, task_id, source, status,
                           detail=(f"mode={_mode1133}" + (f"；{_mode_err1133}" if _mode_err1133 else "")))
            if _cg1133.should_reject(_outcome1133, _mode1133):
                _why1133 = ("出示的派单凭据与该任务台账不一致（零写入）"
                            if _outcome1133 == "wrong"
                            else f"完成回调必须出示派单凭据（当前模式 {_mode1133}）")
                logger.warning(f"[2611231133] {task_id} 回调凭据被拒 outcome={_outcome1133} mode={_mode1133}")
                return JSONResponse({
                    "ok": False, "rejected": True, "reason": _why1133,
                    "credential_outcome": _outcome1133, "mode": _mode1133,
                    "msg": ("请求头带 X-Callback-Credential: <任务书副本 frontmatter 的 credential_id>；"
                            "或用统一回调命令 bash nexus-callback.sh <任务号> <任务书副本> <status> \"<摘要>\""),
                }, status_code=403)
    except Exception as _cg_e1133:  # noqa: BLE001
        # 2611239905：异常时的默认值要落在「不放过」的一侧——重新读一次开关（读不到按 record），
        # 开关是 enforce 就拒绝。原来这里一律「按 record 继续」，等于给强制模式留了个 fail-open 口。
        _mode_fb1133 = "record"
        try:
            from nexusos_xdr.callback import cred_gate as _cg_fb
            _mode_fb1133, _ = _cg_fb.read_mode()
        except Exception as _fb_e:  # noqa: BLE001 — 连开关都读不到：只能按 record（运维路径）
            logger.warning(f"[2611239905] 闸门异常后读开关也失败，按 record: "
                           f"{type(_fb_e).__name__}")
        logger.warning(f"[2611231133] 回调凭据闸门异常（mode={_mode_fb1133}）: "
                       f"{type(_cg_e1133).__name__}: {_cg_e1133}")
        # 2611239908：报告通道是正当的求助流量，闸门自身异常也不该把它拒掉（只记录）
        if _mode_fb1133 == "enforce" and not _miss_report1133:
            return JSONResponse({
                "ok": False, "rejected": True,
                "reason": f"凭据闸门异常（强制模式拒绝）: {type(_cg_e1133).__name__}",
                "mode": _mode_fb1133,
            }, status_code=403)

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
        def _bg_gate_check(_task_id, _body, _state, _files, _summary, _done_ev):
            """后台 gate 检查，结果记 warnings（不阻断 DONE）。"""
            try:
                # Task639 C4: 线程参数 _state 是 done 时刻快照——gate 排队期间
                # patrol 可能已改写 task_state。重新 load 用最新态（None 时回退快照）。
                _state = task_state.load_state(_task_id) or _state
                # 2611111775: context 组装收口单点 _build_gate_context——
                # evidence/files/summary 全量转发与 CLI 直调等价；旧闭包内联
                # 组装漏 evidence 致 G7/EADC 重发场景文本检测误判（1774 残留）。
                _gate_ctx_bg = _build_gate_context(
                    _task_id, _state.get("task_path", ""), _files, _summary, _body)
                _report_full = _gate_ctx_bg["task_path_report"]
                # Task 2611230853 件四：登记类硬拦（G12 机读块 / G14 产出登记）先自动补齐再判
                for _note in _autofix_registration(
                        _task_id, _state.get("task_path", ""), _files, _body.get("status", "")):
                    try:
                        task_state.append_warning(_task_id, f"auto-fixed: {_note}")
                    except Exception:
                        pass
                # 硬拦为什么用 end_reason 不用新状态：task_state.TERMINAL_STATES 被哨兵脚本
                # should_fire.py 硬编码，新增状态=全线炸；被拦任务 status 保持 DONE，
                # 用 end_reason=gate_blocked + /task_state 的 blocked_* + 48h 日汇总显形。
                _ge = gate_engine_check("done", _gate_ctx_bg,
                                        agent=(_state.get("callee") or _body.get("source", "")))
                # 池 #969-B（2611192126）：YAML 层豁免可见性——豁免≠放水，
                # task_state.gate_exempt + 事件流 gate_exempt（日报统计当日
                # 豁免数防滥用无人知）。fail-open：标记失败不阻断 done 链。
                _exem = _ge.get("exempted") or []
                if _exem:
                    _exm_rules = sorted({str(e.get("rule")) for e in _exem})
                    _exm_items = ",".join(str(e.get("item")) for e in _exem)
                    logger.info("[pool#969-B] %s done gate 豁免(%s): %s",
                                _task_id, _exm_rules, _exm_items)
                    try:
                        task_state.update_field(_task_id, "gate_exempt", _exm_rules)
                    except Exception:
                        pass
                    try:
                        task_state.append_warning(
                            _task_id,
                            f"gate_exempt={'/'.join(_exm_rules)}: "
                            f"跳过 {_exm_items}（影子任务豁免，日报可见）")
                    except Exception:
                        pass
                    try:
                        from nexusos_xdr import session_state as _ss_969
                        _ss_969.append_event(
                            "xdr", "gate_exempt",
                            detail=f"{_task_id} 豁免 {len(_exem)} 项"
                                   f"（{'/'.join(_exm_rules)}）: {_exm_items}",
                            target=f"task:{_task_id}")
                    except Exception:
                        pass
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
                    # P0-2 (Task 2611111759): REJECT 硬拦——Task492「记 warning 不阻断」
                    # 升级：REJECT 级 fail → end_reason=gate_blocked + 触发快照 +
                    # owner 认领行 + need_decision 复核回调。全部后台线程收口
                    # （热路径零改动，callback 死锁红线不变）；kill-switch env 可关。
                    # 异常由外层 _ge_err 兜 log，不在热路径、不吞（fail-loud 上抛）。
                    if os.environ.get("NEXUS_P02_HARDBLOCK", "1") == "1":
                        from nexusos_xdr.p02_hardblock import hardblock_rejects

                        hardblock_rejects(
                            _task_id, _state.get("task_path", ""), _ge,
                            report_full=_report_full, summary=_summary,
                            source=_body.get("source", ""), done_event=_done_ev)
                else:
                    logger.info("[Task492][bg-gate] %s gate 通过", _task_id)
                    # P0-① (Task 2611111774): 复核出路——blocked 单重发 done 且本次
                    # gate PASS → 清账回 completed + 池行闭环（1770 P0① 出路断头修复，
                    # enforce 只防重复翻转无回路的补全）。非 blocked 单零开销 no-op；
                    # 同 kill-switch 门（P0-2 家族单一开关）；异常外层 _ge_err 兜 log。
                    if os.environ.get("NEXUS_P02_HARDBLOCK", "1") == "1":
                        from nexusos_xdr.p02_hardblock import maybe_clear_on_pass

                        maybe_clear_on_pass(
                            _task_id, _state.get("task_path", ""),
                            detail=f"bg-gate PASS（重发 done 出路清账）")
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
        # P0-2 (Task 2611111759): DONE 落定事件——硬拦翻转需等主流程终态写入完成，
        # threading.Event 高效等待替代 sleep 轮询（C2 反模式根因修复）。
        _done_ev = threading.Event()
        threading.Thread(target=_bg_gate_check, args=(task_id, body, state, files, summary, _done_ev), daemon=True).start()

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
                    "WHERE action='apply_attempt' AND task_id = ANY(%s) "
                    "AND outcome IS NULL",
                    (task_id_aliases(task_id, ""),),
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
                            by="system/callback-pending-fill",  # 2611230944 甲-6
                            on_behalf=True,
                            entry="system",
                            system_flow="callback-pending-fill",  # 2611230960 乙：显式系统流程参数
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
        _done_ev.set()  # P0-2: 终态已落定（DONE/迟到合并），bg 硬拦可安全判定
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
        # 2611230868（认知诚实闭环根修第一段）：任务完成自动判定曝光。
        # 位置：完成门禁之后（无论过没过都执行）；任何失败只警告不影响回调。
        # 2611230875 修复：调用处只传 task_id 与执行者名——任务路径由函数内
        # 从台账自取（原 _state 在本作用域未定义，每次 NameError 被吞=生产路径从未执行）
        # 2611239003：派单方是主控的任务，自动判定延后到签收之后（开关 defer_judge）。
        # record：照判，但记一条「若延后会推迟多少条」；enforce：真延后（不判）。
        try:
            from nexusos_xdr import ack_policy as _ap868
            _pol868 = _ap868.read_policy()
            _caller868 = str((state or {}).get("caller") or "") if state else ""
            _defer868 = _ap868.should_defer_judge(_caller868, _pol868["defer_judge"])
            if _defer868:
                _n868 = _ap868.count_pending_for_task(task_id)
                _ap868.record("judge_deferred", task_id, _caller868,
                              "回调时延后自动判定（等签收）", extra={"would_defer": _n868})
            else:
                await anyio.to_thread.run_sync(
                    lambda: auto_judge_task_exposures(
                        task_id, body.get("source", ""), ""))
                # 判过就打标：签收路径看到这个字段就不再判（一个任务只判一次）
                try:
                    task_state.update_field(task_id, "auto_judged_at", task_state.now_iso())
                except Exception as _mk868:  # noqa: BLE001
                    logger.warning(f"[2611239003] {task_id} 记 auto_judged_at 失败: {_mk868}")
                if _pol868["defer_judge"] == "record" and _ap868.is_orchestrator(_caller868):
                    _n868 = _ap868.count_pending_for_task(task_id)
                    _ap868.record("judge_deferred_would", task_id, _caller868,
                                  "record 模式：照判，若延后会推迟这些", extra={"would_defer": _n868})
        except Exception as _e868:
            logger.warning(f"[2611230868] 自动判定失败（不影响回调）: {_e868}")

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
                    # Task494: urlopen 包在 run_sync 里（AST 可见 run_sync 上下文）
                    # 2611192100 Q1a: 原写法 .read() 后不 close → FD 泄漏；改为 with 收尾
                    def _fetch(_r=_req):
                        with _urlreq.urlopen(_r, timeout=5) as _fp:
                            return _fp.read()
                    _resp_raw = await anyio.to_thread.run_sync(_fetch)
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
                         WHERE action='apply_attempt' AND task_id = ANY(%s)) AS cand,
                        (SELECT COUNT(*) FROM agent_activity ao
                         JOIN agent_activity aa
                           ON aa.id::text = ao.detail->>'apply_attempt_id'
                           AND aa.action = 'apply_attempt'
                         WHERE ao.action='apply_outcome'
                           AND aa.task_id = ANY(%s)
                           AND COALESCE(ao.detail->>'result','') != 'pending') AS annotated
                    """,
                    (task_id_aliases(task_id, ""), task_id_aliases(task_id, "")),
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


def auto_judge_task_exposures(task_id: str, callee: str, task_path: str) -> dict:
    """2611230868：完成回调自动判定（机械可复核）。

    1. 找本任务名下未结案曝光：task_id 等于本任务的，加上按 bind_task 能归到本任务的。
    2. 读任务产物（task.md/references.md/report.md/evidence.md 存在哪个读哪个），
       算出被引用编号集合（cited_knowledge_ids）。
    3. not_used → UPDATE 结案「未采用」；adopted → 不结案，detail 标「已采用、待给结果」。
    任何失败只记日志不抛（不阻断回调）。
    """
    import psycopg2
    from nexusos_mem.apply_honesty import cited_knowledge_ids, judge_exposures, bind_task
    from nexusos_xdr.task_aliases import task_id_aliases

    try:
        conn = psycopg2.connect("host=127.0.0.1 port=5432 dbname=nexus_mem user=wang")
        # 2611230875：任务路径从台账自取（调用处不传；原传 _state 是 NameError 源头）
        if not task_path:
            _st = task_state.load_state(task_id) or {}
            task_path = _st.get("task_path", "") or ""
        conn.autocommit = False
        cur = conn.cursor()
        # 本任务直标（task_id 列）
        # 2611230889：只处理本任务执行者（或身份为空）的记录；别的身份带本任务号
        # 留下的记录不动，计数进 foreign_untouched 并记日志交主控
        cur.execute(
            "SELECT id, agent_id, target_id, created FROM agent_activity "
            "WHERE action='apply_attempt' AND (outcome IS NULL OR outcome='pending') "
            "AND task_id = ANY(%s)",
            (task_id_aliases(task_id, task_path),))
        _all_direct = cur.fetchall()
        foreign_untouched = [r[0] for r in _all_direct
                             if callee and r[1] and r[1] != callee]
        if foreign_untouched:
            logger.warning(f"[2611230889] {task_id} 有 {len(foreign_untouched)} 条别的身份"
                           f"留下的曝光未处理（交主控）: {foreign_untouched[:5]}")
        cur.execute(
            "SELECT id, agent_id, target_id, created FROM agent_activity "
            "WHERE action='apply_attempt' AND (outcome IS NULL OR outcome='pending') "
            "AND task_id = ANY(%s) AND (agent_id = %s OR agent_id IS NULL OR agent_id = '')",
            (task_id_aliases(task_id, task_path), callee))
        direct = [{"id": r[0], "agent_id": r[1], "target_id": r[2],
                   "created": str(r[3])} for r in cur.fetchall()]
        seen = {e["id"] for e in direct}
        # bind_task 归入（callee 名下无 task_id 的；执行者时间窗=任务全程，近似起止用台账）
        if callee:
            cur.execute(
                "SELECT id, agent_id, target_id, created FROM agent_activity "
                "WHERE action='apply_attempt' AND (outcome IS NULL OR outcome='pending') "
                "AND agent_id = %s AND (task_id IS NULL OR task_id = '')", (callee,))
            # 2611230868 复核追加：bind_task 生产路径传「该执行者最近 10 条任务」
            # （否则同窗多任务规则等于没生效——只传本任务永远单候选）
            import glob as _g868, json as _j868, os as _os868
            _cal_tasks = []
            try:
                _rows_t = []
                for _f in _g868.glob(_os868.path.expanduser('~/.nexus/xdr/tasks/*.json')):
                    try:
                        _d = _j868.load(open(_f, encoding='utf-8'))
                    except Exception:
                        continue
                    if _d.get('callee') == callee:
                        _rows_t.append({"task_id": _d.get("task_id"), "callee": callee,
                                        "started": _d.get("started"),
                                        "finished": _d.get("finished"),
                                        "created": _d.get("created")})
                _rows_t.sort(key=lambda x: str(x.get("created") or ""), reverse=True)
                _cal_tasks = _rows_t[:10]
            except Exception:
                _cal_tasks = []
            # 2611230884 N-16：台账读不到时不造无时间的兜底项（bind_task 会永远归不上）
            tasks = _cal_tasks
            for r in cur.fetchall():
                a = {"id": r[0], "agent_id": r[1], "target_id": r[2],
                     "created": str(r[3])}
                if a["id"] in seen:
                    continue
                if not tasks:
                    logger.warning(f"[2611230884] {task_id} 台账无可用任务列表，曝光 {a['id']} 未归任务（不造无时间兜底）")
                    continue
                b = bind_task(a, tasks)
                if b and b.get("task_id") == task_id:
                    # 2611230884 N-9：归入任务时回填 task_id 列（签收接口只查这一列）
                    try:
                        cur.execute(
                            "UPDATE agent_activity SET task_id = %s WHERE id = %s "
                            "AND (task_id IS NULL OR task_id = '')", (task_id, a["id"]))
                    except Exception:
                        pass
                    direct.append(a)
                    seen.add(a["id"])
        # 读任务产物
        import os as _os868
        texts = []
        if task_path:
            base = _os868.path.dirname(_os868.path.dirname(task_path))
            # 2611230875 甲-1/1b：判据只读执行者写的东西（主控任务书与参考件里
            # 有 exp_injector 注入的编号，会造假 adopted）
            for rel in ("output/report.md", "output/files/evidence.md",
                        "output/negative-results.md"):
                p = _os868.path.join(base, rel)
                if _os868.path.exists(p):
                    try:
                        texts.append(open(p, encoding="utf-8", errors="replace").read())
                    except Exception:
                        pass
        cited = cited_knowledge_ids(texts)
        verdicts = judge_exposures(direct, cited)
        closed = adopted = 0
        for v in verdicts:
            if v["verdict"] == "not_used":
                cur.execute(
                    "UPDATE agent_activity SET outcome='not_applicable', "
                    "detail = detail || jsonb_build_object('auto_judged','not_applicable',"
                    "'judged_by','auto/2611230868','basis','任务产物未引用该经验') "
                    "WHERE id = %s AND (outcome IS NULL OR outcome='pending')",
                    (v["attempt_id"],))
                closed += cur.rowcount
            else:
                # 2611230884 N-3：引用语境含否定词（「不要用/勿用/别用」）→ 留痕提示人工复核
                _neg = any(re.search(r"(?:不要用|勿用|别用|弃用)\s*(?:MEM#?\\d+|经验\\s*\\d+|编号\\s*\\d+)", t)
                           for t in texts)
                cur.execute(
                    "UPDATE agent_activity SET "
                    "detail = detail || jsonb_build_object('auto_judged','adopted',"
                    "'judged_by','auto/2611230868','basis','任务产物引用该经验',"
                    "'neg_context_hint'," + ("true" if _neg else "false") + ") "
                    "WHERE id = %s AND (outcome IS NULL OR outcome='pending')",
                    (v["attempt_id"],))
                adopted += cur.rowcount
        conn.commit()
        cur.close()
        conn.close()
        return {"task_id": task_id, "exposures": len(direct),
                "closed_not_used": closed, "adopted_pending_result": adopted,
                "foreign_untouched": len(foreign_untouched),
                "cited_ids": sorted(cited)[:20]}
    except Exception as e:
        logger.warning(f"[2611230868] 自动判定异常: {type(e).__name__}: {e}")
        return {"task_id": task_id, "error": f"{type(e).__name__}: {e}"}
