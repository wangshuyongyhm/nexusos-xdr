#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
spill.py — Task311 S1: /callback 大 summary 自动外溢（spill-to-disk）

语义不变量（照搬 Harness spill-policy，309 报告 §2.1/§3.1）：
  1. utf8 字节数**严格大于** maxInlineBytes 才触发（== 不触发）
  2. preview 头尾各半（head=⌈budget/2⌉, tail=⌊budget/2⌋），UTF-8 边界安全
  3. notice 字节成本从 cap 内预留（reserve 按全量字节数计价=位数上界，
     含 preview↔notice 的 2 字节 ``\\n\\n`` join）；替换后仍超 cap → 保留原文
  4. fail-open：spill 任何异常只 warning + 返回原文，绝不改变回调结果
  5. LOAD 期配置校验：XDR_SPILL_MAX_INLINE_BYTES 非「非负整数」→ import 即抛
     （坏配置拒绝启动，不得运行期才炸）

store 布局（309 §3.2）::

    $NEXUS_HOME/xdr/spill/<agent>/<task_id>/<6hex>-summary.txt

目录 0700 / 文件 0600 / ``'x'`` 独占写（已存在报错，防预植）。
notice 格式（任务书固定）::

    (Omitted N bytes. Full result stored at: <绝对路径>. Use read with
    offset/limit, or grep this path to search within it.)
"""

import logging
import os
import re
import secrets
from pathlib import Path
from typing import Any, Tuple, Union

from nexusos_common import subpath  # 1960：锚点单源（design §2.1 行3）

logger = logging.getLogger("nexusos-xdr.spill")

DEFAULT_MAX_INLINE_BYTES = 10 * 1024
NOTICE_FMT = (
    "(Omitted {omitted} bytes. Full result stored at: {path}. "
    "Use read with offset/limit, or grep this path to search within it.)"
)
_JOIN = "\n\n"  # preview 与 notice 的 2 字节连接（Harness 同款）
_SAFE_NAME_RE = re.compile(r"^[a-zA-Z0-9_\-]+$")

SPILL_ROOT = Path(os.environ.get("NEXUS_XDR_SPILL_DIR") or subpath("xdr/spill"))


def _load_cap_from_env() -> int:
    """LOAD 期读取并校验配置（不变量5：坏配置拒绝启动，不运行期才炸）。"""
    raw = os.environ.get("XDR_SPILL_MAX_INLINE_BYTES", str(DEFAULT_MAX_INLINE_BYTES))
    try:
        cap = int(raw)
    except (TypeError, ValueError):
        cap = -1
    if cap < 0:
        raise ValueError(
            f"spill: XDR_SPILL_MAX_INLINE_BYTES 必须是非负整数（got {raw!r}），拒绝启动"
        )
    return cap


MAX_INLINE_BYTES = _load_cap_from_env()


def _sanitize_dir_name(name: str) -> str:
    """目录名防注入：source 是自由字符串（/callback 不限字符集），进路径前必须清洗。"""
    name = (name or "").strip()
    if not _SAFE_NAME_RE.match(name):
        name = re.sub(r"[^a-zA-Z0-9_\-]", "_", name) or "unknown"
    return name[:50]


def _utf8_head_tail(text: str, budget: int) -> Tuple[str, int]:
    """头尾各半 preview，UTF-8 边界安全（字节切片后 ignore 解码，不截半个字符）。

    Returns:
        (preview_text, omitted_bytes)——omitted = 原文 utf8 字节数 - 保留字节数
    """
    data = text.encode("utf-8")
    if budget <= 0:
        return "", len(data)
    head = data[: (budget + 1) // 2].decode("utf-8", errors="ignore")
    tail_b = budget // 2
    tail = data[len(data) - tail_b:].decode("utf-8", errors="ignore") if tail_b > 0 else ""
    kept = len(head.encode("utf-8")) + len(tail.encode("utf-8"))
    return head + tail, len(data) - kept


def _store(original: str, agent: str, task_id: str, root: Path) -> Path:
    """原文落盘：新建目录 0700 / 新文件 0600 / 'x' 独占写。

    已存在的目录不强制改权限——chmod 回写会撤销外部注入的故障（0555 测试）
    且违反「不动非自己创建的东西」；写入失败自然走 fail-open。
    """
    if not root.exists():
        root.mkdir(parents=True, mode=0o700)
        os.chmod(root, 0o700)
    task_dir = root / _sanitize_dir_name(agent) / task_id
    for d in (task_dir.parent, task_dir):
        if not d.exists():
            d.mkdir(mode=0o700)
            os.chmod(d, 0o700)
    path = task_dir / f"{secrets.token_hex(3)}-summary.txt"
    with open(path, "x", encoding="utf-8") as f:
        f.write(original)
    os.chmod(path, 0o600)
    return path


def maybe_spill(
    summary: Any,
    *,
    task_id: str,
    agent: str,
    cap: Union[int, None] = None,
    root: Union[Path, None] = None,
) -> Any:
    """/callback 入口截intercept：超阈值返回 preview+notice，否则或任何失败返回原文。

    永不抛异常（fail-open 红线）；cap/root 仅为测试注入，生产走模块默认。
    """
    if not isinstance(summary, str):
        return summary
    _cap = MAX_INLINE_BYTES if cap is None else cap
    _root = SPILL_ROOT if root is None else root
    try:
        total = len(summary.encode("utf-8"))
        if total <= _cap:  # 不变量1：严格大于才触发
            return summary
        if not _SAFE_NAME_RE.match(task_id or ""):
            logger.warning("[spill] task_id 非法，fail-open 保留原文: %r", task_id)
            return summary
        path = _store(summary, agent, task_id, _root)
        # 不变量3：notice 按全量字节数计价（位数上界 ≥ 真实 omitted 位数），
        # 加 2 字节 join，预留后余量才是 preview 预算
        reserve = len(NOTICE_FMT.format(omitted=total, path=str(path)).encode("utf-8")) + len(_JOIN)
        preview_budget = max(0, _cap - reserve)
        preview_text, omitted = _utf8_head_tail(summary, preview_budget)
        notice = NOTICE_FMT.format(omitted=omitted, path=str(path))
        replaced = f"{preview_text}{_JOIN}{notice}" if preview_text else notice
        if len(replaced.encode("utf-8")) > _cap:
            # Harness 同款：文件成无害孤儿，内联保原文（cleanup deferred）
            logger.warning(
                "[spill] 替换后仍超 cap=%d，保留原文（孤儿文件: %s）", _cap, path
            )
            return summary
        logger.info(
            "[spill] summary %dB → %dB 外溢: %s",
            total, len(replaced.encode("utf-8")), path,
        )
        return replaced
    except Exception as exc:  # 不变量4：fail-open
        logger.warning("[spill] fail-open 保留原文: %s", exc)
        return summary
