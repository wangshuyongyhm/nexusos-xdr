#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
guards.py — nexusos-xdr 删除护栏（唯一硬底线）

v0.4 诚实分层：本模块只拦 **dispatchd 自己执行的 shell 命令**。
小弟 TUI 内部的命令由各小弟自己的 hooks/wrapper 拦截（见 agents/*.py），
做不到的小弟标 MANUAL_ONLY。

两类拦截：
- BLOCK: 不可逆操作 → 强制改写为 mv ~/.Trash/ 或拒绝执行
- WARN:  可恢复操作 → 警告但放行

设计来源: nexus-xdr-架构设计.md §4.4 (v0.4)
作者: ZCode Agent
日期: 2026-07-13
"""

import os
import os
import re
import shlex
import sys
import time
from pathlib import Path
from typing import List, Optional, Tuple  # Task 281 P2-21: 加 Optional（rewrite_to_trash 返 Optional[str]）

from nexusos_common import subpath  # 1960：锚点单源（design §2.1 行8）

# OS trash 优先（macOS=~/.Trash；其他平台无 OS 级回收站时回退 $NEXUS_HOME/trash）
TRASH = Path(os.environ.get("NEXUS_TRASH_DIR") or (
    Path.home() / ".Trash" if sys.platform == "darwin" else subpath("trash")))


class DestructiveOperationError(Exception):
    """不可逆的破坏性操作，dispatchd 层拒绝执行。

    捕获此异常的代码应该：1) 不执行原命令 2) 通知 caller 3) 记录到 task 状态。
    """


# === BLOCK 类：不可逆，强制拦截 ===
# 每条: (正则, 处理方式, 说明)
# 处理方式: "rewrite_trash" = 改写为 mv ~/.Trash/
#           "reject"        = 拒绝执行抛异常
_BLOCK_RULES: List[Tuple[str, str, str]] = [
    # 文件删除 → 改写废纸篓（P2-8: 支持长选项 --recursive --force）
    (r"\brm\b(\s+(-[rfRv]+|--recursive|--force))*", "rewrite_trash", "rm → mv ~/.Trash/"),
    (r"\bunlink\b", "rewrite_trash", "unlink → mv ~/.Trash/"),
    (r"\bfind\b.*\s-delete\b", "rewrite_trash", "find -delete → find -exec mv {} ~/.Trash/ \\;"),
    # 数据库不可逆 → 拒绝
    # 注意: TRUNCATE 正则限定后跟 TABLE（避免误拦 truncate 文件命令）
    (r"\bDROP\s+(TABLE|DATABASE|INDEX)\b", "reject", "DROP 禁止，用 ALTER TABLE ... RENAME 归档"),
    (r"\bTRUNCATE\s+TABLE\b", "reject", "TRUNCATE TABLE 禁止，用 DELETE ... WHERE + 归档"),
    (r"\bDELETE\s+FROM\b(?![^(]*WHERE)", "reject", "DELETE 无 WHERE 禁止"),
    # Git 不可逆 → 拒绝
    (r"\bgit\s+push\b.*--force(?!\-with\-lease)", "reject", "git push --force 禁止，用 --force-with-lease"),
    (r"\bgit\s+reset\s+--hard", "reject", "git reset --hard 禁止，用 stash 或 soft reset"),
    (r"\bgit\s+clean\s+-[a-z]*f", "reject", "git clean -f 禁止，移到 .git-trash/"),
]

# === WARN 类：可恢复，警告但放行 ===
_WARN_RULES: List[Tuple[str, str]] = [
    (r">\s*\S", "> 覆盖重定向：建议先备份"),
    (r"\bchmod\s+-R\b", "chmod -R 递归：确认范围"),
    (r"\bchown\s+-R\b", "chown -R 递归：确认范围"),
    (r"\bkill\s+-9\b", "kill -9 强杀：确认进程"),
]


def execute_guard_global(command: str) -> Tuple[str, List[str]]:
    """拦截+改写 shell 命令。

    P1-8 修正: 先检测子shell/命令替换——若命中则拒绝（无法静态保证安全）。
    避免之前 bash -c "rm xxx" 命中正则但 rewrite_to_trash 返回原命令+假警告的 bug。

    Args:
        command: 原始 shell 命令字符串
    Returns:
        (改写后的命令, 警告列表)。警告列表可能为空。
    Raises:
        DestructiveOperationError: 命中 reject 类规则或子shell调用时抛出。
    """
    # P1-8: 子shell / 命令替换检测——无法静态改写，直接拒绝
    # P2-NEW-6: eval 后可能直接跟括号（eval('rm x')），允许零或多个空白
    if re.search(r'\b(bash|sh|zsh|dash)\s+-c\b|[\$\`]\(|\beval\b\s*', command):
        raise DestructiveOperationError(
            f"[BLOCK→拒绝] 检测到子shell/eval/命令替换，无法静态保证安全: {command[:80]}"
        )

    warnings: List[str] = []

    # BLOCK 检查
    for pattern, action, reason in _BLOCK_RULES:
        if re.search(pattern, command, re.I):
            if action == "rewrite_trash":
                rewritten = rewrite_to_trash(command)
                if rewritten is None:
                    # rewrite_to_trash 无法改写——区分两种情况：
                    # 1. rm 无文件参数（如 "rm" 或 "rm --help"）→ 无害，放行
                    # 2. 无法解析的格式 → 拒绝（P1-8）
                    parts_check = command.split()
                    has_file = any(not p.startswith("-") for p in parts_check[1:])
                    if not has_file:
                        # rm 无文件参数，无害放行
                        warnings.append(f"[WARN] {reason} 但无文件参数，放行")
                        continue
                    raise DestructiveOperationError(
                        f"[BLOCK→拒绝] {reason} 但无法安全改写（未知格式/文件不存在）: {command[:80]}"
                    )
                command = rewritten
                warnings.append(f"[BLOCK→改写] {reason}")
            else:  # reject
                raise DestructiveOperationError(f"[BLOCK→拒绝] {reason}: {command}")

    # WARN 检查（放行但记录）
    for pattern, reason in _WARN_RULES:
        if re.search(pattern, command, re.I):
            warnings.append(f"[WARN] {reason}")

    return command, warnings


def rewrite_to_trash(command: str) -> Optional[str]:  # Task 281 P2-21: 返类型 str → Optional[str]（某些路径返 None）
    """把 rm 类删除命令改写为 mv 到废纸篓。

    处理: 单文件/多文件/glob/同名冲突。
    P2-6 修正: find -exec 里的 ~ 用 expanduser 展开（-exec 不走 shell 展开）。
    P2-7 修正: unlink 分支真的限制为单文件。
    保守策略: 解析失败或无法识别时返回 None，调用方据此判断是否假警告。

    Args:
        command: 含 rm 的 shell 命令
    Returns:
        改写后的命令字符串。无法改写时返回 None。
    """
    TRASH.mkdir(exist_ok=True)

    # P2-6: 特殊处理 find -delete → find -exec mv（用展开后的绝对路径）
    m = re.search(r"\bfind\b(.*)\s-delete\b", command)
    if m:
        trash_abs = str(TRASH) + "/"
        return command.replace(" -delete", f' -exec mv {{}} {trash_abs} \\;')

    try:
        parts = shlex.split(command)
    except ValueError:
        # shlex 解析失败（引号不匹配等）→ 无法改写
        return None

    # 提取文件参数（跳过 rm 和标志参数）
    if not parts or parts[0] not in ("rm", "unlink"):
        return None

    # P2-7: unlink 真的限制为单文件
    files = [p for p in parts[1:] if not p.startswith("-")]
    if parts[0] == "unlink":
        files = files[:1]  # unlink 只接受一个参数

    if not files:
        return None

    # 展开 glob 并生成 mv 命令
    import glob as _glob

    mv_cmds: List[str] = []
    for f in files:
        # glob 展开（无通配符时返回原路径）
        matches = _glob.glob(f) or [f]
        for src_path in matches:
            src = Path(src_path).expanduser()
            if not src.exists():
                continue
            dst = TRASH / src.name
            # 同名冲突: 加时间戳.pid 后缀
            if dst.exists():
                dst = TRASH / f"{src.name}.{int(time.time())}.{os.getpid()}"
            mv_cmds.append(f'mv "{src}" "{dst}"')

    if not mv_cmds:
        # 没有任何文件实际存在 → rm 不存在的文件无害，但改写失败
        return None

    if len(mv_cmds) == 1:
        return mv_cmds[0]
    return "; ".join(mv_cmds)


# === 自测 ===
def _self_test() -> None:
    """guards.py 单元自测。直接运行 `python3 guards.py` 执行。"""
    import tempfile

    tmp = Path(tempfile.mkdtemp())
    test_file = tmp / "test.txt"
    test_file.write_text("test")

    cases = [
        # (描述, 输入, 期望: "rewrite"|"reject"|"pass", 检查函数)
        ("rm 单文件", f"rm {test_file}", "rewrite",
         lambda c: c.startswith("mv ") and ".Trash" in c),
        ("rm -rf", f"rm -rf {test_file}", "rewrite",
         lambda c: c.startswith("mv ") and ".Trash" in c),
        ("rm glob", f"rm {tmp}/*.txt", "rewrite",
         lambda c: "mv " in c and ".Trash" in c),
        ("unlink", f"unlink {test_file}", "rewrite",
         lambda c: c.startswith("mv ") and ".Trash" in c),
        ("git push --force", "git push --force origin main", "reject", None),
        ("git reset --hard", "git reset --hard HEAD~3", "reject", None),
        ("git clean -fd", "git clean -fd", "reject", None),
        ("DROP TABLE", "psql -c 'DROP TABLE users'", "reject", None),
        ("TRUNCATE TABLE", "psql -c 'TRUNCATE TABLE logs'", "reject", None),
        ("DELETE 无 WHERE", "psql -c 'DELETE FROM users'", "reject", None),
        ("正常 ls", "ls -la", "pass", None),
        ("正常 git push", "git push origin main", "pass", None),
        # P1-8: 子shell 绕过 → 拒绝
        ("bash -c rm", 'bash -c "rm /tmp/x"', "reject", None),
        ("$(cmd) 替换", "echo $(rm /tmp/x)", "reject", None),
        ("eval rm", "eval 'rm /tmp/x'", "reject", None),
        # P2-NEW-6: eval 无空格直接括号
        ("eval(rm)", 'eval("rm /tmp/x")', "reject", None),
        # P2-8: 长选项
        ("rm --recursive", f"rm --recursive {test_file}", "rewrite",
         lambda c: c.startswith("mv ") and ".Trash" in c),
        ("rm --force", f"rm --force {test_file}", "rewrite",
         lambda c: c.startswith("mv ") and ".Trash" in c),
    ]

    passed = failed = 0
    for desc, cmd, expect, check in cases:
        try:
            rewritten, warnings = execute_guard_global(cmd)
            if expect == "reject":
                print(f"  ✗ FAIL [{desc}]: 期望拒绝但放行了")
                failed += 1
            elif expect == "rewrite":
                if check and check(rewritten):
                    print(f"  ✓ PASS [{desc}] → {rewritten[:60]}")
                    passed += 1
                else:
                    print(f"  ✗ FAIL [{desc}]: 改写结果不符 {rewritten[:60]}")
                    failed += 1
            else:  # pass
                if rewritten == cmd and not warnings:
                    print(f"  ✓ PASS [{desc}] (放行)")
                    passed += 1
                else:
                    print(f"  ? WARN [{desc}]: {warnings}")
                    passed += 1
        except DestructiveOperationError as e:
            if expect == "reject":
                print(f"  ✓ PASS [{desc}] → 拒绝: {str(e)[:60]}")
                passed += 1
            else:
                print(f"  ✗ FAIL [{desc}]: 意外拒绝")
                failed += 1

    # WARN 类测试
    _, w = execute_guard_global("echo hi > /tmp/test.out")
    assert any("覆盖" in x for x in w), "> 应触发 WARN"
    print(f"  ✓ PASS [> 覆盖 WARN]")
    passed += 1

    print(f"\n结果: {passed} passed, {failed} failed")
    if failed:
        raise SystemExit(1)

    # 清理
    import shutil
    shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    print("=== guards.py 自测 ===")
    _self_test()
