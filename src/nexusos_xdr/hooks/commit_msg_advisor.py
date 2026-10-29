#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""commit_msg_advisor.py — Task 390: commit-msg hook 的环节匹配提示 + pitfall 沉淀。

被 .git/hooks/commit-msg 调用，不拦截 commit（只输出信息到 stderr / 写 MEM pitfall）。
独立脚本（非 bash 内联 python3 -c），避免 bash 3.2 单引号转义陷阱（陷阱#1）。

调用:
    python3 commit_msg_advisor.py <changed_files_stdin_lines> <commit_msg>
    changed_files 从 stdin 逐行读（git diff --cached --name-only 输出）

功能1（本文件）: 环节匹配提示 — 按 changed_files 匹配标准库环节，输出已知坑/验证标准到 stderr
功能2（后续 task#3 加）: pitfall 沉淀 — commit_msg 含 [pitfall:...] → 写 MEM
"""
import os
import re
import sys
from pathlib import Path

# 1968 红线收口：vault 根经包内单源锚点解析（env > 哨兵，1957 范式）——
# 缺 vault 时 STDLIB 不可达，环节匹配降级为无提示（hook 本就只输出不拦截）。
try:
    from nexusos_xdr._vault_env import VAULT_ROOT as _VR
    VAULT = str(_VR) if str(_VR) != "/nonexistent-nexus-vault-root" else ""
except Exception:
    VAULT = os.environ.get("NEXUS_VAULT_ROOT", "")
AUTO = f"{VAULT}/05-System/自动化脚本" if VAULT else ""
STDLIB = f"{VAULT}/05-System/NEXUS-CONSTRAINT/环节处理标准库.md" if VAULT else ""


# ============================================================
# 环节→文件模式映射表（从标准库触发判断手动提取，确保匹配准确）
# 一个文件可匹配多环节（如 check_*.sh 同时是 Shell脚本+check检测器）
# 格式: (显示名, [文件名/路径子串匹配], 标准库环节标题关键词)
# ============================================================
STAGE_PATTERNS = [
    ("环节1 HTTP端点", ["api_server.py", "mcp_server.py"]),
    ("环节2 PG读写", ["shared.py", "search.py", "crucible_explore", "crucible_trigger"]),
    ("环节3 Shell脚本", [".sh", "lifecycle", "start_tool", "common.sh"]),
    ("环节4 Gate/YAML", ["gate_engine", "_checks/", "etc/gates/", "gate-triggers"]),
    ("环节5 dispatch/callback", ["dispatchd", "wait_callback", "dispatch"]),
    ("环节6 check检测器", ["check_", "nexus-monitor", "aggregate.py"]),
    ("环节7 部署/重启", [".plist", "install-plists", "services.yaml"]),
    ("环节8 验证/测试", ["test_", "verify_", "VERIFICATION"]),
    ("环节9 检索/向量", ["embed_corpus", "embed_server", "SharedKnowledge"]),
    ("环节10 版本/升级", ["check_agent_versions", "snap-agent", "agent-config-snapshots"]),
    ("环节11 体系/文档", ["NEXUS-CONSTRAINT", "AGENTS.md", "ATOMCODE.md"]),
]

# 已知坑摘要（从标准库提取 Top 坑，避免每次读文件——活文档更新时这里手动同步）
STAGE_HINTS = {
    "环节1 HTTP端点": [
        ("MEM#34768", "HTTP响应格式先验证再写解析"),
        ("MEM#32518", "BaseHTTPRequestHandler 未捕获PG异常返Empty reply"),
        ("DISCIPLINE反例1", "测试环境≠运行环境(cwd差异)"),
    ],
    "环节2 PG读写": [
        ("", "get_kb()并发竞态→double-checked locking"),
        ("", "freshness异常路径conn不close→try/finally"),
        ("MEM#29636", "Memory口径混搭:Hermes本地文件vs PG表"),
    ],
    "环节3 Shell脚本": [
        ("MEM#34313/34320", "python3 -c单引号包裹禁用单引号字符"),
        ("MEM#34471/34493", "中文locale变量名boundary必须用${var}"),
        ("MEM#33385", "grep -c数行不数次(同行多匹配只算1行)"),
        ("MEM#33386", "set -euo下grep|head-1||echo0整数崩溃"),
        ("MEM#25146", "bash EXIT trap吃掉exit code"),
    ],
    "环节4 Gate/YAML": [
        ("MEM#32206", "shell ${VAR}与引擎str.format冲突"),
        ("MEM#21807/21823", "write.py约束表正则bug"),
        ("", "未知check type=REJECT硬拦(确保_checks插件存在)"),
    ],
    "环节5 dispatch/callback": [
        ("MEM#22153", "裸send-keys是陷阱,必须用start_tool.sh dispatch"),
        ("MEM#22885", "dispatch后必须异步等待(禁sleep轮询)"),
        ("MEM#22884", "patrol线程4bug:异常不捕获杀daemon线程"),
        ("MEM#24723", "output_detection误判DONE(三重确认不够)"),
    ],
    "环节6 check检测器": [
        ("references#1", "grep误报大面积(测多行/作用域必误报,改AST)"),
        ("references#2", "字符串包含测结构化字段误匹配"),
        ("references#4", "errlog mtime测崩溃指标错(需内容感知)"),
    ],
    "环节7 部署/重启": [
        ("MEM#29906", "改.py后串联验证运行时生效(不重启=白改)"),
        ("MEM#22945", "部署后必须重启所有依赖服务"),
        ("init§11", "launchctl list第二列历史值盲区"),
    ],
    "环节8 验证/测试": [
        ("Task388", "测试不自包含(依赖运行时状态→造fixture)"),
        ("Task388", "硬编码环境状态(zcode在线→改测逻辑健壮性)"),
        ("Task388", "files数组用不存在路径(造tempfile)"),
    ],
    "环节9 检索/向量": [
        ("Task386", "算法验证必须真实import(禁grep片段重写)"),
        ("Task386", "fsevents漏清(Layer1d只扫近期)"),
    ],
    "环节10 版本/升级": [
        ("MEM#27245", "版本检测PATH口径陷阱(kimi双安装误报)"),
        ("B9", "禁擅自升级(需用户同意+备份+验证)"),
    ],
    "环节11 体系/文档": [
        ("Task382", "引用失效断链(改文档后check_I2)"),
        ("Task384", "数字漂移(跨文档断言审计)"),
        ("MEM#27180", "操作类任务三层检索(文件+PG+图谱)"),
    ],
}

STAGE_VERIFY = {
    "环节1 HTTP端点": "Level3 (curl端点:正常+异常+中文不乱码)",
    "环节2 PG读写": "Level3 (写入含中文+读回+异常rollback不泄漏+并发)",
    "环节3 Shell脚本": "Level2 (bash -n + dry-run) + 边界(空/特殊字符/中文)",
    "环节4 Gate/YAML": "Level2 (yaml.safe_load + check()双向测试)",
    "环节5 dispatch/callback": "Level3 (真实dispatch→patrol→回调→状态机迁移)",
    "环节6 check检测器": "Level3 (构造真问题=error + 正常态=ok + 不误报漏报)",
    "环节7 部署/重启": "Level3 (启动时间>mtime + curl health + 新功能生效)",
    "环节8 验证/测试": "pytest全绿 + 正向pass+反向fail双向",
    "环节9 检索/向量": "Level3 (真实import算法 + hash drift抽样对比)",
    "环节10 版本/升级": "Level3 (备份+登录态+核心链路+五服务+skills)",
    "环节11 体系/文档": "Level2 (write.py --check-refs + check_I2 + check_M2)",
}


def match_stages(changed_files: list) -> list:
    """按 changed_files 匹配环节，返回匹配到的环节显示名列表（去重保序）。"""
    matched = []
    seen = set()
    for stage_name, patterns in STAGE_PATTERNS:
        for cf in changed_files:
            for pat in patterns:
                if pat in cf:
                    if stage_name not in seen:
                        matched.append(stage_name)
                        seen.add(stage_name)
                    break
    return matched


def emit_stage_hints(stages: list) -> None:
    """输出环节提示到 stderr（不拦截 commit）。"""
    if not stages:
        return
    lines = []
    lines.append("=" * 60)
    lines.append(f"📌 本次改动匹配 {len(stages)} 个环节（查标准库: 环节处理标准库.md）")
    for stage in stages:
        lines.append(f"\n【{stage}】")
        hints = STAGE_HINTS.get(stage, [])
        lines.append(f"  已知{len(hints)}坑:")
        for ref, desc in hints:
            ref_tag = f" {ref}" if ref else ""
            lines.append(f"    - {desc}{ref_tag}")
        verify = STAGE_VERIFY.get(stage, "见标准库")
        lines.append(f"  验证标准: {verify}")
    lines.append("=" * 60)
    # 输出到 stderr（任务书要求，python3 -c 的 stderr 显示给用户）
    sys.stderr.write("\n".join(lines) + "\n")


def extract_pitfalls(commit_msg: str) -> list:
    """解析 commit_msg 里的 [pitfall:问题描述] 标记，返回问题描述列表。

    格式（大小写不敏感）：
        [pitfall:HTTP边界未捕获PG异常返Empty reply]
        [pitfall: 中文locale变量名boundary必须用${var} ]
    支持一行多个（用 ][ 分隔）和带空格。
    """
    if not commit_msg:
        return []
    # [pitfall:...] 或 [Pitfall:...] 大小写不敏感
    matches = re.findall(r"\[pitfall:\s*([^\]]+?)\s*\]", commit_msg, re.IGNORECASE)
    return [m.strip() for m in matches if m.strip()]


def persist_pitfall(problem_desc: str, commit_msg: str, stages: list) -> bool:
    """把 pitfall 写入 MEM（shared.py add_knowledge），返回是否写入成功。

    add_knowledge 内置 content_hash 去重（L635-645，相同 title+content 前缀复用既有 id），
    所以同 commit_msg 不会重复写——天然满足陷阱"同commit_msg不重复写"。

    失败不阻断 commit（陷阱：pitfall 沉淀是辅助不是门禁）。
    """
    try:
        sys.path.insert(0, AUTO)
        from nexus.shared import SharedKnowledge
        kb = SharedKnowledge(agent_id="commit-msg-hook")
        # content 含 commit 详情 + 匹配环节，方便后续深挖
        stage_tag = stages[0] if stages else "未匹配环节"
        content = (
            f"[commit-msg hook 自动沉淀]\n"
            f"问题: {problem_desc}\n"
            f"匹配环节: {', '.join(stages) if stages else '未匹配'}\n"
            f"commit: {commit_msg.strip()[:200]}"
        )
        kid = kb.add_knowledge(
            title=problem_desc[:200],  # title 字段有长度约束，截断
            content=content,
            knowledge_type="pitfall",
            tags=[stage_tag, "commit-msg-hook"] + stages,
            project="nexus",
        )
        sys.stderr.write(
            f"📝 已沉淀到 MEM pitfall (id={kid}, 环节:{stage_tag})\n"
        )
        return True
    except Exception as e:
        # 沉淀失败不阻断 commit，只 stderr 告警
        sys.stderr.write(f"⚠️ pitfall 沉淀失败（不阻断 commit）: {e}\n")
        return False


def main():
    """入口: argv[1]=commit_msg, 其余 argv[2:]=changed_files（或 stdin）。"""
    commit_msg = sys.argv[1] if len(sys.argv) > 1 else ""
    # changed_files: argv[2:] 或 stdin
    if len(sys.argv) > 2:
        changed = sys.argv[2:]
    else:
        changed = [l.strip() for l in sys.stdin.read().splitlines() if l.strip()]

    # 功能1: 环节匹配提示（即使 changed 空也跑 pitfall 沉淀，因为它只依赖 commit_msg）
    stages = []
    if changed:
        stages = match_stages(changed)
        emit_stage_hints(stages)

    # 功能2: pitfall 沉淀（commit_msg 含 [pitfall:...] 标记时）
    pitfalls = extract_pitfalls(commit_msg)
    for desc in pitfalls:
        persist_pitfall(desc, commit_msg, stages)


if __name__ == "__main__":
    main()
