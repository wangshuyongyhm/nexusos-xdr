"""exp_injector.py — dispatch 动态注入引擎（阶段 1，08040834）。
dispatch 派发任务时自动检索 MEM 中该 Agent 的历史经验/坑，注入任务书副本尾部——
不靠主控自觉写 references.md，系统自动补。

设计原则（处女原则）：
- 单一职责：只做"关键词提取 + 格式化 + 注入编排"，检索复用 SharedKnowledge（不重造）
- fail-open：MEM 不可达/无结果时不阻塞 dispatch
- 可观测：注入了什么/多少条，写日志

调用方：dispatchd.dispatch() 在副本创建后调用 inject_experiences()。
"""
import logging
import re
import time
from pathlib import Path

logger = logging.getLogger("nexusos-xdr.exp_injector")

# SharedKnowledge 单例（延迟初始化，避免 import 时连 PG）
_kb = None

DEFAULT_TOP_K = 3
INJECT_MARKER = "<!-- exp_injector:自动注入 -->"  # 标记段，防重复注入

# Vault 根稳健定位（task/1755：pypi 布局迁移后 parent 层数变化，旧 4 层假设静默断链——
# 向上找含 05-System 的祖先，兼容 nexusos_xdr/ 旧布局与 pypi/nexusos-xdr/src 新布局）
def _vault_root():
    for _p in Path(__file__).resolve().parents:
        if (_p / "05-System").is_dir():
            return _p
    return Path(__file__).resolve().parents[4]

# CONSTRAINT 目录（B 修复：注入约束规则摘要）
_CONSTRAINT_DIR = _vault_root() / "05-System" / "NEXUS-CONSTRAINT"

# 工具操作卡目录（task/1755 交付 B：tool: 维度词表单一真源=卡 frontmatter tags，
# 与 NEXUS-CONSTRAINT/hooks/tool-card-surface.sh 运行时同读此目录，无第二处硬编码）
_TOOL_CARDS_DIR = _vault_root() / "05-System" / "工具操作卡"
_TOOL_WORDS_CACHE = None


def _tool_words():
    """解析工具操作卡 frontmatter 的 tool:<word> tags（fail-open，缓存）。

    Returns:
        list[(word, card_stem)]，word 小写。卡目录缺失/读取失败返回 []。
    """
    global _TOOL_WORDS_CACHE
    if _TOOL_WORDS_CACHE is not None:
        return _TOOL_WORDS_CACHE
    words = []
    try:
        for p in sorted(_TOOL_CARDS_DIR.glob("*.md")):
            head = p.read_text(encoding="utf-8", errors="ignore")[:600]
            for m in re.finditer(r"tool:([A-Za-z0-9_-]+)", head):
                words.append((m.group(1).lower(), p.stem))
    except Exception as e:
        logger.debug(f"[exp_injector] tool-card 解析失败（fail-open）: {e}")
        words = []
    words = sorted(set(words))
    _TOOL_WORDS_CACHE = words
    return words


def _apply_tool_weighting(task_text: str, results_by_query: dict) -> dict:
    """tool: 维度加权（task/1755 交付 B）：任务书命中工具词时，
    检索结果中 tags 含 tool:<词> 的条目稳定排序前置（纯排序，不改检索 API 本体）。
    """
    try:
        tl = str(task_text or "").lower()
        if not tl:
            return results_by_query
        hit_tags = []
        for word, _card in _tool_words():
            if re.search(r"(^|[^a-z0-9])" + re.escape(word), tl):
                hit_tags.append("tool:" + word)
        if not hit_tags:
            return results_by_query
        for kw, results in list(results_by_query.items()):
            if not isinstance(results, list) or not results:
                continue
            if not all(isinstance(r, dict) for r in results):
                continue  # constraint_hint 等非 MEM 结果段跳过
            def _rank(r, _tags=hit_tags):
                tags = str(r.get("tags", "") or "")
                return 0 if any(t in tags for t in _tags) else 1
            results_by_query[kw] = sorted(results, key=_rank)
        logger.info(f"[exp_injector] tool: 维度加权生效（{hit_tags}）")
    except Exception as e:
        logger.debug(f"[exp_injector] tool 加权失败（fail-open）: {e}")
    return results_by_query

# 动作 → 约束文件映射（哪个动作该注入哪个约束文件的摘要）
_ACTION_CONSTRAINT_MAP = {
    "dispatch": "dispatch-orchestrator.md",
    "init": "init.md",
    "persist": "persist-orchestrator.md",
}
# 角色 → 应注入的 action 列表（Task 465：init 时按角色注入约束）
_ROLE_ACTIONS = {
    "orchestrator": ["init", "dispatch", "persist"],
    "executor": ["init"],
}


def _get_constraint_hint(action: str) -> list:
    """读 CONSTRAINT 约束文件，提取 frontmatter 声明的 inject_sections 段摘要。

    08040834 #1 修复：从关键词猜段改为 frontmatter 声明式（inject_sections 字段）。
    约束文件作者在 frontmatter 声明哪些段该被注入，不靠 exp_injector 猜。

    Args:
        action: 动作名（dispatch/init/persist）

    Returns:
        [{"id": "constraint:xxx", "title": "...", "content": "摘要"}] 或 []
    """
    fname = _ACTION_CONSTRAINT_MAP.get(action)
    if not fname:
        return []
    fpath = _CONSTRAINT_DIR / fname
    if not fpath.exists():
        return []

    try:
        text = fpath.read_text(encoding="utf-8")

        # 1. 解析 frontmatter 的 inject_sections（声明式，不猜）
        inject_sections = _parse_inject_sections(text)
        if not inject_sections:
            # fallback：无声明时用关键词猜（向后兼容旧文件）
            inject_sections = _guess_sections_by_keyword(text)

        if not inject_sections:
            return []

        # 2. 按 inject_sections 提取段内容
        hints = _extract_sections_by_heading(text, inject_sections)

        return [{"id": f"constraint:{fname}",
                 "title": f"[约束] {heading}",
                 "content": content[:200].replace("\n", " ")}
                for heading, content in hints]
    except Exception as e:
        logger.debug(f"[exp_injector] 约束文件读取失败（fail-open）: {e}")
        return []


def _parse_inject_sections(text: str) -> list:
    """从 frontmatter 解析 inject_sections 字段（YAML list）。"""
    import re
    # 匹配 frontmatter 里的 inject_sections
    m = re.search(r'^inject_sections:\s*\n((?:\s+-\s+.+\n)+)', text, re.MULTILINE)
    if not m:
        return []
    items_text = m.group(1)
    # 提取每个 - "section name"
    items = re.findall(r'-\s+"(.+?)"', items_text)
    return items


def _guess_sections_by_keyword(text: str) -> list:
    """fallback：无 inject_sections 声明时，用关键词猜（向后兼容）。"""
    priority_keywords = ["门禁", "铁律", "清单", "边界", "核心"]
    headings = []
    current_heading = ""
    for line in text.splitlines():
        if line.startswith("## ") or line.startswith("### "):
            current_heading = line.lstrip("# ").strip()
            if any(kw in current_heading for kw in priority_keywords):
                headings.append(line.strip())  # 保留原始 ## 前缀
    return headings[:3]


def _extract_sections_by_heading(text: str, target_headings: list) -> list:
    """按目标标题列表提取段内容。target_headings 是完整标题行（含 ## 前缀）。"""
    results = []
    lines = text.splitlines()
    i = 0
    while i < len(lines):
        line = lines[i]
        # 检查是否是目标标题（支持 ## 和 ### 前缀）
        line_stripped = line.strip()
        matched_target = None
        for target in target_headings:
            # 标准化比较（去 # 前缀，去空格）
            target_clean = target.lstrip("# ").strip()
            line_clean = line_stripped.lstrip("# ").strip()
            if target_clean == line_clean:
                matched_target = target
                break
        if matched_target:
            heading = line_stripped.lstrip("# ").strip()
            # 收集该段内容（到下一个 ## 或 ### ）
            section_lines = []
            i += 1
            while i < len(lines):
                next_line = lines[i]
                if next_line.startswith("## ") or next_line.startswith("### "):
                    break
                if next_line.strip() and not next_line.startswith(">"):
                    section_lines.append(next_line)
                i += 1
            content = "\n".join(section_lines).strip()
            if content and len(content) > 10:
                results.append((heading, content))
        else:
            i += 1
    return results


def _get_kb():
    """获取 SharedKnowledge 单例（延迟初始化）。"""
    global _kb
    if _kb is None:
        # 1636 活①收口: 原此处 insert(parent.parent=nexusos-xdr/src)对本 import 无效
        # (nexusos_mem 在另一包), 删冗余 hack——nexusos_mem 靠 pip 安装/extra 解析(1597)
        from nexusos_mem.shared import SharedKnowledge
        _kb = SharedKnowledge(agent_id="xdr-exp-injector")
    return _kb


def _safe_truncate(text: str, max_len: int) -> str:
    """安全截断——不在中文词中间切断（在空格/标点/ASCII 边界截）。

    08040834 修复：原 title[:20] 硬切导致"dispatch 注入"被截成"dispatch 注"。
    改为：如果截断点在中文中间，回退到最近的空格或 ASCII 边界。
    """
    if len(text) <= max_len:
        return text.strip()
    truncated = text[:max_len]
    # 从截断点往前找安全边界（空格/ASCII 转 中文 的边界）
    for i in range(len(truncated) - 1, max(0, len(truncated) - 8), -1):
        char = truncated[i]
        prev = truncated[i-1] if i > 0 else ""
        # 空格是安全边界
        if char == " ":
            return truncated[:i].strip()
        # ASCII→中文 边界（如 "dispatch 注" → 截到 "dispatch"）
        if char.isascii() and not prev.isascii():
            return truncated[:i].strip()
        # 中文→ASCII 边界
        if not char.isascii() and prev.isascii():
            return truncated[:i].strip()
    return truncated.strip()


def _extract_keywords(task_text: str, target_agent: str) -> list:
    """从任务书文本提取检索关键词。

    策略：标题核心词 + 目标 Agent 名 + 核心目标段核心词。
    只取短词（2-20 字），不取整句（防精确匹配时搜不到）。
    用 _safe_truncate 防中文断词（08040834 修复）。
    """
    keywords = []
    # 1. 目标 Agent 名（搜该 Agent 的近期坑）
    keywords.append(target_agent)
    # 2. 任务书标题核心词（# 开头第一行，取冒号后的主题词）
    for line in task_text.splitlines():
        line = line.strip()
        if line.startswith("# "):
            title = line[2:]
            # 按冒号分割取主题部分（如 "Task 430: 调研 X" → "调研 X"）
            if ":" in title or "：" in title:
                title = title.split(":")[-1].split("：")[-1].strip()
            # 安全截断到 20 字符（防中文断词）
            title = _safe_truncate(title, 20)
            if title:
                keywords.append(title)
            break
    # 3. "核心目标" 段第一个实词（安全截断到 15 字符）
    in_core = False
    for line in task_text.splitlines():
        if "核心目标" in line:
            in_core = True
            continue
        if in_core:
            line = line.strip()
            if line and not line.startswith("#") and not line.startswith("-") and not line.startswith(">"):
                snippet = _safe_truncate(line.replace("，", "").replace(",", ""), 15)
                if snippet and len(snippet) >= 2:
                    keywords.append(snippet)
                break
    return [k for k in keywords if k]


def _search(query: str, tag: str = None, knowledge_type: str = None,
            require_match: bool = False, agent_filter: str = None,
            exclude_monitor_alerts: bool = True) -> list:
    """搜 MEM（复用 SharedKnowledge），返回结果列表。失败返回空列表（fail-open）。

    Args:
        query: 检索词
        tag: 过滤标签（可选）
        knowledge_type: knowledge_type 过滤（experience/pitfall/document 等）
        require_match: True 时要求结果 title/content/tags 必须包含 query
                       （防语义搜索太宽泛，如 target_agent 名匹配到无关经验）
        agent_filter: 只返回 source_agent == agent_filter 的结果（精确到该 Agent 写的经验，
                      不是"提到该 Agent"的经验。防噪音）
        exclude_monitor_alerts: True 时排除 title 含"【监控告警】"的结果
                                （这些是 nexus-monitor 自动写的临时告警，不是真经验。
                                 注意：只是查询时不选，MEM 数据不删——不失忆）
    """
    try:
        kb = _get_kb()
        results = kb.search(query, top_k=DEFAULT_TOP_K, tag=tag, knowledge_type=knowledge_type)
        # 可选：要求精确匹配（防语义搜索太宽泛）
        if require_match and query:
            query_lower = query.lower()
            results = [r for r in results
                       if query_lower in str(r.get("title", "")).lower()
                       or query_lower in str(r.get("content", "")).lower()
                       or query_lower in str(r.get("tags", "")).lower()]
        # 可选：按 source_agent 精确过滤（C 修复：防"提到 oc"匹配到所有含 oc 的内容）
        if agent_filter:
            results = [r for r in results
                       if str(r.get("source_agent", "")).lower() == agent_filter.lower()]
        # 可选：排除监控告警（A 修复：临时告警不是真经验。查询侧过滤不删数据）
        if exclude_monitor_alerts:
            results = [r for r in results
                       if "【监控告警】" not in str(r.get("title", ""))]
        return results
    except Exception as e:
        logger.debug(f"[exp_injector] MEM search 失败（fail-open）: {e}")
        return []


def _format_injection(target_agent: str, results_by_query: dict,
                      uk_reminder: str = "") -> str:
    """把检索结果格式化为 markdown 注入段。

    Task583: uk_reminder 非空时附在经验段后（假设腐烂提醒，≤3 行）；
    无经验结果但有 uk_reminder 时仍建段（提醒独立生效）。
    """
    if not any(results_by_query.values()) and not uk_reminder:
        return ""

    lines = [
        "",
        INJECT_MARKER,
        "## 🔧 自动注入：相关经验（exp_injector 拉取，非主控手写）",
        f"> 目标 Agent: {target_agent} | dispatch 时自动从 MEM 检索",
        "",
    ]
    total = 0
    for query, results in results_by_query.items():
        if not results:
            continue
        lines.append(f"### 检索: {query}")
        for r in results:
            total += 1
            rid = r.get("id", "?")
            title = r.get("title", "")[:60]
            content = str(r.get("content", ""))[:200].replace("\n", " ")
            ktype = r.get("knowledge_type", "?")
            lines.append(f"- **#{rid}** [{ktype}] {title}")
            lines.append(f"  {content}")
        lines.append("")
    if uk_reminder:
        lines.append(uk_reminder)
        lines.append("")
    lines.append(f"（共 {total} 条，来源 MEM SharedKnowledge）")
    lines.append("")
    return "\n".join(lines)


# === Task583: UK 假设腐烂提醒（未决池 #29(旧)） ===
# UK 假设写进 report 后无人验证就腐烂——dispatch 时扫近期任务 report 的 UK 象限行，
# 未验证假设附注入段提醒小弟跟进，给假设一条回收路径。
_UK_LINE_RE = re.compile(r'UK[（(]未知已知[）)]\s*[：:]\s*(.+)')


def _scan_stale_uk(task_path: str, max_hits: int = 2, window_days: int = 14) -> list:
    """扫 task/ 根下近期任务的 report.md UK 象限行（未验证假设）。

    fail-open（任何异常返回 []）；行含「已验证」跳过；每任务取 1 条防刷屏。
    返回 [{"task": 目录名, "uk": 假设文本}, ...]，≤ max_hits（提醒总长 ≤3 行）。
    """
    try:
        task_file = Path(task_path)
        if not task_file.exists():
            return []
        # .../task/NN/input/task.md → parents[0]=input parents[1]=NN parents[2]=task
        task_root = task_file.resolve().parents[2]
        if task_root.name != "task":
            return []
        now = time.time()
        uk_pat = _UK_LINE_RE
        hits = []
        reports = sorted(task_root.glob("*/output/report.md"),
                         key=lambda p: p.stat().st_mtime, reverse=True)
        for rep in reports:
            try:
                if now - rep.stat().st_mtime > window_days * 86400:
                    break  # mtime 降序，后续更旧
                for line in rep.read_text(encoding="utf-8", errors="ignore").splitlines():
                    if "UK" not in line or "已验证" in line:
                        continue
                    m = uk_pat.search(line)
                    if m and m.group(1).strip():
                        uk_text = m.group(1).strip()[:80]
                        # Task583: 过滤模板占位符（<我假设的前提> 等 <> 包裹的
                        # 教学示例行——真实数据 485/489 report 含模板文本会误报）
                        if "<" in uk_text and ">" in uk_text:
                            continue
                        hits.append({"task": rep.parent.parent.name, "uk": uk_text})
                        break  # 每任务 1 条
                if len(hits) >= max_hits:
                    break
            except OSError:
                continue
        return hits
    except Exception as e:
        logger.debug(f"[exp_injector] UK 扫描异常（fail-open）: {e}")
        return []


def _format_uk_reminder(hits: list) -> str:
    """UK 假设腐烂提醒段（1 行标题 + ≤2 条命中，共 ≤3 行）。"""
    lines = ["### ⚠️ 假设腐烂提醒（上次任务未验证假设，本次请跟进或声明不适用）"]
    for h in hits:
        lines.append(f"- 「{h['task']}」：{h['uk']}")
    return "\n".join(lines)


def inject_experiences(dispatched_text: str, task_id: str, target_agent: str,
                       task_text: str, task_path: str = "") -> str:
    """主入口：检索 MEM + 返回注入后的文本。

    Args:
        dispatched_text: 任务书副本当前文本（已做 task_id 替换）
        task_id: xdr task_id
        target_agent: 目标 Agent 名（oc/ds/kimi 等）
        task_text: 原始任务书文本（用于提取关键词）
        task_path: 任务书路径（可选，Task583 UK 假设腐烂提醒用——扫同 task/ 根
                   近期 report 的未验证 UK 假设行）

    Returns:
        注入后的文本（追加经验段）。MEM 不可达或无结果时返回原文本（不变）。
    """
    # 防重复注入
    if INJECT_MARKER in dispatched_text:
        logger.debug(f"[exp_injector] {task_id} 已有注入标记，跳过")
        return dispatched_text

    keywords = _extract_keywords(task_text, target_agent)
    if not keywords:
        logger.info(f"[exp_injector] {task_id} 未提取到关键词，跳过")
        return dispatched_text

    # 按关键词检索（C 修复：agent_filter 精确到该 Agent 写的经验，防"提到 oc"噪音）
    results_by_query = {}
    for kw in keywords:
        results = _search(kw, tag="experience", knowledge_type="experience",
                          require_match=True, agent_filter=target_agent)
        if results:
            results_by_query[kw] = results

    # 专搜目标 Agent 写的近期坑（pitfall，agent_filter 精确 + 排除监控告警）
    pitfalls = _search(target_agent, knowledge_type="pitfall",
                       agent_filter=target_agent)[:3]
    if pitfalls:
        results_by_query[f"{target_agent} 近期坑"] = pitfalls

    # task/1755 交付 B：tool: 维度加权——任务书命中工具词时 tool:<词> 标签条目前置
    results_by_query = _apply_tool_weighting(task_text, results_by_query)

    # B 修复：注入 CONSTRAINT 约束规则摘要（不只是 MEM 经验）
    constraint_hint = _get_constraint_hint("dispatch")
    if constraint_hint:
        results_by_query["dispatch 约束摘要"] = constraint_hint

    # Task583: UK 假设腐烂提醒——扫近期任务 report 未验证假设（fail-open）
    uk_reminder = ""
    uk_hits = _scan_stale_uk(task_path)
    if uk_hits:
        uk_reminder = _format_uk_reminder(uk_hits)
        logger.info(f"[exp_injector] {task_id} UK 假设腐烂提醒 {len(uk_hits)} 条")

    injection = _format_injection(target_agent, results_by_query, uk_reminder)
    if not injection:
        logger.info(f"[exp_injector] {task_id} MEM 无相关经验，不注入")
        return dispatched_text

    logger.info(f"[exp_injector] {task_id} 注入 {sum(len(v) for v in results_by_query.values())} 条经验（关键词: {keywords}）")
    return dispatched_text + "\n" + injection


def inject_for_gate_reject(gate_failures: list, task_id: str) -> list:
    """gate reject 时搜 MEM 同类 pitfall，返回经验列表附在 error 响应里。

    和 inject_experiences 不同：
    - inject_experiences 注入到任务书副本文本（dispatch 时用）
    - inject_for_gate_reject 返回结构化列表（gate reject 响应附 related_experiences 字段）

    复用 _search（SharedKnowledge），不重造。

    Args:
        gate_failures: gate 失败项列表（如 ["G1_REJECT: 文件不存在", "G4_REJECT: 行数<30"]）
        task_id: task_id（日志用）

    Returns:
        相关 pitfall 经验列表（空列表则无相关经验）
    """
    if not gate_failures:
        return []

    # 从 gate_failures 提取检索关键词
    # 策略：gate id（G1/G2/C4 等，搜该 gate 已知坑）+ 描述核心词
    keywords = set()
    for f in gate_failures:
        # gate id（如 "G1_REJECT: ..." → "G1"）
        if "_" in f:
            gate_id = f.split("_")[0]
            if gate_id:
                keywords.add(gate_id)
        # 描述核心词（冒号后，取前 20 字符）
        if ":" in f:
            desc = f.split(":", 1)[1].strip()
            if desc and len(desc) >= 4:
                keywords.add(desc[:20])

    if not keywords:
        return []

    # 搜同类 pitfall
    all_results = []
    for kw in keywords:
        results = _search(kw, knowledge_type="pitfall", require_match=True)
        all_results.extend(results)

    # 去重（按 id）
    seen = set()
    unique = []
    for r in all_results:
        rid = r.get("id")
        if rid and rid not in seen:
            seen.add(rid)
            unique.append(r)

    if unique:
        logger.info(f"[exp_injector] {task_id} gate reject 注入 {len(unique)} 条同类 pitfall")
    # 只取 JSON 可序列化的字段（MEM 结果可能含 datetime 等不可序列化对象）
    safe = []
    for r in unique[:3]:
        safe.append({
            "id": r.get("id"),
            "title": r.get("title", ""),
            "content": str(r.get("content", ""))[:200],
            "knowledge_type": r.get("knowledge_type", ""),
            "tags": r.get("tags", []),
        })
    return safe


def inject_for_init(agent: str, role: str = "executor") -> dict:
    """init 时（session/register）注入该 Agent 近期坑 + 未决问题 + 约束摘要。

    和前两个不同：
    - inject_experiences 注入到副本（dispatch 用）
    - inject_for_gate_reject 返回 pitfall 列表（gate reject 用）
    - inject_for_init 返回结构化 dict（register 响应附 init_context 字段）

    Task 465: 按角色注入约束摘要（不靠 Agent 自觉读约束文件）。
    主控(orchestrator) → init+dispatch+persist 约束，小弟(executor) → init 约束。

    Args:
        agent: Agent 名（zcode/oc/ds 等）
        role: 角色（orchestrator/executor），默认 executor（向后兼容）

    Returns:
        {"recent_pitfalls": [...], "recent_experiences": [...], "constraint_hints": [...]}
        空列表则无相关（fail-open）
    """
    result = {"recent_pitfalls": [], "recent_experiences": [], "constraint_hints": []}
    try:
        # 1. 该 Agent 近期坑（pitfall 类型）
        pitfalls = _search(agent, knowledge_type="pitfall", require_match=True)
        result["recent_pitfalls"] = [
            {"id": r.get("id"), "title": r.get("title", "")[:60],
             "content": str(r.get("content", ""))[:150]}
            for r in pitfalls[:3]
        ]
        # 2. 该 Agent 近期高价值经验（experience 类型）
        exps = _search(agent, knowledge_type="experience", require_match=True)
        result["recent_experiences"] = [
            {"id": r.get("id"), "title": r.get("title", "")[:60],
             "content": str(r.get("content", ""))[:150]}
            for r in exps[:3]
        ]
        # 3. 按角色注入约束摘要（Task 465）
        for action in _ROLE_ACTIONS.get(role, ["init"]):
            result["constraint_hints"].extend(_get_constraint_hint(action))
        total = len(result["recent_pitfalls"]) + len(result["recent_experiences"]) + len(result["constraint_hints"])
        if total:
            logger.info(f"[exp_injector] init 注入 {agent}(role={role}): {len(result['recent_pitfalls'])} pitfall + {len(result['recent_experiences'])} experience + {len(result['constraint_hints'])} constraint")
    except Exception as e:
        logger.debug(f"[exp_injector] init 注入失败（fail-open）: {e}")
    return result
