#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
base.py — AgentBase 抽象接口

所有 Agent（hermes/zcode/oc/omp/kimi/ds/ds2/ac）继承此类。
统一接口: send_task / ensure_no_popup / rescue_keys / is_idle

设计来源: nexus-xdr-架构设计.md §4.3 (v0.4)
tmux 交互基于 dispatch.sh 11步流程实测
配置源: ~/.hermes/tools.yaml (单一真相源)

作者: ZCode Agent
日期: 2026-07-13
"""

import os
import re
import sys
import time
import subprocess
import logging
import threading
from pathlib import Path
from typing import List, Optional

logger = logging.getLogger("nexusos-xdr.agent")

# Task 280 P1-12: 模块级 send_keys 锁（替代原 hasattr 懒初始化双检锁）
# 所有 AgentBase 实例共享，避免两个线程同时进 send_keys 各自创建 Lock 导致锁失效
_SEND_LOCK = threading.Lock()

# === 路径常量 ===
# 跨产品契约：hermes 工具配置真源 ~/.hermes/tools.yaml（非 NEXUS 私有布局，
# 不迁 .nexus——hermes 进程自身也读它）
_HERMES = Path.home() / ".hermes"
_TOOLS_YAML = _HERMES / "tools.yaml"

# Task 282 P2-3: sys.path 初始化改用 nexus.paths_setup 单源
# Task668/1960: _vault_env 已含 SCRIPTS_DIR 副作用（1960 迁 .nexus/xdr/scripts）
from nexusos_xdr import _vault_env as _ve  # noqa: F401
_ve.setup_sys_path()

# === busy 标记（dispatch.sh Step 1 实测 + 2026-07-19 Task 274 扩展）===
# Task 274 扩展：补 kimi/omp/ac 实际 busy 标志（之前漏导致 patrol 误判 DONE）
# Task 275 修正：移除 ⏵|⏸（ac.py _WORKING_PATTERN 用 Working|Running|Percolating|Noodling
#   作为真实工作标志；⏵ 是 ac 状态栏常驻"auto 模式"标志，与 busy 无关，会永久误判 busy）
_BUSY_PATTERNS = re.compile(
    r"esc interrupt|Percolating|Noodling|Working"
    # Braille 旋转（oc/opencode 原生）
    r"|⠙|⠹|⠸|⠼|⠴|⠦|⠧|⠇|⠏"
    # kimi 月相（思考中）
    r"|🌑|🌒|🌓|🌔|🌕"
    # kimi 工具调用/重试（pane 显示这些 = 正在工作）
    r"|●\s*Used|✗\s*Used|Used\s+Read|Used\s+Write|Used\s+Bash|Used\s+Grep"
    # kimi 错误重试（卡死循环也视为 busy，防误判 done）
    r"|Model hit max_tokens|model response ended"
    # kimi 完成提示后思考下一步（"✨ 继续" 是过渡状态非真完成）
    r"|✨\s*继续"
    # omp 半月
    r"|◐|◑|◓"
    ,
    re.I,
)

# Task482: shell prompt 检测——agent TUI 崩溃退到 zsh/bash shell prompt 时，
# pane 显示的不是 TUI 而是裸 shell（任务命令会被 shell 当普通命令执行 → command not found）。
# 保守设计：只匹配 "用户名@主机名 路径 符号" 结构（zsh/bash 默认 prompt），
# 不匹配单独的 %/$/# （TUI 输入框也可能有这些字符）。
_SHELL_PROMPT_PATTERNS = re.compile(
    # zsh 默认 prompt: wang@MacBook-Pro ~ %  /  wang@MacBook-Pro /path %
    r"\w+@[\w\-]+\s+\S+\s*[%$#]\s*$"
    # bash 默认 prompt: wang@MacBook-Pro:/path$
    r"|\w+@[\w\-]+:\S+[%$#]\s*$"
    # 崩溃信号（TUI abort 后通常显示崩溃原因）
    r"|zsh:\s*(abort|segmentation\s+fault)"
    r"|(Segmentation fault|Traceback \(most recent)"
    ,
    re.I | re.MULTILINE,  # Task482-patch1: re.MULTILINE 让 $ 匹配每行末尾（多行 pane 真实场景）
)

# 输入行前缀形态（CodeWhale ❯ / kimi │> 等 TUI 的草稿输入行）——
# Task1269 草稿正向证据判据（与 steer.sh draft_check 同源口径）
_INPUT_PROMPT_RE = re.compile(r"^\s*[│┃]?\s*[>❯]\s*")

# CodeWhale 0.9.12 详情查看器页脚（空框 Enter 误开的覆盖层）
# 覆盖态下一切 UI 判据失明（busy 标记被盖），调用方须先清理或中止
_VIEWER_FOOTER_RE = re.compile(r"q/Esc\s+close", re.I)

# xdr callback 端点（P1-1 Task 277：19999 半退役，回调走 task_state 单一真相源）
NOTIFY_BASE = "http://127.0.0.1:19998"  # 原 :19999 已退役，改走 xdr /callback


class AgentBase:
    """所有 Agent 的基类。

    子类必须实现: ensure_no_popup()（A 方案预授权）
    子类可覆盖:   send_task()（提交差异）/ rescue_keys() / popup_patterns()
    """

    # === 静态属性（子类覆盖）===
    name: str = "base"
    tmux_session: str = "base"
    agent_type: str = "unknown"  # opencode/kimi/deepseek/atomcode

    # 审批策略（调用方声明，决定它调用别人时怎么审批）
    # auto_approve_all: 全自动（默认，删除护栏兜底）
    # auto_approve_except: 除危险操作外自动
    # manual: 每个都问
    approval_policy: str = "auto_approve_all"

    # P1-6: 小弟 hooks 层（v0.4 §三 P4 分层护栏第二层）
    # 设计要求每个小弟自己的 hooks/wrapper 拦截 TUI 内部命令。
    # 当前所有子类未实现 hooks 注入 → 删除护栏只覆盖 dispatchd 层（第一层）。
    # 子类实现 hooks 注入后改为 True。
    HOOKS_IMPLEMENTED: bool = False

    # === tmux 工具 ===

    def _tmux(self, args: List[str], timeout: int = 5, check: bool = False) -> subprocess.CompletedProcess:
        """执行 tmux 命令。"""
        cmd = ["tmux"] + args
        return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, check=check)

    def capture_pane(self, lines: int = 30) -> Optional[str]:
        """抓取 tmux pane 末尾 N 行。

        Task 280 P1-13: 失败返回 None（原返空字符串）。
        区分"失败"与"空 pane"——is_idle 拿空串会判 idle=True（误判），
        拿 None 时调用方应保守判非 idle。

        Task 285 修正: tmux `-S -N` 语义是"从光标上方 N 行开始抓到屏幕底"，
        实际抓 30+ 行不是 N 行。改用 `-S -{N*3}` + Python tail N 行后处理，
        确保只返回真正末 N 行（避免历史活动日志污染 is_idle 判定）。
        """
        try:
            # 多抓一些再 tail（防 tmux -S 边界），最后只保留末 lines 行
            fetch_lines = max(lines * 3, 30)
            r = self._tmux(["capture-pane", "-t", f"={self.tmux_session}:", "-p", "-S", f"-{fetch_lines}"])
            output = r.stdout
            if not output:
                return output
            # tail lines 行（按 \n 分割）
            all_lines = output.split("\n")
            return "\n".join(all_lines[-lines:]) if len(all_lines) > lines else output
        except Exception as e:
            logger.warning(f"[{self.name}] capture_pane 失败: {e}")
            return None  # Task 280 P1-13: 失败返 None（不再返空串）

    def send_keys(self, keys: List[str]) -> bool:
        """发送按键到 tmux session。

        P1-new-2(oc) 修正: 加 _send_lock 防 patrol check_popup 和主线程 send_task 按键交错。
        Task 280 P1-12: _send_lock 改模块级（原 hasattr 懒初始化双检锁失效——
          两个线程同时进 send_keys，hasattr 检查都 False，创建两个 Lock）。

        Args:
            keys: 按键列表，如 ["请读取任务", "Enter"] 或 ["Right", "Enter"]
        """
        try:
            with _SEND_LOCK:  # Task 280 P1-12: 模块级 Lock（所有实例共享，无双检锁问题）
                self._tmux(["send-keys", "-t", f"={self.tmux_session}:"] + keys)
            return True
        except Exception as e:
            logger.error(f"[{self.name}] send_keys 失败: {e}")
            return False

    def has_session(self) -> bool:
        """tmux session 是否存在。"""
        try:
            r = self._tmux(["has-session", "-t", f"={self.tmux_session}"], timeout=3)
            return r.returncode == 0
        except Exception:
            return False

    # === 状态检测 ===

    def is_idle(self) -> bool:
        """检查是否空闲（不在执行任务）。

        基于 dispatch.sh Step 1: capture-pane 末 3 行 grep busy 标记。
        命中 busy → 不空闲（正在工作）。
        P2-9 修正: session 存在 + pane 空 = 刚启动 = idle（不再误判为不空闲）。
        Task 280 P1-13: capture_pane 失败返 None 时保守判非 idle（避免 tmux 挂掉误判 idle）。
        """
        if not self.has_session():
            return False
        tail = self.capture_pane(lines=3)
        if tail is None:
            # P1-13: capture_pane 失败（tmux 异常）→ 保守判非 idle，避免 dispatch 到挂掉的小弟
            return False
        if not tail.strip():
            # P2-9: session 在但 pane 空 → 刚启动渲染中，视为 idle
            return True
        if _BUSY_PATTERNS.search(tail):
            return False
        # Task482: shell prompt 检测——TUI 崩溃退到 shell prompt 时，不是 idle 而是 TUI 挂了
        if _SHELL_PROMPT_PATTERNS.search(tail):
            logger.warning(f"[{self.name}] 检测到 shell prompt（TUI 可能已崩溃退到 shell），判非 idle")
            return False
        return True

    def _input_has_draft(self, sent_text: str, lines: int = 8) -> bool:
        """正向证据判定：已发送文本是否仍滞留输入框（=未提交）。

        Task1269 重写（处女原则，整体替换旧 _is_input_box_visible 的错误
        语义）：旧函数以「输入框边框可见」判「未提交」——CodeWhale 0.9.12
        输入框（❯ Type a follow-up…）运行中恒可见 → 恒判未提交 → 盲发
        Enter → 空框 Enter 打开详情查看器遮罩整 pane。
        新语义：最近 N 行的输入行（❯/│❯ 形态）里仍含已发送文本的锚串
        （前后各 20 字，空白归一比对——长文本在框内滚动截断时尾部锚兜底）
        才算草稿滞留；占位符/无输入行=已提交。抓不到 pane = 不确定 =
        按已提交处理（绝不盲补 Enter）。
        """
        tail = self.capture_pane(lines=lines)
        if not tail:
            return False
        norm = re.sub(r"\s+", "", sent_text)
        if len(norm) < 4:
            return False  # 文本太短无区分度，按已提交（不盲补）
        anchors = {norm[:20], norm[-20:]}
        for line in tail.splitlines():
            m = _INPUT_PROMPT_RE.match(line)
            if not m:
                continue
            content = re.sub(r"\s+", "", line[m.end():])
            if any(a and a in content for a in anchors):
                return True
        return False

    # === A 方案：预授权（子类必须实现）===

    def ensure_no_popup(self) -> None:
        """启动/发任务前调用，确保 TUI 不弹审批。

        每个小弟的 yolo/config 预授权方式不同，子类必须覆盖。
        默认实现: 无操作（诚实标注"未实现 A 方案"）。
        """
        logger.debug(f"[{self.name}] ensure_no_popup: 默认无操作（子类应覆盖）")

    # === B 兜底：弹窗检测+救援 ===

    def rescue_keys(self) -> List[str]:
        """救援键序，命中弹窗时发这个。

        默认 ["Right", "Right", "Enter"]（选 Allow Always）。
        子类可覆盖: ds/ds2 用 ["a", "a"]，kimi 用 ["Enter"]。
        """
        return ["Right", "Right", "Enter"]

    def popup_patterns(self) -> List[str]:
        """弹窗检测关键词（正则）。仅 B 兜底用。

        默认从 tools.yaml 读，读不到返回空。
        """
        return []

    def check_popup(self) -> bool:
        """检测当前 pane 是否有审批弹窗，命中则自动发救援键。

        缺陷3修复: task_monitor 路径不稳（API 签名变化会 TypeError），
        改为关键词检测为主路径（更可靠），task_monitor 仅作可选增强。
        """
        # 主路径: 本类关键词检测（可靠，无外部依赖）
        patterns = self.popup_patterns()
        if patterns:
            tail = self.capture_pane(lines=10)
            for p in patterns:
                if re.search(p, tail, re.I):
                    logger.info(f"[{self.name}] 检测到弹窗（pattern: {p}），发救援键 {self.rescue_keys()}")
                    self.send_keys(self.rescue_keys())
                    return True

        # 增强路径: task_monitor（可选，失败静默降级）
        try:
            from task_monitor import check_permission_popups, load_tools_config  # type: ignore
            if not hasattr(self, "_tools_cfg"):
                self._tools_cfg = load_tools_config()
            # 注意: check_permission_popups 不接受 session_filter 参数
            hits = check_permission_popups(self._tools_cfg)
            return hits > 0
        except Exception:
            # task_monitor 不可用或 API 不兼容 → 主路径已处理，这里静默
            return False

    # === 发任务（子类可覆盖提交差异）===

    def build_task_text(self, task_path: str, task_id: str) -> str:
        """构造发往 TUI 的指令文本。

        Task 279 P1-6: task_path 可能是 dispatched 副本（路径含 task.dispatched.<task_id>.md）。
        小弟读副本时拿到的 task_id 已是 xdr-ac-XXX 格式，与 task_state 主键一致。
        回调时用这个 task_id（不是任务书 hardcode 的值）。
        """
        return f"请读取并执行以下任务书文件: {task_path}"

    def build_callback_instruction(self, task_id: str) -> str:
        """构造 done 回调指令（让小弟自己发回调）。

        修正 dispatch.sh Step 5 的 bug: source 字段用真实 tool 名。

        P1-1 Task 277 (2026-07-20)：回调 URL 从 :19999/report 改为 :19998/callback，
        走 task_state 单一真相源。格式兼容（task_id/source/session/status/summary）。

        注意：这个函数实际上未被调用——回调指令当前是写进任务书正文。
        但仍要改保持代码与架构一致。
        """
        return (
            f'IMPORTANT: When done, send callback: curl -s -X POST '
            f'{NOTIFY_BASE}/callback -d '  # /report → /callback, NOTIFY_BASE → :19998
            f'\'{{"task_id":"{task_id}","source":"{self.name}",'
            f'"session":"{self.tmux_session}","status":"done",'
            f'"summary":"done"}}\''
        )

    def submit_keys(self) -> List[List[str]]:
        """提交按键序列（每个元素是一次 send_keys 调用）。

        默认: 单次 ["Enter"]。
        子类覆盖:
          - ds/ds2: CodeWhale 0.9.12 单批即提交（实际序列在重写的 send_task）
          - ac: sleep 2 后补 Enter
        """
        return [["Enter"]]

    def send_task(self, task_path: str, task_id: str) -> bool:
        """发送任务书到 tmux session。

        回调指令不再额外 send-keys（会导致小弟把它当普通文字留在输入框）。
        回调要求必须写进任务书正文（参考 07-Templates/Task-Dispatch-Template.md）。
        dispatchd 的 patrol 线程兜底检测完成（三条件输出检测 + 19999 回调查询）。

        步骤：
        1. is_idle 二次确认（dispatchd 已持锁）
        2. send-keys 任务文本（任务书路径）
        3. send-keys 提交键（子类处理差异）
        4. 提交状态验证 + 重试

        注意：task_id 上下文文件由 dispatchd.py 主流程写（非 send_task），
        避免 AC 等子类重写 send_task 不调 super 时绕过。
        """
        # Step 1: is_idle 二次确认
        if not self.is_idle():
            logger.error(f"[{self.name}] send_task 失败: session 不空闲")
            return False

        # Step 2: 发任务文本
        task_text = self.build_task_text(task_path, task_id)
        if not self.send_keys([task_text]):
            return False
        time.sleep(2)

        # Step 3: 提交按键（子类差异）
        for keys in self.submit_keys():
            self.send_keys(keys)
            time.sleep(1)

        # 不再额外 send-keys 回调指令——回调写进任务书正文，小弟能读到
        # patrol 会兜底：三条件输出检测 + 19999 回调查询

        # Step 4: 提交确认（Task1269 正向证据）——仅草稿滞留才补 1 次 Enter；
        # 禁盲补（CodeWhale 0.9.12 空框 Enter=打开详情查看器遮罩整 pane）
        time.sleep(1)
        if self._input_has_draft(task_text):
            logger.info(f"[{self.name}] 草稿滞留输入框，补 1 次 Enter")
            self.send_keys(["Enter"])
            time.sleep(2)

        # Task482: 派后即查——投递后检测 shell 报错（防 TOCTOU：is_idle 时 TUI 在，send_keys 时 TUI 崩了）
        time.sleep(2)
        try:
            post_tail = self.capture_pane(lines=5)
            if post_tail and re.search(r"command not found|zsh:|not found", post_tail, re.I):
                logger.error(f"[{self.name}] 派后即查失败: 投递到 shell（command not found），TUI 可能已崩溃")
                return False
        except Exception:
            pass  # fail-open: capture_pane 异常不阻塞投递判定

        return True

    # === 配置加载（tools.yaml）===

    @classmethod
    def load_tools_config(cls) -> dict:
        """从 tools.yaml 加载配置。

        返回 {tool_key: {tmux_session, popup_detection, rescue_keys, ...}}。
        支持 extends + overrides 嵌套合并。
        读不到返回空 dict。
        """
        if not _TOOLS_YAML.exists():
            return {}
        try:
            import yaml  # type: ignore
            with open(_TOOLS_YAML, encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
            tools = data.get("tools", {})
            defaults = data.get("defaults", {})

            def merge_overrides(base_cfg: dict, overrides: dict) -> dict:
                """递归合并 overrides 到 base_cfg（overrides 优先）。"""
                result = dict(base_cfg)
                for k, v in (overrides or {}).items():
                    if isinstance(v, dict) and isinstance(result.get(k), dict):
                        result[k] = merge_overrides(result[k], v)
                    else:
                        result[k] = v
                return result

            # 处理 extends + overrides 继承
            resolved = {}
            for key, cfg in tools.items():
                cfg = dict(cfg or {})
                parent_key = cfg.pop("extends", None)
                overrides = cfg.pop("overrides", None)
                if parent_key and parent_key in tools:
                    parent = dict(tools[parent_key] or {})
                    parent.pop("extends", None)
                    parent_overrides = parent.pop("overrides", None)
                    # parent 的 overrides 先合并到 parent
                    parent = merge_overrides(parent, parent_overrides)
                    merged = {**defaults, **parent, **cfg}
                else:
                    merged = {**defaults, **cfg}
                # 最后应用自己的 overrides
                merged = merge_overrides(merged, overrides)
                resolved[key] = merged
            return resolved
        except ImportError:
            logger.warning("PyYAML 未安装，tools.yaml 配置不可用")
            return {}
        except Exception as e:
            logger.warning(f"加载 tools.yaml 失败: {e}")
            return {}

    @classmethod
    def config_for(cls, tool_name: str) -> dict:
        """获取指定小弟的配置（带 extends 解析）。"""
        cfg = cls.load_tools_config()
        return cfg.get(tool_name, {})

    # === repr ===

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} name={self.name} session={self.tmux_session} type={self.agent_type}>"
