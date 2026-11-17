#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""nexusos_xdr.patrol.state — PatrolState：patrol 监控循环的可变状态显式对象（Task609 Step B）。

收敛背景（#57 批次最难项）：
    原 _start_patrol_thread 内十个单元素 list（`x = [0]` 惯用法）作为可变状态
    跨闭包传递（外层定义 → _check_output_completion / patrol_loop 闭包读写）。
    开源可读性最差 + 行为风险最高（MEM#59000 风险点）。

使用域分析（收敛判定依据，全部跨闭包共享 → 全部收敛）：
    idle_confirmation_count   → _check_output_completion + patrol_loop reset 语义
    last_pane_hash/stuck_count/last_stuck_notify_time/death_rounds
                              → patrol_loop（stuck 检测三态 + 死亡轮计数）
    step_count/last_window_tool_calls/cost_so_far
                              → patrol_loop（Task 297 cap 计数）
    step_cap_fired_at/cost_cap_fired_at
                              → patrol_loop（reminder 单次注入闸）

线程/并发语义：
    单 patrol 线程内使用（daemon 线程私有），无跨线程共享——dataclass 无锁。
    🔴 同对象纪律（T601 教训）：实例由 _start_patrol_thread 创建一次，
    所有闭包共享同一实例（`state` 名绑定），禁止中途替换/复制。
"""
from dataclasses import dataclass, field
from typing import Optional, Set


@dataclass
class PatrolState:
    """单个 patrol 监控线程的可变状态（原十个单元素 list 的显式收敛）。"""

    # === 场景1: done 三重确认（Task 274/303/08040834 四重）===
    idle_confirmation_count: int = 0          # 连续 idle 确认次数（reset 语义保留）

    # === 场景2: stuck 检测（Task 273 P1-2）===
    last_pane_hash: Optional[str] = None       # 上一轮 pane 哈希（None=首轮）
    stuck_count: int = 0                        # 连续无变化轮数
    last_stuck_notify_time: float = 0.0         # 上次 STUCK 通知的 monotonic 时间（冷却用）

    # === S3 (Task315): 进程死亡检测 ===
    death_rounds: int = 0                       # 连续 has-session 失败轮数（2 轮才合成 interrupted）

    # === Task 297 P0-1/P0-2: loop 安全控制 ===
    step_count: int = 0                         # 工具调用差量累计（单调）
    last_window_tool_calls: Set[str] = field(default_factory=set)  # 上一轮工具调用集合
    cost_so_far: float = 0.0                    # 累计成本（pane 解析，fail-open）
    step_cap_fired_at: Optional[float] = None   # step cap 触发 monotonic 时戳（None=未触发）
    cost_cap_fired_at: Optional[float] = None   # cost cap 触发 monotonic 时戳

    # === Task 1849: cost 读数背离去噪（宁可漏杀不可误杀）===
    # 10-19 1842 误杀：pane 快照 $0→$59 噪声跳变直接 ratchet 进 cost_so_far
    # 驱动 cap-kill。背离读数不进 cost_so_far，只记 WARNING 告警；连续
    # RISING_ROUNDS 轮「背离且比上轮更高」才升级为可信（真实暴涨读数在爬升，
    # UI 噪声跳变后恒定——用增量连续性区分两者）。
    cost_divergent_count: int = 0               # 连续背离轮数（告警/诊断用）
    cost_divergent_rising: int = 0              # 背离期连续「读数比上轮高」轮数
    cost_last_divergent_reading: float = 0.0    # 上轮背离读数（rising 判定基准）
