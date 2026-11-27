# Changelog

All notable changes to the nexusos packages are documented in this file.

格式：Keep a Changelog 1.1；四包 lockstep 单文件共享（同一版本号 = 四包同版本），
每个版本下按 `### nexusos-common / gate / mem / xdr` 四节分账，节内按
Added / Changed / Deprecated / Removed / Fixed / Security 分组。
版本策略与 layout_version 轴见各包 README 与 design §7（task/1955）。

## [1.2.0] - 2026-11-27

> 四包 lockstep 版本线：gate / mem / xdr 升 1.2.0；common 本轮无改动，仍为 1.0.0。
> 本节每一条都对着「三个发布仓 `git diff --name-status origin/master HEAD`」的清单写（已发布 1.1.0 → 本次快照，按 tree 比）；清单原文见开源主仓本次同步的任务产出 `changelog-basis-a-dist-repos.txt`。主仓白名单补全带来的新增（主仓副本追赶）另见 `changelog-basis-b-oss-main-repo.txt`，不混进发布仓的账。

### nexusos-common

无改动（1.0.0）。

### nexusos-gate

**Added**
- `etc/gates/_checks/commit_file_cap.py`：单笔提交文件数上限检查
- `etc/gates/_checks/session_log_selfcheck.py`：会话日志自检
- `scripts/staged_tests_grouped.py`：代码门禁 C5 的分组跑测试助手

**Changed**
- `gate_engine.py`：事件签名、派单真拦、被拦欠账显形；新增 `RETIRED_SCENARIOS` 注册表——调用已下线场景（`loop_start` / `loop_complete` / `deploy-sync`）返回明确的「已下线」说明而非静默通过
- `etc/gates/_checks/done_ack.py`、`outcome_backfill.py`、`pool_rows_flipped.py`、`persist_pool_ledger.py`、`pool_write_channel.py`、`doc_symbol_refs.py`：随门禁核心口径同步调整（`pool_rows_flipped.py` 另去掉一处对已退役检查器的 docstring 引用）
- `etc/gates/_checks/tests/test_2611111773_g1_annotation.py`：随上述调整同步
- `README.md`：示例路径改为随包的 `etc/checks/examples/`（原先写的 `./examples/gates/` 在装上的包里不存在）

**Removed**
- 下线三个门禁场景及其专属检查器（`loop_start` / `loop_complete` / `deploy-sync`；`agent_archive_updated.py`、`deploy_synced.py`、三份场景 YAML）——源码里已移出可恢复，本次发布仓快照里不再包含

### nexusos-mem

**Added**
- `redact_credential.py`：入库时把凭据号替换成固定占位——切片入库的两个入口与写入接口的两个入口走同一个函数、同一个占位，替换不拒
- `machine_identities.py`：机器身份——机器名单里的名字不签发检索凭据

**Changed**
- `search.py`：全文检索语句不再全表扫描（短查询走窗口近路；先看旧语句的执行计划，不需要就沿用）；重排的冷却只留给「服务坏了」，忙就当次降级返回并带重试等待时间，响应里说明这一次有没有重排
- `rerank_server.py`：重排冷却与降级口径同步
- `embed_server.py`：向量服务按固定形状补齐（20 档）、按模式清缓存、内存越线时的自愈重启；`embed_corpus.py` 随之同步
- `api_server.py`：给结果接口校验凭据与记录归属；重请求并发闸 503 计数在 `/health` 暴露 `busy_rejects`；403 凭据缺失的提示语写明「去哪拿」
- `apply_honesty.py`、`shared.py`：检索凭据的签发判断收拢到唯一的函数
- `context_capture.py`、`skip_rules.py`、`scripts/backfill_identity.py`：随上述口径同步
- `README.md`：加「已知限制」节——独立发布的 mem 包不随包带建表语句，先用开源主仓 `db/` 目录里的结构与迁移建库

**Fixed**
- `search.py`：检索语句出错后的退路三处缺陷——缺回滚、事务已中止属于内部错误、回滚后语句超时要重设

**Security**
- `api_server.py`、`apply_honesty.py`、`shared.py`：结果接口校验凭据与记录归属；签发判断收拢

### nexusos-xdr

**Added**
- `ack_policy.py`：派单的人签收、签收入台账；投递回执与签收分成不同的字段
- `callback/cred_gate.py`：回调必须出示派单凭据（开关默认只记录）
- `proc_origin.py`、`proclog_filter.py`：服务端查每个写请求的进程来历（只记录）
- `hooks/sensitive_ops.py`、`hooks/sensitive_ops_guard.py`：敏感操作守卫
- `stall_watch.py`：停滞盯守；轻推脚本路径 `NEXUS_STEER_SCRIPT` 优先、未设用默认
- `task_aliases.py`：任务号别名
- `email_inbound.py`：邮件入站——待决策通知变成邮件回调（依赖的各 Agent 钩子脚本不随开源版发行）

**Changed**
- `ack.py`、`callback/handler.py`、`callback/outcome_autofill.py`、`credential.py`、`dispatch/main.py`、`dispatchd.py`、`task_state.py`、`session_state.py`：统一回调命令与统一签收命令的身份与凭据处理；投递回执与签收分字段
- `patrol/loop.py`：巡检判完成的时机
- `mcp_server.py`：删掉 `loop_start` 的死钩子（`_on_loop_start_pass`）与它的注册；`mcp_auth.py`、`httpapi/routes.py`、`presence/routes.py` 随凭据与身份口径同步
- `hooks/broadcast_guard.py`、`completion_guard.py`、`interaction_guard.py`、`selfcheck_evidence_guard.py`、`state_transition_guard.py`：守卫口径同步
- `exp_injector.py`、`needdecision_notify.py`、`notify_push.py`：通知链路同步
- `p02_hardblock.py`、`layer2_calibrate.py`、`fault_http_server.py`、`tools/registry.py`：随上述口径同步

## [Unreleased]

### nexusos-common

**Added**
- anchors 锚点解析包：`nexus_home` / `vault_root` / `task_root` / `subpath` / `expand_env`，显式报错 + WARN 降级规则（task 1957）
- 私路径 lint 扫描器 `nexusos_common.lint`（15 pattern 数据文件化），基线 204 hits/13 类与 audit §1 对账（task 1957）
- `LAYOUT_VERSION` / `SUPPORTED_LAYOUTS` 布局版本轴常量——独立于包 semver 的目录布局版本，与 `.nexus/config.toml [meta] layout_version` 对账（task 1963）

### nexusos-gate

**Changed**
- `_vault_env` / `gate_engine._ensure_vault_root` / `_checks/_vault_root.py` 锚点解析改调 nexusos-common，parents[N] 内联推导删除（不留双源）（task 1957）
- 五私人 check OSS 化处置：config_key_refs/codegraph_wrapper 插件化、persist_pool_ledger/doc_consistency 示例化、e2e_suite_evidence 配置迁 `$NEXUS_HOME/gate/e2e-paths.yaml`（无配置 = WARN skip 非 REJECT）（task 1959）
- 包更名 `nexus-gate` → `nexusos-gate`（目录/模块/pyproject/import 一次切净，无 shim）（task 1962）
- 版本 0.1.0 → 1.0.0，加入四包 lockstep 版本线（task 1963）

**Added**
- `nexusos.gate.checks` entry_points 插件协议：engine sorted(name) 稳定序加载 + `checks.d` 声明式启用（task 1959）

### nexusos-mem

**Changed**
- `_vault_env` 锚点解析改调 nexusos-common，parents[N] 内联推导删除（task 1957）
- 包更名 `nexus-mem` → `nexusos-mem`（目录/模块/pyproject/import 一次切净，无 shim）（task 1962）
- 版本 0.1.0 → 1.0.0，加入四包 lockstep 版本线（task 1963）

### nexusos-xdr

**Fixed**
- events 四事故修复：迁移函数 samefile+realpath 双防线防自引用环、guard 转 deque tail 读（治 readlines 整读假死）、消费者全盘点对账（monitor 三脚本 + mcp_server gate-rollup + checks.yaml 同修）（task 1958）

**Changed**
- `~/.hermes` 家族 15 条映射迁移 `.nexus`（env 单源 + 幂等 `nexus migrate`，三次重跑 0 MOVED）（task 1960）
- 包更名 `nexus-xdr` → `nexusos-xdr`（目录/模块/pyproject/import 一次切净，无 shim）（task 1962）
- 版本线与四包 lockstep 对齐 1.0.0（task 1963）

**Added**
- 运维 CLI `python3 -m nexusos_xdr.cli`（update/doctor）：PyPI 版本比对、layout_version 迁移提示、one-liner 升级；doctor 六服务/DB/守卫/版本一致性对账（`nexus` 命令名归 nexusos-common 向导 CLI——1963/1965 并行撞名收敛，xdr 不再声明 scripts 入口）（task 1963）
- tag 驱动发布流：GitHub Actions push tag `v*` → build → TestPyPI（正式 PyPI 为用户授意节点，不自动）（task 1963）

### 迁移说明

- 0.x 版本线无兼容义务（design §7：v1.0 全新起跳）。包名更名后旧名 `nexus-gate/mem/xdr` 不再发布。
