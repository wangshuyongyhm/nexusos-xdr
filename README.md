# nexusos-xdr

**NEXUS-XDR — Agent 调度总线（dispatch / callback / presence / patrol + MCP :19998）**

多 Agent 体系的通信协议层与任务调度中枢：主控 Agent 通过它把任务派给执行 Agent（写任务书 → dispatch 注册 → 等待回调 → 验收），执行 Agent 通过它回报进度与结果。所有任务状态走单一真相源 `task_state`，派任务/回调/监控/救援全链路可追踪。

## 核心特性

- **任务全生命周期**：`/dispatch` 注册任务（签发 dispatch/search 凭据、自动注入相关经验）→ `started/progress/done/failed/need_decision` 四态回调 → patrol 监控超时/卡死/完成检测
- **P0-1 ACK 回执链（2611111762）**：dispatch 副本自动注入回执协议（执行者收到任务书第一动作 `POST /ack` 带幂等键）；`ack` 字段状态机 pending→acked，超窗无回执无物证→同键重投一次→仍无→`delivery_failed` 终态（ACK 窗 300s 与执行超时分离）；steer 同幂等键（`POST /steer` 登记 + `/ack kind=steer` 回执，jsonl 台账）；取代 patrol 旧盲重投（180s 无回调即重投的双投源，#615 探针实证）
- **MCP + HTTP 双协议**：同一服务（默认 `127.0.0.1:19998`）同时暴露 MCP 工具面（`/mcp`，Agent 直接调）和 HTTP API（`/dispatch`、`/callback`、`/ack`、`/steer`、`/task/<id>`，脚本/任意语言可调）
- **PRESENCE 在场感知**：`session/register`、`session/active`、`session/events`、op-log 操作意图声明——多主控并存时的冲突检测与协调基础
- **凭据完整性体系**：dispatch 契约凭据 + search 契约凭据签发/校验/防伪/定时清理（cred_reaper）
- **门禁集成**：关键动作前自动过 nexusos-gate（dispatch 前查任务书规范、done 前查五层落盘）
- **删除护栏**：`guards` 模块提供全体系唯一硬底线防护

## 安装

```bash
# 发布至 PyPI 前：本地路径安装（仓库根目录执行；三包目录并列时同命令安装三件套）
pip install ./nexusos-gate ./nexusos-mem ./nexusos-xdr
# 验证（1597 干净 venv 实证口径：十模块 import 全绿）
python -c "import nexusos_xdr.mcp_server, nexusos_xdr.mcp_auth, nexusos_mem, nexusos_gate; print('ok')"
```

要求 Python >= 3.10。**依赖 nexusos-gate**（体系内本地包，须同环境安装）；`nexusos-mem` 为可选依赖（`[mem]` extra——PyPI 无此包，硬依赖会让干净环境安装失败；缺席时相关功能用到才报错并给出安装指引）。另依赖 `fastmcp>=3.4.4,<3.5`、`anyio>=4.0`、`starlette>=1.0`、`psycopg2-binary>=2.9`、`pyyaml>=5.4`。

PyPI 发布后可用 `pip install nexusos-xdr`（当前包未发布，请勿直接照抄）。

本包**没有** `[full]` extras（那是 nexusos-mem 的向量能力包）。运行时需要 PostgreSQL（凭据/任务状态落库）。

## 快速上手

一次完整的任务调度生命周期（HTTP 方式）：

```bash
# 1. 派任务（注册 task_id，写任务书后调用）
curl -s -X POST http://127.0.0.1:19998/dispatch \
  -H "Content-Type: application/json" \
  -d '{"task_dir":"/path/to/task/input/task.md","target_agent":"my-agent",...}'

# 2. 执行方回调进度
curl -s -X POST http://127.0.0.1:19998/callback \
  -H "Content-Type: application/json" \
  -d '{"task_id":"xdr-agent-a-xxx","source":"agent-a","status":"progress","summary":"..."}'

# 3. 查询任务状态
curl -s "http://127.0.0.1:19998/task_state?agent=agent-a"

# 3.5 执行者投递回执（P0-1 ACK 链：任务书副本顶部的回执 curl 原样执行即可）
curl -s -X POST http://127.0.0.1:19998/ack \
  -H "Content-Type: application/json" \
  -d '{"task_id":"xdr-agent-a-xxx","key":"ack-xxxx","agent":"agent-a","kind":"dispatch"}'

# 4. 在场感知：注册自己 + 看谁在线
curl -s -X POST http://127.0.0.1:19998/session/register \
  -H "Content-Type: application/json" \
  -d '{"agent":"my-agent","role":"executor","session_id":"...","status":"init"}'
curl -s http://127.0.0.1:19998/session/active
```

Python 方式（门禁便捷导出）：

```python
from nexusos_xdr import gate_check
result = gate_check(scenario="dispatch", context={...})
```

## 配套脚本（`~/.nexus/xdr/scripts/`，非包内，pip 不安装）

派任务链路上的两个客户端脚本，语义以本文为准（2611172042 B2 / 2611172044 B4 后）：

- `wait_callback.sh`：主控侧等待/感知通道。**超时只发 `timeout_warn` 告警，不改 task_state**（终态判定权归 patrol 的 grace 区间）；重挂时用「挂载快照（status/callbacks 长度/末条 ts）」识别残影——挂载即终态且无新回调会继续等待而不是秒退重消费；tmux 目标一律精确（`has-session -t "=name"`、`send-keys`/`capture-pane -t "=name:"`）。
- `steer.sh`：救援投递 + 消费验证。判据四通道：草稿滞留判否（kimi/kimi2 只认框线内 `│ >` 草稿，裸 `❯` 回显排除）/ 推理态指示（braille spinner + `ctrl-s to add guidance`）/ transcript 新增行 / CodeWhale 排队态；窗口 5 拍×2s、失败自动重投 1 次。脚本单源在 `~/.hermes/scripts/steer.sh`（`~/.nexus/xdr/scripts/steer.sh` 为 symlink），`--dry-run` 只打印不投递。

## 依赖分层

| 层 | 内容 |
|----|------|
| 硬依赖 | nexusos-gate（体系内）、fastmcp / anyio / starlette / psycopg2-binary / pyyaml |
| 可选依赖 | nexusos-mem（`[mem]` extra——检索注入/经验回填用，缺席容错） |
| 数据层 | PostgreSQL（凭据、任务状态、校准数据） |
| 服务层 | 本服务 :19998；联动 nexusos-mem api :11537 / embed :11436 |
| 包结构 | `mcp_server` 双协议入口 + `dispatch` / `callback` / `presence` / `patrol` / `tools` 五大子包（`agents/` 驱动层不在发布包内——bring your own agent drivers，见下） |

## 环境变量（常用）

| 变量 | 默认值 | 作用 |
|------|--------|------|
| `NEXUS_VAULT_ROOT` | env > 包位置哨兵推导 > 占位 | Vault 根目录（任务书/落盘路径解析用，干净环境可缺省） |
| `NEXUS_OUTCOME_AUTOFILL` | `0`（关） | 经验套用 outcome 自动回填开关（`1` 开） |
| `NEXUS_P02_HARDBLOCK` | `1`（开） | P0-2 REJECT 硬拦开关（`0` 关，回退 Task492 只记 warning 行为） |
| `P02_POOL_FILE` | `<vault>/06-Memory/未决问题池.md` | 认领行目标池（测试缝，fixture 用） |
| `P02_EVENTS_URL` | `http://127.0.0.1:19998/session/event` | gate_blocked 事件端点（测试缝，fixture 用） |
| `NEXUS_P02_FALLBACK_LEDGER` | `<NEXUS_HOME>/xdr/gate-block-fallback.jsonl` | 池写失败降级台账（B5，测试缝 fixture 用） |

## P0-2 REJECT 硬拦（Task 2611111759）

done 回调后台 gate（`_bg_gate_check`）发现 REJECT 级 fail 时，`p02_hardblock`
模块在收口链上执行四件事（全部后台线程，callback 热路径零改动）：

1. **触发快照**：REJECT 明细 + 检查器版本/文件指纹（done.yaml sha256 + `_checks/*.py`
   mtime）+ gate 输出全量 JSON → 任务目录 `output/gate-block-snapshot.json`（原子写）。
   历史归因唯一解：主控代补抹形态/检查器活体修改致重放漂移（Task 2611111750 实证）。
2. **硬拦翻转**：`end_reason: completed → gate_blocked`（status 仍 DONE、result 不动；
   锁内 RMW，幂等——复核重扫不覆盖首次 `gate_block` 元数据）。
3. **复核通道（防死锁）**：blocked≠死，三路出路——owner 认领行入未决问题池
   （pool_write_channel 锁内 append）+ need_decision 回调记录（caller 可见）+
   need_decision 即时通知（Task909 降级链）。
4. **G16 联动**：监控侧 `check_G16_gate_residue.sh` 残留行升级带 owner/task/快照
   认领单（task 归属在消费侧从 task_state 反解，事件 detail 无 task_id）。

出路闭环（Task 2611111774 补全）：owner 修复产出重发 done → bg-gate PASS 分支
`maybe_clear_on_pass` 清账（end_reason 回 completed + `gate_block.cleared_at`
留痕 + 池行自动翻 [x] 闭环）——重发 done 即真销账；清账同步清 `p02_hardblocked`
幂等标志（生命周期对称：清账后可再拦）。无出路场景（任务被 FAILED/CANCELLED
抢先终态）硬拦自动放弃只留快照。幂等双保险（1774 P0②）：编排头 `once_flag`
挡同进程重复 done 副作用 + 池行 lookaround 锚挡跨进程重复入池；池写失败降级
jsonl 台账（B5，`NEXUS_P02_FALLBACK_LEDGER` 可覆盖）。

context 组装单点（Task 2611111775）：`handler._build_gate_context` 是 bg-gate
context 的唯一组装点——callback 载荷的 evidence/files/summary/experiences/
source 全量转发，与 CLI 直调 gate 等价。1774 残留边角实证：旧闭包内联组装漏
`evidence`（G7/EADC REJECT 级消费）致重发 done 场景降级文本检测、
`maybe_clear_on_pass` 不触发、出路断头只能函数直调清账；单点化后断链不可复发，
旧载荷无 evidence 键不注入（兼容判据不变）。

## 配置样例（bring your own agent drivers）

发布包不含 `agents/` 驱动层（CLI Agent 的 tmux 适配器、弹窗救援等与具体工具链强绑定）——接入自有 Agent 驱动时：

- 参照 `agents` 驱动的接入协议（register/callback/task_state 三接口）实现适配器，字段含义见服务端 API 文档；
- 驱动所需的外部工具配置（如启动脚本路径、弹窗关键词）由驱动自带，服务端核心不读取；
- 核心 HTTP 链（`/dispatch` 注册、`/callback`、`presence`）不依赖任何驱动配置，无驱动也可用。

## OTel GenAI semconv 对齐（2611111758）

事件流写入侧（`session_state.append_event` 单一咽喉点）自动为每条事件派生 `otel_*` 对齐字段——**只增不改**：既有字段（`ts` / `ts_local` / `agent` / `type` / `detail` / `target` / extra 透传键）名与语义零变动；调用方 extra 显式提供的 `otel_*` 优先（可覆盖派生值）。不引入 OTel SDK/采集器，纯字段对齐，未来接 Langfuse/LangSmith/Arize 等观测平台零改造。

| xdr 事件字段 | OTel GenAI semconv | 对齐方式 | 落地字段 |
|---|---|---|---|
| （常量） | `gen_ai.system` | 直映（产生事件的系统身份） | `otel_gen_ai_system="nexus"` |
| `agent` | `gen_ai.agent.name` | 直映 | `otel_gen_ai_agent_name` |
| `type` | `gen_ai.operation.name` | 语义映射（xdr 事件类型即操作语义） | `otel_gen_ai_operation_name` |
| `type` | `event.name` | 命名空间化 | `otel_event_name="nexus.<type>"` |
| （常量） | `event.domain` | 直映 | `otel_event_domain="gen_ai"` |
| `extra.session_id` / 注册表缓存 | `gen_ai.session.id` | 需补字段：extra 顶层优先，缺席时回退内存提示缓存（register/heartbeat 顺手维护，注销时清除），仍无则省略 | `otel_gen_ai_session_id` |
| `ts` | Span/Log 时间戳 | 直映（ISO 8601 UTC，OTel 原生可解析） | 不新增字段 |
| `detail` | event payload / attributes | NEXUS 特有，无强对应——保留原字段，平台侧自行映射 | 不映射 |
| `target` | （无对应） | NEXUS 特有（op 意图目标/冲突检测） | 不映射 |
| `ts_local` | （无对应） | NEXUS 特有（本地时区可读性） | 不映射 |

回溯说明：历史事件（本特性上线前）无 `otel_*` 字段——观测平台侧按缺省维度处理；字段派生只在写入时发生，不做存量回填。

## 与另两件的关系

nexusos-xdr 是三件套的**编排者**，声明依赖另外两个：关键动作过 nexusos-gate 门禁（dispatch/done 场景的强制检查），dispatch 时从 nexusos-mem 检索注入相关经验、回调时回填经验套用结果。反向地，gate/mem 内部有少量函数级懒 import 本包的 session_state / credential（历史耦合，批4 后逐步解耦）。

## License

MIT License。详见仓库根目录 [LICENSE](./LICENSE)。
