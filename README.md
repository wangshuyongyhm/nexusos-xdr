# nexusos-xdr

**NEXUS-XDR — Agent 调度总线（dispatch / callback / presence / patrol + MCP :19998）**

多 Agent 体系的通信协议层与任务调度中枢：主控 Agent 通过它把任务派给执行 Agent（写任务书 → dispatch 注册 → 等待回调 → 验收），执行 Agent 通过它回报进度与结果。所有任务状态走单一真相源 `task_state`，派任务/回调/监控/救援全链路可追踪。

## 核心特性

- **任务全生命周期**：`/dispatch` 注册任务（签发 dispatch/search 凭据、自动注入相关经验）→ `started/progress/done/failed/need_decision` 四态回调 → patrol 监控超时/卡死/完成检测
- **MCP + HTTP 双协议**：同一服务（默认 `127.0.0.1:19998`）同时暴露 MCP 工具面（`/mcp`，Agent 直接调）和 HTTP API（`/dispatch`、`/callback`、`/task/<id>`，脚本/任意语言可调）
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

## 配置样例（bring your own agent drivers）

发布包不含 `agents/` 驱动层（CLI Agent 的 tmux 适配器、弹窗救援等与具体工具链强绑定）——接入自有 Agent 驱动时：

- 参照 `agents` 驱动的接入协议（register/callback/task_state 三接口）实现适配器，字段含义见服务端 API 文档；
- 驱动所需的外部工具配置（如启动脚本路径、弹窗关键词）由驱动自带，服务端核心不读取；
- 核心 HTTP 链（`/dispatch` 注册、`/callback`、`presence`）不依赖任何驱动配置，无驱动也可用。

## 与另两件的关系

nexusos-xdr 是三件套的**编排者**，声明依赖另外两个：关键动作过 nexusos-gate 门禁（dispatch/done 场景的强制检查），dispatch 时从 nexusos-mem 检索注入相关经验、回调时回填经验套用结果。反向地，gate/mem 内部有少量函数级懒 import 本包的 session_state / credential（历史耦合，批4 后逐步解耦）。

## License

MIT License。详见仓库根目录 [LICENSE](./LICENSE)。
