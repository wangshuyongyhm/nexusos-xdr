#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""notify_push — NEXUS 出向推送适配器（P1 通知层技术件，Task677）。

设计真源: task/649-agent-proxy-design/output/report.md §6（L158-213）。
通道定版（用户 08201125 手机端确认）: Server酱 主通道（已扫码绑定）+
TG 可插拔备用 + xdr alert 事件兜底。本单只做消息发出侧——接线 xdr 服务
属影子转正后的集成单；AGENT_PUSH_ENABLED 开关供集成单消费（§设计约束 9:
代理人自动推送转正前挡板，L1/L3 不受限）。

分级路由（2611192105 回调化重构，用户定向「有重要事立马汇报」）:
  即时档——事件重要性判定（非时间窗），四触发任一立即发一封人话邮件:
    ① 需用户决定（decision_required=true 或级别 L1）
    ② 资金交易（关键词：下单/成交/资金/买卖）
    ③ 不可逆操作预告（删除/迁移/覆盖/清理生产）
    ④ 体系红灯（REJECT 级告警：gate 连续 fail/服务 down/check error）
    静默时段(23:00-08:00)规则不变——仅 L1 语义穿透，其余降级进日报。
  汇总档——其余 L2/L3 全部进每日 17:30 一封日报（三区：✅已自动处理/
    ⏳挂起中/📊当日数字）；L2 30min 窗保留防抖，到期只转存不发邮件。
  L3 仍只入队列记录（同日条目出现在日报 ✅ 区）。

人话模板（两条邮件正文统一，禁机器行/JSON/路径堆砌）:
  [状态图标] <事项一句话>
  发生了什么：<1-2 句>
  需要你做什么：<具体指令或「无需动作」>｜急不急：<期限/不急>
  标题规范：即时档 `[需你决定] <一句话>`；汇总 `NEXUS 日报 <MM-DD>`

失败降级（649 §6.5）: HTTP 非 200/超时 → 退避 1m/5m/15m ×3 → 切备用通道
→ 双败落 xdr alert 事件。任何路径不允许静默丢待决。

凭据: Server酱 sendkey 从 $NEXUS_HOME/secrets/push_serverchan.key 读（只读；
值不进代码/日志/异常输出——URL 打印前 key 段脱敏 ***）。

纯标准库（urllib），无第三方依赖（psycopg2 仅调度锚定惰性导入）。CLI:
  python3 -m nexusos_xdr.notify_push --self-test       # 全 mock 自检
  python3 -m nexusos_xdr.notify_push --send-test       # 真发 1 条即时档（验收用）
  python3 -m nexusos_xdr.notify_push --daily-digest    # 发当日日报并重锚 17:30
  python3 -m nexusos_xdr.notify_push --register-digest # 仅注册/重锚调度行
"""
from __future__ import annotations

import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path

from nexusos_common import subpath, vault_root  # 1960：锚点单源（design §2.1 行5）

# ── 常量（全部路径参数化：锚点经 nexusos_common，零本机硬编码）──
# NEXUS_XDR_NOTIFY_DIR 是目录级锚点（行5b/6 两文件共用，design §2.1）
NOTIFY_DIR = Path(os.environ["NEXUS_XDR_NOTIFY_DIR"]) if os.environ.get("NEXUS_XDR_NOTIFY_DIR") else subpath("xdr/notify")
QUEUE_FILE = NOTIFY_DIR / "push_queue.json"
XDR_EVENT_URL = "http://127.0.0.1:19998/session/event"

SERVERCHAN_API = "https://sctapi.ftqq.com/{key}.send"
TG_API = "https://api.telegram.org/bot{token}/sendMessage"

L2_WINDOW_SEC = 30 * 60          # L2 防抖窗（2611192105：到期只转日报缓存，不发邮件）
SILENCE_START, SILENCE_END = 23, 8   # 23:00-08:00（本地时间，陷阱 6）
BACKOFF_SCHEDULE = (60, 300, 900)    # 指数退避 1m/5m/15m（单测 monkeypatch 置 0）
MSG_TTL_HOURS = 24               # 回复有效期

# ── 即时档触发词（2611192105 §A：事件重要性判定，非时间窗）──
FUNDS_RE = re.compile(r"下单|成交|资金|买卖")            # ② 资金交易
IRREVERSIBLE_RE = re.compile(r"删除|迁移|覆盖|清理生产")  # ③ 不可逆操作预告
RED_RE = re.compile(r"REJECT|不可连|服务\s*down|check\s*error|连续\d+轮|escalated",
                    re.IGNORECASE)                        # ④ 体系红灯
IMMEDIATE_TITLE_PREFIX = "[需你决定] "
_IMMEDIATE_ICON = {"decision": "❓", "red": "🔴", "funds": "💰",
                   "irreversible": "⚠️", "l1": "❗"}

# ── 汇总档（每日 17:30 一封，挂 nexus/scheduler.py 日频行）──
DIGEST_HOUR, DIGEST_MINUTE = 17, 30
DIGEST_TASK_NAME = "nexus-daily-digest"
XDR_EVENTS_URL = "http://127.0.0.1:19998/session/events"   # 📊 gate 统计数据源
SCHEDULER_DSN_DEFAULT = "postgresql://localhost/nexus_mem"  # 与 scheduler.py 同源
MONITOR_STATE_FILE_ENV = "NEXUS_MONITOR_STATE_FILE"          # 单测注入缝
PENDING_POOL_RELPATH = ("06-Memory", "未决问题池.md")  # ⏳ 挂起区数据源
# 2611230883：活跃视图退役 → 改读源文件；原值 ("06-Memory", "未决问题池-active.md")

# 2611230883：未决池活跃视图退役——读者一律读源文件，按状态列自己筛「未闭环」。
# 规则：七列规范行里状态格是 待办/进行中/待用户/挂起 之一 → 未闭环；
#       ✅已闭环 / ❌已取消 → 不算；没有状态格或状态不在六个取值里 → 按未闭环算（宁可多算）。
_OPEN_STATUSES = {"待办", "进行中", "待用户", "挂起"}
_ALL_STATUSES = _OPEN_STATUSES | {"✅已闭环", "❌已取消"}


def _pool_row_open(line: str) -> bool:
    cells = [c.strip() for c in line.strip().strip("|").split("|")]
    status = next((c for c in cells if c in _ALL_STATUSES), "")
    return status == "" or status in _OPEN_STATUSES

# ── 说明白标准（2611192240 §E，用户定向「多解释几个字会死？」）──
# 术语零裸奔：内部词当场展开；G 码仅 G14 有双源实证释义（任务书示例+
# test_gate_engine），其余用诚实通用释义，不臆造具体含义。
def expand_terms(text: str) -> str:
    """内部术语展开成人话（对不懂 NEXUS 的读者自包含可懂）。"""
    if not text:
        return text
    def _g(m):
        n = m.group(1)
        if n == "14":
            return "G14（任务书的产出登记段缺失或不合规）"
        return f"G{n}（质量门禁第 {n} 项检查）"
    text = re.sub(r"gate_blocked", "质量门禁拦截（任务产出没过自动检查）", text)
    text = re.sub(r"\bREJECT\b", "硬性不通过（必须修复才放行）", text)
    text = re.sub(r"\bWARN\b", "软性提醒（不拦截执行）", text)
    text = re.sub(r"\bneed_decision\b", "等你拍板的卡点", text)
    text = re.sub(r"G(\d+)", _g, text)
    return text


_WHY_DEFAULT = {
    "decision": "这条在等你最终拍板——只有你能定，Agent 不能替你决定",
    "l1": "高风险事项，按规则必须让你第一时间知道",
    "red": "体系健康红灯，持续发生会影响后续所有任务",
    "funds": "涉及资金/交易，按规则必须让你实时知悉",
    "irreversible": "即将执行不可逆操作，预告期内你有否决窗口",
}
_CONSEQ_DEFAULT = {
    "decision": "不处理：该任务会一直挂起，等你回复后才继续推进",
    "l1": "不处理：相关流程保持暂停，不会自行消失",
    "red": "不处理：异常持续累积，每天日报都会标记",
    "funds": "不处理：错过时效可能影响交易处理窗口",
    "irreversible": "不处理：到期后将按预告执行该操作",
}


def build_email_body(icon: str, subject: str, what: str, action: str,
                     urgency: str, kind: str = None, evidence: dict = None,
                     detail: str = "") -> str:
    """四要素分层正文（§E）：首段结论五要素 + 展开详情层。宁多勿少。

    evidence 可显式覆盖 why/consequence（调用域知情的 bridge 优先）。
    """
    ev = evidence or {}
    why = str(ev.get("why") or "") or _WHY_DEFAULT.get(kind, "与你的 NEXUS 体系相关，需你知悉")
    consequence = (str(ev.get("consequence") or "")
                   or _CONSEQ_DEFAULT.get(kind, "不处理：该事项保留在每日日报中持续可见"))
    lines = [
        f"[{icon}] {subject}",
        f"发生了什么：{expand_terms(what)}",
        f"为什么与你有关：{expand_terms(why)}",
        f"你需要做什么：{expand_terms(action)}｜急不急：{urgency}",
        f"不处理会怎样：{expand_terms(consequence)}",
    ]
    if detail:
        lines += ["", "—— 展开详情 ——", expand_terms(detail)]
    return "\n".join(lines)

# 🔴 影子转正挡板（任务书 §2 设计约束）：False 时代理人路径的 L2 推送被挡
# （返回 blocked，不入队不丢弃——调用方可决定降级 L3）。L1/L3 不受此开关限制。
# 集成单在影子转正后翻 True。
AGENT_PUSH_ENABLED = False

_NUM_EMOJI = ["1\u20e3", "2\u20e3", "3\u20e3", "4\u20e3", "5\u20e3",
              "6\u20e3", "7\u20e3", "8\u20e3", "9\u20e3"]


_LEVEL_LABEL = {"L1": "高风险", "L2": "待决", "L3": "记录"}


def _redact(text: str) -> str:
    """异常/日志输出脱敏：SCT key 段与 TG token 段替换 ***（陷阱 3，保留 .send 等后缀）。"""
    out = text
    if "SCT" in out:
        def _scrub(part):
            if part.startswith("SCT"):
                suffix = part[part.index("."):] if "." in part else ""
                return "***" + suffix
            return part
        out = "/".join(_scrub(p) for p in out.split("/"))
    return _redact_tg(out)


def _redact_tg(text: str) -> str:
    """TG token 段脱敏（/bot<token>/ → /bot***/）。"""
    if "/bot" in text:
        head, _, tail = text.partition("/bot")
        token = tail.split("/")[0]
        if token:
            return head + "/bot***/" + tail[len(token) + 1:]
    return text


def _http_error_detail(e: urllib.error.HTTPError) -> str:
    """把 HTTP 4xx/5xx 的响应体捞进错误信息（Task1588 可观测性修复）。

    HTTPError 默认只有一句 "HTTP Error 400: Bad Request"——真正的根因
    （如 Server酱配额超限 code=40001）在响应体里，不捞出来用户就永远
    只能看到"推送失败"却不知道为什么。返回串以 "4xx"/"5xx" 开头，
    deliver() 靠这个前缀判断要不要跳过重试。
    """
    body = e.read().decode("utf-8", "replace")
    e.close()
    try:
        api = json.loads(body)
        detail = f"api_code={api.get('code')}: {api.get('message', '')}"
    except ValueError:
        # 响应体不是 JSON（网关裸 HTML 之类）——原文截断保留线索，别让长页面刷屏
        detail = f"body={body[:200]!r}"
    kind = "4xx" if e.code < 500 else "5xx"
    return f"{kind} http={e.code} {detail}"


def format_body(blocker: str, background: str, rationale: str = "") -> str:
    """规范 body 构造器（649 §6.3 七字段中「卡点/背景/依据」三字段的载体）。

    push() 的 body 形参接受自由文本；用本函数生成则模板七字段标头齐全
    （背景超过 3 行取前 3 行——设计约束「背景≤3行」）。
    """
    bg_lines = [l for l in background.strip().splitlines() if l.strip()][:3]
    parts = [f"卡点：{blocker}", "背景：" + " / ".join(bg_lines)]
    if rationale:
        parts.append(f"依据：{rationale}")
    return "\n".join(parts)


# ═══════════════════════════════════════════════════════════════════
# 通道插件（ChannelBase + 注册表——可扩展，649 §6.1）
# ═══════════════════════════════════════════════════════════════════

class ChannelBase:
    """通道插件基类。子类实现 send() 返回 (ok: bool, err: str|None)。"""

    name = "base"

    def available(self) -> bool:
        raise NotImplementedError

    def send(self, title: str, desp: str) -> tuple:
        raise NotImplementedError


class ServerChan(ChannelBase):
    """Server酱（主通道，用户已扫码绑定）。POST 表单 title+desp（desp=markdown）。

    成功判据: HTTP 200 且响应 JSON code == 0。
    """

    name = "serverchan"

    def __init__(self, key_file: Path | None = None):
        # 调用时解析（design §2.1 规范）：默认参数在 def 时绑定会让
        # NEXUS_XDR_PUSH_KEY_FILE/测试 monkeypatch 失效（1960 实证）
        self._key_file = key_file or Path(os.environ.get("NEXUS_XDR_PUSH_KEY_FILE")
                                          or subpath("secrets/push_serverchan.key"))
        self._key = None

    def available(self) -> bool:
        try:
            self._key = self._key_file.read_text(encoding="utf-8").strip()
        except OSError:
            self._key = None
        return bool(self._key)

    def send(self, title: str, desp: str) -> tuple:
        if not self.available():
            return False, "sendkey 文件缺失或为空"
        url = SERVERCHAN_API.format(key=self._key)
        data = urllib.parse.urlencode({"title": title, "desp": desp}).encode()
        try:
            with urllib.request.urlopen(
                    urllib.request.Request(url, data=data), timeout=10) as resp:
                payload = json.loads(resp.read().decode("utf-8", "replace"))
                if payload.get("code") == 0:
                    return True, None
                return False, _redact(f"api code={payload.get('code')}")
        except urllib.error.HTTPError as e:
            # 必须放在 URLError 分支前面：HTTPError 是 URLError 的子类，
            # 放后面永远轮不到。4xx 响应体里才有真根因（如配额 40001）
            return False, _redact(_http_error_detail(e))
        except (urllib.error.URLError, TimeoutError, OSError, ValueError) as e:
            return False, _redact(f"{type(e).__name__}: {e}")


class Telegram(ChannelBase):
    """TG 备用通道。凭据从 env 读（NEXUS_TG_BOT_TOKEN / NEXUS_TG_CHAT_ID）；
    缺失=通道优雅禁用（不报错不崩溃）。"""

    name = "telegram"

    def __init__(self):
        self._token = os.environ.get("NEXUS_TG_BOT_TOKEN", "")
        self._chat = os.environ.get("NEXUS_TG_CHAT_ID", "")

    def available(self) -> bool:
        return bool(self._token and self._chat)

    def send(self, title: str, desp: str) -> tuple:
        if not self.available():
            return False, "TG 凭据缺失（env 未配置）"
        url = TG_API.format(token=self._token)
        data = json.dumps({
            "chat_id": self._chat, "text": f"{title}\n\n{desp}",
            "parse_mode": "Markdown",
        }, ensure_ascii=False).encode()
        try:
            with urllib.request.urlopen(
                    urllib.request.Request(
                        url, data=data, headers={"Content-Type": "application/json"}),
                    timeout=10) as resp:
                payload = json.loads(resp.read().decode("utf-8", "replace"))
                if payload.get("ok"):
                    return True, None
                return False, _redact(f"tg api ok={payload.get('ok')}")
        except urllib.error.HTTPError as e:
            # 同 ServerChan：TG 启用后防同款盲区（响应体根因被丢弃）
            return False, _redact(_http_error_detail(e))
        except (urllib.error.URLError, TimeoutError, OSError, ValueError) as e:
            return False, _redact(f"{type(e).__name__}: {e}")

_EMAIL_ADDR_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")


def _redact_email(text: str) -> str:
    """Email 路径脱敏：邮箱地址替换 ***@***（陷阱 3，同 serverchan key 先例——
    授权码只在 login 调用中出现、smtplib 异常文本不回显，地址是唯一可能漏项）。"""
    return _EMAIL_ADDR_RE.sub("***@***", text)


# ── 回信接收指引（2611192240 §F）：每封外发邮件尾部统一追加 ──
REPLY_FOOTER = ("\n\n——\n本邮件可直接回复：写指令内容（如「2611192240 按A」或「APPROVE」），"
                "系统每 5 分钟收一次并自动转达对应 Agent。")

# 2611230844：系统外发邮件的来源标记——收件箱=发件箱，入站桥（email_inbound.classify_inbound）
# 见此头一律归档，不当用户回信指令。头名/值与 email_inbound 的同名常量必须一致。
ORIGIN_HEADER = "X-NEXUS-Origin"
ORIGIN_SYSTEM = "system"

class EmailChannel(ChannelBase):
    """Email 通道（L2 清单/晨报主用——Server酱免费 5 条/天被 L2 吃光的分流，
    用户定向 2026-11-18）。smtplib 标准库；凭据 secrets/push_email.json
    （smtp_host/smtp_port/account/auth_code/to_addr/use_tls）——缺文件=
    通道未配置，available()=False 优雅跳过（降级链照走，先部署凭据后补）。

    成功判据: SMTP 会话 login+sendmail 无异常。
    端口语义参数化（陷阱 5）：465=SMTP_SSL，其余=SMTP+按 use_tls 决定 STARTTLS。
    """

    name = "email"


    def __init__(self, cred_file: Path | None = None):
        # 调用时解析（同 ServerChan 1960 惯例：默认参数 def 时绑定会废掉 env 覆写）
        self._cred_file = cred_file or Path(os.environ.get("NEXUS_XDR_PUSH_EMAIL_FILE")
                                            or subpath("secrets/push_email.json"))
        self._cfg = None

    def available(self) -> bool:
        need = ("smtp_host", "smtp_port", "account", "auth_code", "to_addr")
        try:
            raw = json.loads(self._cred_file.read_text(encoding="utf-8"))
            if all(str(raw.get(k) or "").strip() for k in need):
                self._cfg = raw
            else:
                self._cfg = None
        except (OSError, ValueError):
            self._cfg = None
        return self._cfg is not None

    def send(self, title: str, desp: str) -> tuple:
        if not self.available():
            return False, "email 凭据缺失（secrets/push_email.json 未配置）"
        import smtplib
        from email.header import Header
        from email.mime.text import MIMEText
        cfg = self._cfg
        port = int(cfg["smtp_port"])
        try:
            mime = MIMEText(desp + REPLY_FOOTER, "plain", "utf-8")  # §F：每封邮件尾部回复指引
            mime["Subject"] = Header(title, "utf-8")
            mime["X-NEXUS-Origin"] = ORIGIN_SYSTEM  # 2611230844：系统来源标记（入站桥据此归档）
            mime["From"] = cfg["account"]
            mime["To"] = cfg["to_addr"]
            if port == 465:
                smtp = smtplib.SMTP_SSL(cfg["smtp_host"], port, timeout=15)
            else:
                smtp = smtplib.SMTP(cfg["smtp_host"], port, timeout=15)
            try:
                if port != 465 and bool(cfg.get("use_tls", True)):
                    smtp.starttls()
                smtp.login(cfg["account"], cfg["auth_code"])
                smtp.sendmail(cfg["account"], [cfg["to_addr"]], mime.as_string())
            finally:
                smtp.quit()
            return True, None
        except smtplib.SMTPAuthenticationError as e:
            # 授权码错=确定性失败——4xx 前缀让 deliver 快换通道不烧退避；
            # str(e)=服务器响应码+原因（不含授权码），地址类漏项由脱敏兜住
            return False, _redact_email(f"4xx smtp auth: {type(e).__name__}: {e}")
        except smtplib.SMTPException as e:
            msg = f"{type(e).__name__}: {e}"
            # 永久性 SMTP 拒绝族标 4xx 快换；临时类照常走退避重试
            if type(e).__name__ in ("SMTPSenderRefused", "SMTPRecipientsRefused",
                                    "SMTPDataError", "SMTPNotSupportedError"):
                msg = "4xx smtp: " + msg
            return False, _redact_email(msg)
        except (OSError, TimeoutError) as e:
            return False, _redact_email(f"{type(e).__name__}: {e}")


# 注册表（集成单可注入新通道: REGISTRY["sms"] = SmsChannel）
# 路由（2611172061 用户定向 2026-11-18）：L1 维持 Server酱主用（立即语义
# 不动，陷阱 1）；L2 清单/晨报 Email 主用（零配额压力）+ Server酱备用；
# TG 可插拔位置不变；降级链末位 xdr alert 兜底不变（不丢消息铁律）。
REGISTRY = {"serverchan": ServerChan, "telegram": Telegram, "email": EmailChannel}
CHANNEL_ORDER = ("email", "serverchan", "telegram")      # 默认链=全域邮件首发（用户定向 2026-11-18 17:1x：Email 主用，Server酱降备用）
L2_CHANNEL_ORDER = ("email", "serverchan", "telegram")   # L2 清单/晨报专用链（两链同序，语义留档）


# ═══════════════════════════════════════════════════════════════════
# 队列持久化（L2 防抖窗 + L3 记录 + 日报缓存）
# ═══════════════════════════════════════════════════════════════════

def _queue_load(path: Path = QUEUE_FILE) -> dict:
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {"l2_window": None, "l2_items": [], "digest_items": [], "l3_log": []}
    # 旧队列迁移（2611192105）：晨报缓存 silent_items 并入日报缓存，一次性幂等
    legacy = state.pop("silent_items", None)
    if legacy:
        state.setdefault("digest_items", []).extend(legacy)
    state.pop("pending_digest", None)
    return state


def _queue_save(state: dict, path: Path = QUEUE_FILE) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, ensure_ascii=False, indent=1), encoding="utf-8")


def _in_silence(now=None) -> bool:
    """静默时段 23:00-08:00（本地时间，陷阱 6）。"""
    lt = time.localtime(now if now is not None else time.time())
    return lt.tm_hour >= SILENCE_START or lt.tm_hour < SILENCE_END


# ═══════════════════════════════════════════════════════════════════
# 消息模板（649 §6.3 七字段）+ 选项渲染
# ═══════════════════════════════════════════════════════════════════

def render_options(options) -> str:
    """编号选项渲染：1️⃣2️⃣3️⃣…（元素为 str 或 {text, note}，最多 9 项）。"""
    if not options:
        return ""
    lines = []
    for i, opt in enumerate(options[:9]):
        if isinstance(opt, dict):
            text, note = opt.get("text", ""), opt.get("note", "")
            entry = text + (f"（{note}）" if note else "")
        else:
            entry = str(opt)
        lines.append(f"{_NUM_EMOJI[i]} {entry}")
    return "\n".join(lines)


def render_message(level: str, title: str, body: str, options, msg_id: str) -> str:
    """七字段模板：任务/卡点+背景+依据（body 载体）/编号选项/回复格式/24h 有效期。

    body 传自由文本时原样渲染；用 format_body() 构造则字段标头齐全。
    """
    head = f"【NEXUS 待决 #{msg_id}】（{level}/{_LEVEL_LABEL.get(level, '')}）"
    parts = [head, f"任务：{title}", body]
    opts = render_options(options)
    if opts:
        parts.append(f"选项：{opts.replace(chr(10), ' ')}")
        n = len(options[:9])
        replies = " / ".join(f"{msg_id}-{i + 1}" for i in range(n))
        parts.append(f"回复：{replies}（{MSG_TTL_HOURS}h 内有效）")
    else:
        parts.append(f"回复：见 Console 决策队列（{MSG_TTL_HOURS}h 内有效）")
    return "\n".join(parts)


# ═══════════════════════════════════════════════════════════════════
# 人话模板（2611192105 §C）+ 即时档判定（§A 四触发）
# ═══════════════════════════════════════════════════════════════════

def render_human(icon: str, subject: str, what: str, action: str,
                 urgency: str = "不急") -> str:
    """人话三行制：两条邮件（即时档/日报）正文统一格式，禁机器行。"""
    return (f"[{icon}] {subject}\n"
            f"发生了什么：{what}\n"
            f"需要你做什么：{action}｜急不急：{urgency}")


def _immediate_kind(level: str, title: str, body: str,
                    decision_required: bool = False, evidence: dict = None) -> str | None:
    """即时档触发判定（§A 四触发，任一命中返回触发类别，否则 None）。

    L3 恒不触发（只存语义不变）；判定范围 = title+body 关键词 + 显式参数。
    """
    if level == "L3":
        return None
    ev = evidence or {}
    if decision_required or ev.get("decision_required"):
        return "decision"
    text = f"{title}\n{body}"
    if ev.get("kind") == "system_red" or RED_RE.search(text):
        return "red"
    if FUNDS_RE.search(text):
        return "funds"
    if IRREVERSIBLE_RE.search(text):
        return "irreversible"
    if level == "L1":
        return "l1"
    return None


def _human_view(kind: str, level: str, title: str, body: str,
                options, evidence: dict = None) -> tuple:
    """把 push() 入参映射为人话三行字段 (icon, subject, what, action, urgency)。

    evidence 显式字段优先（{what, action, urgency}）；否则从 body 前两行/
    编号选项推导——format_body 构造的「卡点：/背景：」标头会被剥掉。
    """
    ev = evidence or {}
    subject = title.strip() or "（无标题）"
    what = str(ev.get("what") or "").strip()
    if not what:
        picks = []
        for line in (body or "").strip().splitlines():
            s = line.strip()
            if not s:
                continue
            for tag in ("卡点：", "背景：", "依据："):
                if s.startswith(tag):
                    s = s[len(tag):].strip()
                    break
            if s and s not in picks:     # 2611192240：卡点/背景同文去重（×2 拼接根因）
                picks.append(s)
                if len(picks) >= 2:
                    break
        what = "；".join(picks) or "（详情见正文）"
    action = str(ev.get("action") or "").strip()
    if not action:
        if options:
            action = "回复：" + render_options(options).replace(chr(10), " ")
        else:
            action = "无需动作"
    urgency = str(ev.get("urgency") or "").strip() or (
        "尽快" if kind in ("decision", "l1", "red", "funds") else "不急")
    if action == "无需动作":
        urgency = "不急"   # 2611192240 §C：动作/急度联动——无动作必不急，禁矛盾
    return _IMMEDIATE_ICON.get(kind, "❗"), subject, what, action, urgency


# ═══════════════════════════════════════════════════════════════════
# 降级链（649 §6.5）: 主通道退避×3 → 备用 → xdr alert 兜底
# ═══════════════════════════════════════════════════════════════════

def _xdr_alert(detail: str) -> bool:
    """双通道全败的最终落点：POST xdr /session/event（type=alert）。

    不允许静默丢待决——本函数自身失败也打印 stderr（最终可见性）。
    Task1568 (1564 池建议①): 前置注册态自查——agent 未注册/失效时不再 400 盲推，
    降级用 nexus-monitor（注册名，Bug11 惯例）+ detail 前缀归因。
    """
    agent = "notify_push"
    try:
        import sys as _sys
        # src/ 在 parents[1]（nexusos-xdr/src/nexusos_xdr/notify_push.py）——
        # 未 editable 安装的环境也能找到 nexusos_xdr 包；已安装时 insert 是无害冗余
        _sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
        from nexusos_xdr.session_state import validate_agent
        ok, reason = validate_agent(agent)
        if not ok:
            agent = "nexus-monitor"
            detail = f"[notify_push-fallback] {detail}"
            print(f"[notify_push] agent 'notify_push' 注册态失效({reason})，"
                  f"兜底降级 {agent}", file=sys.stderr)
    except Exception as e:
        # 自查链路自身异常 → 保守降级（注册名保兜底可达）
        agent = "nexus-monitor"
        detail = f"[notify_push-fallback] {detail}"
        print(f"[notify_push] 注册态自查异常({e})，兜底降级 {agent}", file=sys.stderr)
    body = json.dumps({
        "agent": agent, "type": "alert",
        "detail": detail[:2000], "target": "",
    }, ensure_ascii=False).encode()
    try:
        with urllib.request.urlopen(
                urllib.request.Request(
                    XDR_EVENT_URL, data=body,
                    headers={"Content-Type": "application/json"}), timeout=5) as resp:
            return resp.status == 200
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        print(f"[notify_push] xdr alert 兜底也失败（不丢弃，stderr 留痕）: "
              f"{_redact(str(e))}", file=sys.stderr)
        return False


def _sleep(sec: float) -> None:   # 单测 monkeypatch 锚点
    time.sleep(sec)


def _emit_push_delivered(result: dict, title: str) -> None:
    """2611192130 J1: push_delivered —— 通道投递结果落流（MEM#118115 空白⑧）。

    与 push_queue.last_flush_result 同源数据入流（deliver() 返回值原样拆字段）；
    fire-and-forget：事件失败不影响投递主链（不丢消息铁律 > 观测完整性）。
    agent=notify_push（注册名，AGENT_REGISTRY 实证在册）。
    """
    try:
        import sys as _sys
        _sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
        from nexusos_xdr import session_state as _ss
        _ss.emit_system_event(
            "notify_push", "push_delivered",
            detail=f"delivered={result.get('delivered')} channel={result.get('channel')} "
                   f"errors={len(result.get('errors', []))} | {title[:60]}",
            target=f"push:{result.get('channel') or 'none'}",
            extra={"delivered": bool(result.get("delivered")),
                   "channel": result.get("channel"),
                   "fallback_alert": bool(result.get("fallback_alert")),
                   "errors": [str(e) for e in result.get("errors", [])[-3:]]},
        )
    except Exception:
        pass  # 观测失败绝不影响投递结果返回


def deliver(title: str, desp: str, order: tuple = None) -> dict:
    """带降级链的投递：order 逐通道，单通道内退避×BACKOFF_SCHEDULE。

    order=None 用 CHANNEL_ORDER（L1 语义——Server酱主用）；L2 清单/晨报
    传 L2_CHANNEL_ORDER（Email 主用）。返回 {"delivered", "channel",
    "fallback_alert", "errors"}。
    """
    errors = []
    for chan_name in (order or CHANNEL_ORDER):
        chan = REGISTRY[chan_name]()
        if not chan.available():
            errors.append(f"{chan_name}: disabled")
            continue
        attempt_errors = []
        for attempt in range(len(BACKOFF_SCHEDULE) + 1):
            ok, err = chan.send(title, desp)
            if ok:
                _r_ok = {"delivered": True, "channel": chan_name,
                         "fallback_alert": False, "errors": errors + attempt_errors}
                _emit_push_delivered(_r_ok, title)  # 2611192130 J1: push_delivered
                return _r_ok
            attempt_errors.append(f"{chan_name}#{attempt + 1}: {err}")
            if err and err.startswith("4xx"):
                # 4xx 是确定性失败——配额没重置前重发几次都是 40001，
                # 白烧三次退避等待；直接换下一通道（5xx/网络错仍照常重试）
                break
            if attempt < len(BACKOFF_SCHEDULE):
                _sleep(BACKOFF_SCHEDULE[attempt])
        errors.extend(attempt_errors)
    # 双通道全败 → xdr alert 兜底（不静默丢）
    fallback = _xdr_alert(f"notify_push 双通道全败: {title} | " + " ; ".join(errors[-3:]))
    _r_fail = {"delivered": False, "channel": None,
               "fallback_alert": fallback, "errors": errors}
    _emit_push_delivered(_r_fail, title)  # 2611192130 J1: push_delivered（全败也留痕）
    return _r_fail


# ═══════════════════════════════════════════════════════════════════
# 主接口
# ═══════════════════════════════════════════════════════════════════

def _sweep_l2_window(state: dict) -> dict:
    """到期 L2 防抖窗内容转入日报缓存（2611192105：只转存，不再发邮件）。"""
    win = state.get("l2_window")
    items = state.get("l2_items", [])
    if not items:
        state["l2_window"] = None
        return state
    if win is not None and time.time() - win < L2_WINDOW_SEC:
        return state   # 窗口未到期（防抖保留）
    state.setdefault("digest_items", []).extend(items)
    state["l2_window"] = None
    state["l2_items"] = []
    return state


def push(level: str, title: str, body: str, options=None,
         via_agent: bool = False, queue_path: Path = QUEUE_FILE,
         decision_required: bool = False, evidence: dict = None) -> dict:
    """推送入口（649 §6 接口 + 2611192105 回调化）。level ∈ {"L1","L2","L3"}。

    Args:
        level: L1 立即 / L2 防抖窗→17:30 日报 / L3 仅记录
        title/body: 任务标题与正文（body 自由文本或 format_body() 构造）
        options: 编号选项列表（str 或 {text, note}）——即时档渲染进「需要你做什么」
        via_agent: 代理人路径标记——True 且 AGENT_PUSH_ENABLED=False 时
            L2 被挡板拦截（影子转正前语义，返回 blocked；不丢不推）。
        queue_path: 队列文件（测试注入 tmp_path）
        decision_required: 即时档触发①——需用户决定，立即人话邮件
        evidence: 结构化人话字段 {what, action, urgency, kind}；
            kind="system_red" 显式标体系红灯（即时档触发④）。
    Returns:
        {"status": sent|queued|queued_silent|recorded|blocked, "msg_id",
         "kind"（即时档触发类别，命中时）, ...投递详情}
    """
    if level not in ("L1", "L2", "L3"):
        raise ValueError(f"level 必须是 L1/L2/L3，得到 {level!r}")
    msg_id = uuid.uuid4().hex[:4].upper()

    if via_agent and level == "L2" and not AGENT_PUSH_ENABLED:
        return {"status": "blocked", "msg_id": msg_id,
                "reason": "AGENT_PUSH_ENABLED=False（影子转正挡板，L2 代理路径不推）"}

    state = _queue_load(queue_path)
    try:
        if level == "L3":
            state.setdefault("l3_log", []).append(
                {"msg_id": msg_id, "title": title, "ts": time.time()})
            _queue_save(state, queue_path)
            return {"status": "recorded", "msg_id": msg_id}

        kind = _immediate_kind(level, title, body, decision_required, evidence)
        if kind and (level == "L1" or not _in_silence()):
            # 即时档：L1 语义静默穿透（规则不变）；立即发一封人话邮件
            icon, subject, what, action, urgency = _human_view(
                kind, level, title, body, options, evidence)
            desp = build_email_body(icon, subject, what, action, urgency,
                                    kind=kind, evidence=evidence, detail=body)
            result = deliver(f"{IMMEDIATE_TITLE_PREFIX}{title}", desp)
            state.setdefault("immediate_log", []).append(
                {"msg_id": msg_id, "title": title, "kind": kind, "ts": time.time()})
            _queue_save(state, queue_path)
            return {"status": "sent" if result["delivered"] else "fallback",
                    "msg_id": msg_id, "kind": kind, "deliver": result}

        # 汇总档：静默时段只存（仅 L1 语义穿透）；其余进 30min 防抖窗
        state = _sweep_l2_window(state)   # 到期旧窗先转日报缓存（跨窗拆批）
        entry = {"msg_id": msg_id, "level": level, "title": title, "body": body}
        if _in_silence():
            state.setdefault("digest_items", []).append(entry)
            _queue_save(state, queue_path)
            return {"status": "queued_silent", "msg_id": msg_id,
                    "note": "静默时段(23-08)只存不推，并入 17:30 日报"}
        state.setdefault("l2_items", []).append(entry)
        if state.get("l2_window") is None:
            state["l2_window"] = time.time()
        _queue_save(state, queue_path)
        return {"status": "queued", "msg_id": msg_id,
                "window_items": len(state["l2_items"]),
                "note": f"L2 防抖窗 {L2_WINDOW_SEC // 60}min，到期转 17:30 日报（不发邮件）"}
    finally:
        pass


def _collect_pending_pool(top: int = 3) -> list | None:
    """⏳ 挂起区数据源：未决问题池活跃未决段前 N 条 [(编号, 摘要)]。

    解析口径同 nexus-morning-report.collect_pending_top3（表格行 | N | 描述 |）。
    池文件/vault 不可得返回 None（该区显示「不可得」，不阻塞日报）。
    """
    vr = vault_root()
    if vr is None:
        return None
    try:
        text = vr.joinpath(*PENDING_POOL_RELPATH).read_text(encoding="utf-8")
    except OSError:
        return None
    items, in_active = [], False
    for line in text.splitlines():
        if line.startswith("## "):
            in_active = "活跃未决" in line
            continue
        if not in_active:
            continue
        m = re.match(r"^\|\s*(\d+)\s*\|\s*(.+?)\s*\|", line)
        if not m:
            continue
        if not _pool_row_open(line):
            continue        # 2611230883：源文件含闭环/取消行，按状态筛掉
        desc = expand_terms(re.sub(r"\[\[|\]\]|\*\*|`", "", m.group(2)))  # §E 术语展开
        items.append((m.group(1), desc[:80]))
        if len(items) >= top:
            break
    return items


def _collect_gate_stats() -> tuple | None:
    """📊 gate 通过率：xdr 事件 API 今日 gate_check 计数 (passed, total)。

    事件端点单页上限内取近 500 条（高频日可能截断——数字行标注窗口口径）。
    """
    try:
        with urllib.request.urlopen(f"{XDR_EVENTS_URL}?limit=500", timeout=8) as resp:
            events = json.loads(resp.read().decode("utf-8", "replace")).get("events", [])
    except (urllib.error.URLError, TimeoutError, OSError, ValueError):
        return None
    today = time.strftime("%Y-%m-%d")
    gates = [e for e in events if e.get("type") == "gate_check"
             and str(e.get("ts_local", "")).startswith(today)]
    passed = sum(1 for e in gates if "passed=True" in str(e.get("detail", "")))
    return passed, len(gates)


def _collect_check_health() -> tuple | None:
    """📊 check 健康 + 漂移计数：nexus-monitor state 最新一轮各 check 状态。

    返回 (counts_dict, drift_count)；漂移 = V3 points_drift + M10 drift_entities。
    """
    path = Path(os.environ.get(MONITOR_STATE_FILE_ENV)
                or (vault_root() / "05-System" / "自动化脚本" / "nexus-monitor"
                    / "state" / "monitor-state.json"))
    try:
        checks = json.loads(path.read_text(encoding="utf-8")).get("checks", {})
    except (OSError, ValueError, TypeError):
        return None
    counts, drift = {}, 0
    for cid, c in checks.items():
        if cid == "META" or not isinstance(c, dict):
            continue
        st = c.get("status", "unknown")
        counts[st] = counts.get(st, 0) + 1
        m = c.get("metrics") or {}
        drift += int(m.get("points_drift", 0) or 0) + int(m.get("drift_entities", 0) or 0)
    return counts, drift


def daily_digest(queue_path: Path = QUEUE_FILE) -> dict:
    """每日 17:30 日报：三区一封（✅已自动处理 / ⏳挂起中 / 📊当日数字）。

    nexus/scheduler.py 日频任务调用（--daily-digest CLI）；无论当日有无
    积压都发一封（用户定向：每日一封、有重点、有状态区分）。发送后清
    日报缓存与 immediate_log（次日重新计数）。
    """
    state = _queue_load(queue_path)
    state = _sweep_l2_window(state)
    auto = state.get("digest_items", []) + state.get("l2_items", [])
    today = time.strftime("%Y-%m-%d")
    l3_today = [e for e in state.get("l3_log", [])
                if time.strftime("%Y-%m-%d", time.localtime(e.get("ts", 0))) == today]
    zone1 = auto + [{"title": e.get("title", "")} for e in l3_today]
    state["digest_items"], state["l2_items"], state["l2_window"] = [], [], None

    pool = _collect_pending_pool()
    gate = _collect_gate_stats()
    health = _collect_check_health()
    immediate_n = len(state.get("immediate_log", []))

    lines = [f"✅ 已自动处理（{len(zone1)} 条，无需动作）"]
    for it in zone1[:9]:
        lines.append(f"  ✅ {expand_terms(str(it.get('title', '')))[:80]}")
    if len(zone1) > 9:
        lines.append(f"  …等共 {len(zone1)} 条")
    lines.append("")
    lines.append("⏳ 挂起中（等你裁决）")
    if pool:
        lines.extend(f"  ⏳ #{num} {desc}" for num, desc in pool)
    else:
        lines.append("  （池不可得或无积压）")
    lines.append("")
    lines.append("📊 当日数字")
    lines.append(f"  · gate 通过 {gate[0]}/{gate[1]}（近500事件窗）" if gate
                 else "  · gate 统计不可得（xdr 事件端点未响应）")
    if health:
        counts, drift = health
        lines.append("  · check 健康 " + " / ".join(
            f"{k} {v}" for k, v in sorted(counts.items())))
        lines.append(f"  · 漂移计数 {drift}")
    else:
        lines.append("  · check 状态不可得（monitor-state 未读到）")
    lines.append(f"  · 即时推送 {immediate_n} 条（今日已单独邮件）")

    result = deliver(f"NEXUS 日报 {time.strftime('%m-%d')}",
                     "\n".join(lines), order=L2_CHANNEL_ORDER)
    state["immediate_log"] = []
    state["last_digest_result"] = result
    state["last_digest_date"] = today
    _queue_save(state, queue_path)
    return {"status": "sent" if result["delivered"] else "fallback",
            "zone1": len(zone1), "pool": len(pool or []),
            "immediate_today": immediate_n, "deliver": result}


# ── 调度接线（挂现有 nexus/scheduler.py 日频行，禁新建定时组件）──

def _next_digest_run(now: float = None) -> "object":
    """下一个 17:30 本地时刻（aware datetime；已过则明天）。"""
    from datetime import datetime, timedelta
    now = time.time() if now is None else now
    lt = time.localtime(now)
    candidate = datetime(lt.tm_year, lt.tm_mon, lt.tm_mday,
                         DIGEST_HOUR, DIGEST_MINUTE)
    if time.mktime(candidate.timetuple()) <= now:
        candidate += timedelta(days=1)
    return candidate.astimezone()


def anchor_digest_schedule(dsn: str = None) -> dict:
    """注册/重锚 scheduler 日频行：next_run_at 锚定下一个 17:30。

    scheduler 只有 interval 语义（now+86400 每轮带扫描抖动，长期漂移），
    本函数 upsert 绝对时刻纠偏——日报每次跑完自锚，跑偏自愈。psycopg2
    惰性导入：notify_push 主体保持零三方依赖，仅此调度接线需要 PG。
    """
    try:
        import psycopg2
    except ImportError:
        return {"ok": False,
                "error": "psycopg2 不可用（日报照发，时刻锚定退化为 interval 漂移）"}
    cmd = json.dumps({"command": f'"{sys.executable}" "{Path(__file__).resolve()}" --daily-digest',
                      "timeout": 600}, ensure_ascii=False)
    conn = psycopg2.connect(dsn or os.environ.get("NEXUS_PG_DSN", SCHEDULER_DSN_DEFAULT))
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO scheduled_tasks
                        (name, action, action_args, interval_sec, recurring,
                         max_iterations, expire_at, next_run_at, run_count, created_at)
                    VALUES (%s, 'shell', %s::jsonb, 86400, true, NULL, NULL, %s, 0, now())
                    ON CONFLICT (name) DO UPDATE SET
                        action_args = EXCLUDED.action_args,
                        next_run_at = EXCLUDED.next_run_at,
                        last_error = NULL
                    """,
                    (DIGEST_TASK_NAME, cmd, _next_digest_run()))
    finally:
        conn.close()
    return {"ok": True, "name": DIGEST_TASK_NAME,
            "next_run_at": _next_digest_run().isoformat()}


# ═══════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════

def _self_test() -> int:
    """全 mock 自检：不发任何真实网络请求。2611172061 Email 三分支 +
    2611192105 回调化（即时档四触发/防抖窗不发信/日报聚合/人话模板）。

    🔴 REGISTRY 必须把 email 也 mock 掉：CHANNEL_ORDER 已全域邮件首发
    （26111718 用户定向），本机存在真实凭据时只 mock serverchan 会让
    L1 分支真发 SMTP（旧版自检在此泄漏真实邮件——本版修正）。
    """
    import tempfile
    import unittest.mock as mock

    def _mk(cls, avail=True, send=(True, None)):
        m = mock.Mock(spec=cls)
        m.available.return_value = avail
        m.send.return_value = send
        return m

    _self_mod = sys.modules[__name__]
    q1 = Path("/tmp/_np_selftest.json")
    q1.unlink(missing_ok=True)

    # ── 分级路由 + 即时档四触发（2611192105 §A）──
    em0, sc0 = _mk(EmailChannel), _mk(ServerChan)
    with mock.patch.dict(REGISTRY, {"email": lambda: em0, "serverchan": lambda: sc0}), \
         mock.patch.object(_self_mod, "_in_silence", return_value=False):
        r1 = push("L1", "自检-任务", format_body("自检卡点", "自检背景行1\n行2", "自检依据"),
                  options=["甲", "乙"], queue_path=q1)
        r3 = push("L3", "自检-记录", "x", queue_path=q1)
        trig = {
            "decision": push("L2", "触发①-需决定", "正文无关键词",
                             queue_path=q1, decision_required=True),
            "funds": push("L2", "触发②-资金", "下单成交提醒", queue_path=q1),
            "irreversible": push("L2", "触发③-不可逆", "即将删除生产索引", queue_path=q1),
            "red": push("L2", "触发④-红灯", "体系 check error REJECT", queue_path=q1),
        }
    n_immediate = 5  # r1(L1) + 四触发，全部走 email 主通道
    immediate_ok = (r1["status"] == "sent" and r1.get("kind") == "l1"
                    and all(r["status"] == "sent" and r.get("kind") == k
                            for k, r in trig.items()))
    imm_titles = [c[0][0] for c in em0.send.call_args_list[:n_immediate]]
    imm_bodies = [c[0][1] for c in em0.send.call_args_list[:n_immediate]]
    immediate_title_ok = all(t.startswith("[需你决定] ") for t in imm_titles)
    immediate_body_ok = all(b.splitlines()[0].startswith("[") and "发生了什么：" in b
                            and "为什么与你有关：" in b and "你需要做什么：" in b
                            and "不处理会怎样：" in b and "—— 展开详情 ——" in b
                            and "{" not in b
                            for b in imm_bodies)   # 2611192240 §E 四要素+详情层
    terms_expanded = ("gate_blocked" not in expand_terms("gate_blocked REJECT G14 G9 warn")
                      and "REJECT" not in expand_terms("REJECT")
                      and "产出登记段" in expand_terms("G14")
                      and "第 9 项检查" in expand_terms("G9"))
    msg = render_message("L1", "t", format_body("k", "b", "r"),
                         ["o1", {"text": "o2", "note": "n"}], "AB12")
    fields = all(k in msg for k in ("任务：", "卡点：", "背景：", "依据：", "选项：", "回复：", "有效"))
    human_msg = render_human("❓", "主题X", "事情Y", "回复Z", "尽快")
    human_ok = (human_msg.splitlines()
                == ["[❓] 主题X", "发生了什么：事情Y", "需要你做什么：回复Z｜急不急：尽快"])

    # ── 防抖窗不发邮件：到期转日报缓存（2611192105 §B）──
    q2 = Path("/tmp/_np_selftest_win.json")
    q2.unlink(missing_ok=True)
    em_q, sc_q = _mk(EmailChannel), _mk(ServerChan)
    with mock.patch.dict(REGISTRY, {"email": lambda: em_q, "serverchan": lambda: sc_q}), \
         mock.patch.object(_self_mod, "_in_silence", return_value=False):
        ra = push("L2", "窗-甲", "b", queue_path=q2)
        rb = push("L2", "窗-乙", "b", queue_path=q2)
        real_time = time.time
        with mock.patch.object(_self_mod, "time") as mt:
            mt.time.return_value = real_time() + L2_WINDOW_SEC + 1
            mt.localtime = time.localtime
            mt.strftime = time.strftime
            rc = push("L2", "新窗-丙", "b", queue_path=q2)
        st2 = json.loads(q2.read_text(encoding="utf-8"))
    window_no_email = (ra["status"] == rb["status"] == "queued" and rc["status"] == "queued"
                       and em_q.send.call_count == 0 and sc_q.send.call_count == 0
                       and len(st2["digest_items"]) == 2 and len(st2["l2_items"]) == 1)

    # ── 日报聚合：三区一封 + 标题 + 清缓存（2611192105 §B）──
    em_d = _mk(EmailChannel)
    with mock.patch.dict(REGISTRY, {"email": lambda: em_d}), \
         mock.patch.object(_self_mod, "_collect_pending_pool",
                           return_value=[("980", "notify 邮件回调化验收")]), \
         mock.patch.object(_self_mod, "_collect_gate_stats", return_value=(8, 10)), \
         mock.patch.object(_self_mod, "_collect_check_health",
                           return_value=({"ok": 40, "warn": 2}, 1)):
        dd = daily_digest(q2)
    d_title, d_body = em_d.send.call_args[0]
    digest_ok = (dd["status"] == "sent" and d_title.startswith("NEXUS 日报 ")
                 and "✅ 已自动处理（3 条" in d_body and "窗-甲" in d_body
                 and "⏳ 挂起中" in d_body and "#980" in d_body
                 and "📊 当日数字" in d_body and "8/10" in d_body
                 and json.loads(q2.read_text(encoding="utf-8"))["digest_items"] == [])

    # ── 旧队列迁移：silent_items → digest_items（一次性幂等）──
    q3 = Path("/tmp/_np_selftest_legacy.json")
    q3.write_text(json.dumps({"silent_items": [{"title": "旧晨报积压"}],
                              "l2_items": [], "l3_log": []}, ensure_ascii=False),
                  encoding="utf-8")
    legacy_migrated = (len(_queue_load(q3)["digest_items"]) == 1
                       and "silent_items" not in _queue_load(q3))

    # ── Email 分支（2611172061）──
    # ① 凭据缺失：真 EmailChannel 指向不存在文件 → available()=False（不崩）
    with tempfile.TemporaryDirectory() as td:
        em_missing = EmailChannel(cred_file=Path(td) / "nope.json")
        cred_missing = em_missing.available() is False
        # ② 真发：mock smtplib 层验证 send 全流程（凭据齐 → login+sendmail 调用）
        import smtplib as _smtplib
        cred_f = Path(td) / "push_email.json"
        cred_f.write_text(json.dumps({
            "smtp_host": "smtp.test", "smtp_port": 465, "account": "a@t.cn",
            "auth_code": "SECRET-CODE", "to_addr": "b@t.cn", "use_tls": True},
            ensure_ascii=False),
            encoding="utf-8")
        em_real = EmailChannel(cred_file=cred_f)
        smtp_mock = mock.MagicMock()
        with mock.patch.object(_smtplib, "SMTP_SSL", return_value=smtp_mock) as ssl_cls:
            ok_send, err = em_real.send("标题", "正文")
        smtp_ok = (ok_send and err is None and ssl_cls.call_args[0][:2] == ("smtp.test", 465)
                   and smtp_mock.login.call_args[0] == ("a@t.cn", "SECRET-CODE"))
        smtp_footer = "可直接回复" in smtp_mock.sendmail.call_args[0][2]  # §F 尾部指引入信体
        # ③ 4xx：授权码错 → err 以 4xx 开头（deliver 快换通道依据）且地址脱敏
        smtp_mock2 = mock.MagicMock()
        smtp_mock2.login.side_effect = _smtplib.SMTPAuthenticationError(535, b"auth fail a@t.cn")
        with mock.patch.object(_smtplib, "SMTP_SSL", return_value=smtp_mock2):
            bad_send, bad_err = em_real.send("标题", "正文")
        smtp_4xx = (bad_send is False and bad_err.startswith("4xx")
                    and "a@t.cn" not in bad_err and "***@***" in bad_err)

    # ── 路由断言：全域邮件首发（26111718 用户定向）+ 败退 + 双败落事件 ──
    with mock.patch.object(_self_mod, "_sleep"), \
         mock.patch.object(_self_mod, "_xdr_alert", return_value=True) as xdr_alert:
        # L2 链：Email 主用（成功则 Server酱不被碰）
        em_ok, sc_ok = _mk(EmailChannel), _mk(ServerChan)
        with mock.patch.dict(REGISTRY, {"email": lambda: em_ok, "serverchan": lambda: sc_ok}):
            r_l2 = deliver("t", "d", order=L2_CHANNEL_ORDER)
        l2_email_primary = (r_l2["delivered"] and r_l2["channel"] == "email"
                            and sc_ok.send.call_count == 0)
        # L2 败退：Email 4xx 一次即换（不烧退避重试），Server酱接住
        em_4xx, sc_ok2 = _mk(EmailChannel, send=(False, "4xx smtp auth: x")), _mk(ServerChan)
        with mock.patch.dict(REGISTRY, {"email": lambda: em_4xx, "serverchan": lambda: sc_ok2}):
            r_failover = deliver("t", "d", order=L2_CHANNEL_ORDER)
        l2_failover = (r_failover["delivered"] and r_failover["channel"] == "serverchan"
                       and em_4xx.send.call_count == 1)
        # 默认链（L1 语义）全域邮件首发：Email 主用，Server酱备用不被碰
        em_ok2, sc_ok3 = _mk(EmailChannel), _mk(ServerChan)
        with mock.patch.dict(REGISTRY, {"email": lambda: em_ok2, "serverchan": lambda: sc_ok3}):
            r_l1 = deliver("t", "d")
        l1_email_primary = (r_l1["delivered"] and r_l1["channel"] == "email"
                            and sc_ok3.send.call_count == 0)
        # 双败：全通道失败/不可用 → xdr alert 兜底（不丢消息铁律）
        em_bad = _mk(EmailChannel, send=(False, "4xx smtp auth: x"))
        sc_bad = _mk(ServerChan, send=(False, "4xx api code=40001"))
        with mock.patch.dict(REGISTRY, {"email": lambda: em_bad, "serverchan": lambda: sc_bad,
                                        "telegram": lambda: _mk(Telegram, avail=False)}):
            r_both = deliver("t", "d", order=L2_CHANNEL_ORDER)
        both_fail_fallback = (r_both["delivered"] is False and r_both["fallback_alert"]
                              and xdr_alert.called)

    checks = [
        ("L1 即时档 sent（kind=l1）", immediate_ok),
        ("即时档标题 [需你决定] 前缀", immediate_title_ok),
        ("即时档四要素+详情层（§E）", immediate_body_ok),
        ("术语零裸奔展开（§E）", terms_expanded),
        ("Email 尾部回复指引（§F）", smtp_footer),
        ("L3 recorded", r3["status"] == "recorded"),
        ("七字段模板（遗留通道格式）", fields),
        ("选项渲染", "1\u20e3" in msg and "（n）" in msg),
        ("人话模板三行制", human_ok),
        ("防抖窗不发邮件→转日报缓存", window_no_email),
        ("日报三区聚合+清缓存", digest_ok),
        ("旧队列 silent_items 迁移", legacy_migrated),
        ("脱敏", _redact("https://sctapi.ftqq.com/SCT123abc.send") == "https://sctapi.ftqq.com/***.send"),
        ("Email 凭据缺失=通道禁用不崩", cred_missing),
        ("Email 真发流程（SSL+login 参数）", smtp_ok),
        ("Email 4xx+地址脱敏", smtp_4xx),
        ("L2 路由 Email 主用", l2_email_primary),
        ("L2 Email 4xx 一次快换→Server酱", l2_failover),
        ("默认链全域邮件首发（26111718）", l1_email_primary),
        ("双败落 xdr alert 兜底", both_fail_fallback),
    ]
    for name, ok in checks:
        print(f"  {'✅' if ok else '❌'} {name}")
    for p in (q1, q2, q3):
        Path(p).unlink(missing_ok=True)
    return 0 if all(ok for _, ok in checks) else 1


def main(argv=None) -> int:
    argv = argv if argv is not None else sys.argv[1:]
    if "--self-test" in argv:
        return _self_test()
    if "--daily-digest" in argv:
        result = daily_digest()
        anchor = anchor_digest_schedule()   # 跑完重锚明日 17:30（防 interval 漂移）
        print(json.dumps({"digest": result, "anchor": anchor},
                         ensure_ascii=False, indent=1))
        return 0 if result["status"] == "sent" else 1
    if "--register-digest" in argv:
        result = anchor_digest_schedule()
        print(json.dumps(result, ensure_ascii=False, indent=1))
        return 0 if result.get("ok") else 1
    if "--send-test" in argv:
        result = push("L1", "notify_push 即时档验收",
                      format_body("即时档通道验收", "本条为验收用真实发送（人话模板）"),
                      evidence={"what": "2611192105 即时档真发验收：邮件通道+人话模板",
                                "action": "无需动作（验收测试）", "urgency": "不急"})
        print(json.dumps(result, ensure_ascii=False, indent=1))
        return 0 if result["status"] == "sent" else 1
    print(__doc__)
    return 0


if __name__ == "__main__":
    sys.exit(main())
