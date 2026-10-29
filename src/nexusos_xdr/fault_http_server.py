"""fault_http_server.py — 最小故障注入 HTTP 服务器（Task596，池 #52）。

测试基建：给重试/fail-open 路径做**真实故障演练**（真端口真 socket，非 mock）。
stdlib only；随机端口防并行打架；上下文管理器起停，teardown 必净。

行为（运行时可切，set_behavior）：
  ok    → 200 + JSON（默认 {"ok": true}，可 body= 自定义——如 embed 向量响应）
  500   → HTTP 500（服务端错误）
  stall → 握住连接不响应 stall_seconds（> 客户端 timeout 即客户端超时路径）
  slow  → 延迟 slow_seconds 后 200（慢而不死）
  reset → 不回包直接掐断连接（客户端见 RemoteDisconnected/ConnectionReset）

用法：
    with FaultHTTPServer() as srv:
        srv.set_behavior("500")
        ... 客户端调用（应走降级/重试路径）...
        srv.set_behavior("ok")
        ... 恢复路径 ...
        srv.requests  # [(path, behavior_at_request), ...]
"""
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class _Handler(BaseHTTPRequestHandler):
    """行为由 server 实例属性驱动（FaultHTTPServer 注入），handler 自身无状态。"""

    def _handle(self):
        # 读干 body（防客户端写端 broken pipe 噪音），记录请求时的行为快照
        try:
            length = int(self.headers.get("Content-Length") or 0)
            if length > 0:
                self.rfile.read(length)
        except Exception:
            pass
        self.server.requests.append((self.path, self.server.behavior))
        behavior = self.server.behavior

        if behavior == "reset":
            # 不发任何响应直接关连接 → 客户端 http.client.RemoteDisconnected
            try:
                self.connection.close()
            except Exception:
                pass
            return
        if behavior == "stall":
            time.sleep(self.server.stall_seconds)  # 握住；客户端 timeout 先炸
            self._reply(200, self.server.ok_body)
            return
        if behavior == "slow":
            time.sleep(self.server.slow_seconds)
            self._reply(200, self.server.ok_body)
            return
        if behavior == "500":
            self._reply(500, {"error": "fault-injected 500"})
            return
        self._reply(200, self.server.ok_body)

    do_POST = _handle
    do_GET = _handle

    def _reply(self, code, obj):
        body = json.dumps(obj).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):  # 静默（测试输出干净）
        pass


class FaultHTTPServer:
    """上下文管理器式故障服务器。with 起停；port/url/requests 只读。"""

    def __init__(self, stall_seconds: float = 5.0, slow_seconds: float = 0.5):
        self._srv = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        self._srv.daemon_threads = True  # stall 残留线程不阻塞测试进程退出
        self._srv.behavior = "ok"
        self._srv.requests = []
        self._srv.ok_body = {"ok": True}
        self._srv.stall_seconds = stall_seconds
        self._srv.slow_seconds = slow_seconds
        self._thread = threading.Thread(target=self._srv.serve_forever, daemon=True)

    @property
    def port(self) -> int:
        return self._srv.server_address[1]

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    @property
    def requests(self) -> list:
        """[(path, 请求时行为), ...]——断言重试次数/恢复顺序用。"""
        return list(self._srv.requests)

    def set_behavior(self, name: str, body: dict = None,
                     stall_seconds: float = None, slow_seconds: float = None):
        if name not in ("ok", "500", "stall", "slow", "reset"):
            raise ValueError(f"未知行为: {name}")
        self._srv.behavior = name
        if body is not None:
            self._srv.ok_body = body
        if stall_seconds is not None:
            self._srv.stall_seconds = stall_seconds
        if slow_seconds is not None:
            self._srv.slow_seconds = slow_seconds

    def __enter__(self) -> "FaultHTTPServer":
        self._thread.start()
        return self

    def __exit__(self, *exc):
        self._srv.shutdown()       # 停 accept 循环
        self._srv.server_close()   # 释放监听 socket（teardown 必净）
        return False
