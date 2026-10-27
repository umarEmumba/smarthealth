"""Stack-free tests for tools/validation-ui (quick task 261027-bfy).

No Compose stack is needed: the proxy is exercised against a stub upstream
HTTP server on 127.0.0.1, with stdlib http.client so Host/Origin/headers are
exactly controlled.
"""

from __future__ import annotations

import http.client
import importlib.util
import inspect
import json
import re
import socket
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import pytest

UI_DIR = Path(__file__).resolve().parents[2] / "tools" / "validation-ui"


def _load(name: str, file_name: str) -> Any:
    spec = importlib.util.spec_from_file_location(name, UI_DIR / file_name)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


serve = _load("validation_ui_serve", "serve.py")


# --------------------------------------------------------------------- stub


class Recorded:
    def __init__(self, method: str, target: str, headers: dict[str, str], body: bytes) -> None:
        self.method = method
        self.target = target
        self.headers = headers
        self.body = body


class StubUpstream:
    def __init__(self) -> None:
        self.requests: list[Recorded] = []
        self.responder: Callable[[BaseHTTPRequestHandler, Recorded], None] = self._echo
        stub = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args: object) -> None:
                return None

            def _any(self) -> None:
                length = int(self.headers.get("Content-Length") or 0)
                body = self.rfile.read(length) if length else b""
                rec = Recorded(
                    self.command,
                    self.path,
                    {k.lower(): v for k, v in self.headers.items()},
                    body,
                )
                stub.requests.append(rec)
                stub.responder(self, rec)

            do_GET = do_POST = do_PUT = do_PATCH = do_DELETE = do_HEAD = do_OPTIONS = _any

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.port = self.server.server_address[1]
        self.url = f"http://127.0.0.1:{self.port}"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    @staticmethod
    def _echo(handler: BaseHTTPRequestHandler, rec: Recorded) -> None:
        body = json.dumps({"ok": True}).encode()
        handler.send_response(200)
        handler.send_header("Content-Type", "application/json")
        handler.send_header("Content-Length", str(len(body)))
        handler.end_headers()
        if handler.command != "HEAD":
            handler.wfile.write(body)

    def respond(
        self,
        status: int,
        body: bytes = b"",
        headers: list[tuple[str, str]] | None = None,
        *,
        delay: float = 0.0,
    ) -> None:
        def responder(handler: BaseHTTPRequestHandler, rec: Recorded) -> None:
            if delay:
                time.sleep(delay)
            handler.send_response(status)
            for name, value in headers or []:
                handler.send_header(name, value)
            if status not in (204, 304):
                handler.send_header("Content-Length", str(len(body)))
            handler.end_headers()
            if body and handler.command != "HEAD" and status not in (204, 304):
                handler.wfile.write(body)

        self.responder = responder

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()


class Proxy:
    def __init__(self, gateway_url: str) -> None:
        self.server = serve.make_server(gateway_url, port=0)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def request(
        self,
        method: str,
        path: str,
        *,
        body: bytes | None = None,
        headers: dict[str, str] | None = None,
        host: str | None = None,
    ) -> tuple[int, dict[str, str], list[tuple[str, str]], bytes]:
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=10)
        try:
            conn.putrequest(method, path, skip_host=True, skip_accept_encoding=True)
            conn.putheader("Host", host if host is not None else f"127.0.0.1:{self.port}")
            for name, value in (headers or {}).items():
                conn.putheader(name, value)
            if body is not None:
                conn.putheader("Content-Length", str(len(body)))
            conn.endheaders(body)
            resp = conn.getresponse()
            data = resp.read()
            pairs = resp.getheaders()
            return resp.status, {k.lower(): v for k, v in pairs}, pairs, data
        finally:
            conn.close()

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()


@pytest.fixture
def stub() -> Iterator[StubUpstream]:
    s = StubUpstream()
    try:
        yield s
    finally:
        s.close()


@pytest.fixture
def proxy(stub: StubUpstream) -> Iterator[Proxy]:
    p = Proxy(stub.url)
    try:
        yield p
    finally:
        p.close()


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


# ------------------------------------------------------------------- static


def test_static_page_served_with_security_headers(proxy: Proxy) -> None:
    status, headers, _pairs, body = proxy.request("GET", "/")
    assert status == 200
    assert headers["content-type"].startswith("text/html")
    assert b"SmartHealth Phase 2.1 validation UI" in body
    assert "script-src 'self'" in headers["content-security-policy"]
    assert headers["x-content-type-options"] == "nosniff"
    for asset in ("/lib.js", "/core.js"):
        status, headers, _p, _b = proxy.request("GET", asset)
        assert status == 200, asset
        assert "javascript" in headers["content-type"]
    status, _h, _p, _b = proxy.request("GET", "/favicon.ico")
    assert status == 204


def test_every_asset_referenced_by_index_is_served(proxy: Proxy) -> None:
    html = (UI_DIR / "index.html").read_text(encoding="utf-8")
    refs = re.findall(r'(?:src|href)="([^"]+)"', html)
    assert refs, "index.html references no assets"
    for ref in refs:
        status, _h, _p, _b = proxy.request("GET", ref)
        assert status == 200, ref


def test_unknown_and_traversal_paths_are_404_and_never_proxied(proxy: Proxy, stub: StubUpstream) -> None:
    for path in ("/serve.py", "/README.md", "/../serve.py", "/static/x", "//api/auth/login"):
        status, _h, _p, _b = proxy.request("GET", path)
        assert status == 404, path
    with socket.create_connection(("127.0.0.1", proxy.port), timeout=10) as sock:
        sock.sendall(
            f"GET http://evil.example/api/x HTTP/1.1\r\nHost: 127.0.0.1:{proxy.port}\r\n\r\n".encode()
        )
        reply = sock.recv(4096)
    assert reply.startswith(b"HTTP/1.0 404") or reply.startswith(b"HTTP/1.1 404")
    assert stub.requests == []


# -------------------------------------------------------------------- proxy


def test_proxy_forwards_method_body_and_headers(proxy: Proxy, stub: StubUpstream) -> None:
    payload = b'{"slot_id": "abc"}'
    for method in ("POST", "PUT", "PATCH", "DELETE", "OPTIONS"):
        stub.requests.clear()
        status, _h, _p, _b = proxy.request(
            method,
            "/api/appointments",
            body=payload,
            headers={
                "Content-Type": "application/json",
                "Authorization": "Bearer fake.token.value",
                "Idempotency-Key": "k-1",
                "Accept-Encoding": "gzip",
            },
        )
        assert status == 200
        seen = stub.requests[0]
        assert seen.method == method
        assert seen.body == payload
        assert seen.headers["authorization"] == "Bearer fake.token.value"
        assert seen.headers["idempotency-key"] == "k-1"
        assert seen.headers["host"] == f"127.0.0.1:{stub.port}"
        assert "accept-encoding" not in seen.headers


def test_raw_path_and_query_forwarded_verbatim(proxy: Proxy, stub: StubUpstream) -> None:
    target = "/api/slots?start=2026-01-05T10%3A00%3A00%2B05%3A00&status=any"
    status, _h, _p, _b = proxy.request("GET", target)
    assert status == 200
    assert stub.requests[0].target == target


def test_status_codes_and_headers_pass_through(proxy: Proxy, stub: StubUpstream) -> None:
    cases: list[tuple[int, bytes, list[tuple[str, str]]]] = [
        (201, b'{"id":"x"}', [("Location", "/api/things/x")]),
        (204, b"", []),
        (401, b'{"detail":"invalid"}', []),
        (409, b'{"detail":"conflict"}', []),
        (422, b'{"detail":"bad"}', []),
        (429, b'{"detail":"slow"}', [("Retry-After", "7")]),
        (503, b'{"detail":"down"}', []),
        (200, b"[1,2]", [("X-Total-Count", "42")]),
    ]
    for status, body, headers in cases:
        stub.respond(status, body, headers)
        got_status, got_headers, _p, got_body = proxy.request("GET", "/api/x")
        assert got_status == status
        assert got_body == (b"" if status == 204 else body)
        for name, value in headers:
            assert got_headers[name.lower()] == value
        if status == 204:
            assert got_body == b""


def test_redirects_are_not_followed(proxy: Proxy, stub: StubUpstream) -> None:
    stub.respond(307, b"", [("Location", "/api/elsewhere")])
    status, headers, _p, _b = proxy.request("GET", "/api/x")
    assert status == 307
    assert headers["location"] == "/api/elsewhere"


def test_repeated_response_headers_survive(proxy: Proxy, stub: StubUpstream) -> None:
    stub.respond(200, b"{}", [("X-Multi", "one"), ("X-Multi", "two")])
    _s, _h, pairs, _b = proxy.request("GET", "/api/x")
    assert [v for k, v in pairs if k.lower() == "x-multi"] == ["one", "two"]


def test_head_has_no_body(proxy: Proxy, stub: StubUpstream) -> None:
    stub.respond(200, b'{"a":1}', [("X-Total-Count", "3")])
    status, headers, _p, body = proxy.request("HEAD", "/api/x")
    assert status == 200
    assert body == b""
    assert headers["x-total-count"] == "3"
    assert headers["content-length"] == str(len(b'{"a":1}'))


def test_upstream_down_is_502_json() -> None:
    port = _free_port()
    p = Proxy(f"http://127.0.0.1:{port}")
    try:
        status, headers, _p, body = p.request("GET", "/api/x")
    finally:
        p.close()
    assert status == 502
    assert headers["content-type"].startswith("application/json")
    detail = json.loads(body)["detail"]
    assert f"127.0.0.1:{port}" in detail
    assert "Traceback" not in detail


def test_upstream_timeout_is_504_json(proxy: Proxy, stub: StubUpstream, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(serve, "UPSTREAM_TIMEOUT", 0.3)
    stub.respond(200, b"{}", delay=1.0)
    status, _h, _p, body = proxy.request("GET", "/api/slow")
    assert status == 504
    assert "detail" in json.loads(body)


def test_oversized_body_is_413(proxy: Proxy, stub: StubUpstream) -> None:
    status, _h, _p, _b = proxy.request("POST", "/api/x", body=b"x" * (serve.BODY_LIMIT_BYTES + 1))
    assert status == 413
    assert stub.requests == []


def test_chunked_request_is_411(proxy: Proxy, stub: StubUpstream) -> None:
    with socket.create_connection(("127.0.0.1", proxy.port), timeout=10) as sock:
        sock.sendall(
            (
                f"POST /api/x HTTP/1.1\r\nHost: 127.0.0.1:{proxy.port}\r\n"
                "Transfer-Encoding: chunked\r\n\r\n0\r\n\r\n"
            ).encode()
        )
        reply = sock.recv(4096)
    assert b" 411 " in reply.split(b"\r\n", 1)[0]
    assert stub.requests == []


# ----------------------------------------------------------------- security


def test_binds_loopback_only(proxy: Proxy) -> None:
    assert proxy.server.server_address[0] == "127.0.0.1"
    assert serve.BIND_HOST == "127.0.0.1"
    assert "host" not in inspect.signature(serve.make_server).parameters
    assert not any("host" in name for name in inspect.signature(serve.make_server).parameters)


def test_rejects_foreign_host_and_origin(proxy: Proxy, stub: StubUpstream) -> None:
    status, _h, _p, body = proxy.request("GET", "/api/x", host="evil.example")
    assert status == 403
    assert json.loads(body)["detail"] == "forbidden host"
    status, _h, _p, body = proxy.request("GET", "/", host="evil.example")
    assert status == 403
    status, _h, _p, body = proxy.request(
        "POST", "/api/x", body=b"{}", headers={"Origin": "http://evil.example"}
    )
    assert status == 403
    assert json.loads(body)["detail"] == "forbidden origin"
    assert stub.requests == []
    status, headers, _p, _b = proxy.request(
        "POST", "/api/x", body=b"{}", headers={"Origin": f"http://127.0.0.1:{proxy.port}"}
    )
    assert status == 200
    assert "access-control-allow-origin" not in headers
    status, _h, _p, _b = proxy.request("GET", "/api/x", host=f"localhost:{proxy.port}")
    assert status == 200


def test_ui_config_reports_gateway_without_credentials(proxy: Proxy, stub: StubUpstream) -> None:
    status, _h, _p, body = proxy.request("GET", "/_ui/config")
    assert status == 200
    assert json.loads(body) == {"gateway_url": stub.url}


def test_invalid_gateway_url_is_rejected() -> None:
    for bad in ("ftp://localhost:8000", "http://", "http://user:pass@localhost:8000"):
        with pytest.raises(ValueError):
            serve.make_server(bad, port=0)


def test_cli_entrypoint_reads_gateway_url_env_and_port(stub: StubUpstream) -> None:
    import os

    port = _free_port()
    env = {**os.environ, "GATEWAY_URL": stub.url}
    proc = subprocess.Popen(
        [sys.executable, str(UI_DIR / "serve.py"), "--port", str(port)],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        body = b""
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            try:
                conn = http.client.HTTPConnection("127.0.0.1", port, timeout=2)
                conn.request("GET", "/_ui/config", headers={"Host": f"127.0.0.1:{port}"})
                resp = conn.getresponse()
                body = resp.read()
                conn.close()
                if resp.status == 200:
                    break
            except OSError:
                time.sleep(0.2)
        assert json.loads(body)["gateway_url"] == stub.url
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
        for stream in (proc.stdout, proc.stderr):
            if stream:
                stream.close()


# ------------------------------------------------------------ JS contracts

import base64
import shutil

NODE = shutil.which("node")
JS_FILES = sorted(UI_DIR.glob("*.js"))

# Names of the HTML-parsing DOM sinks, assembled so this file does not itself
# trip a repository-wide scan for them.
_BANNED_SINKS = [
    "inner" + "HTML",
    "outer" + "HTML",
    "insertAdjacent" + "HTML",
    "document" + r"\.write",
    r"\beval\s*\(",
    r"\bnew\s+Function\b",
    r"\bFunction\s*\(",
    "src" + "doc",
    r"setAttribute\(\s*['\"]on",
]


def test_js_has_no_html_injection_sinks() -> None:
    files = [*JS_FILES, UI_DIR / "index.html"]
    assert len(JS_FILES) == 4
    for path in files:
        text = path.read_text(encoding="utf-8")
        for pattern in _BANNED_SINKS:
            assert not re.search(pattern, text), f"{path.name} matches banned sink {pattern!r}"
    html = (UI_DIR / "index.html").read_text(encoding="utf-8")
    for match in re.finditer(r"<script\b([^>]*)>(.*?)</script>", html, re.S):
        assert "src=" in match.group(1), "inline script"
        assert match.group(2).strip() == "", "inline script body"
    assert not re.search(r"\sstyle\s*=", html), "style attribute in index.html"
    assert not re.search(r"\son[a-z]+\s*=", html), "event-handler attribute in index.html"


def test_js_api_paths_match_gateway_route_table() -> None:
    from services.gateway.routing import ROUTE_TABLE

    prefixes = {prefix for prefix, _service in ROUTE_TABLE}
    combined = ""
    for path in JS_FILES:
        text = path.read_text(encoding="utf-8")
        combined += text
        for literal in re.findall(r"/api/[A-Za-z0-9_-]+", text):
            assert literal in prefixes, f"{path.name}: {literal} is not a gateway route prefix"
    assert "/internal" not in combined
    for needed in (
        "/api/auth/login",
        "/api/auth/refresh",
        "/api/auth/logout",
        "/api/patients/me",
        "/api/patients",
        "/api/providers",
        "/api/clinics",
        "/api/slots",
        "/api/slots/generate",
        "/api/appointments",
        "/api/visits",
        "/availability",
        "/deactivate",
        "/cancel",
        "/reschedule",
    ):
        assert needed in combined, needed


def test_no_external_urls_or_token_literals() -> None:
    for path in sorted(UI_DIR.iterdir()):
        if path.suffix not in {".js", ".html", ".css", ".py", ".md"}:
            continue
        text = path.read_text(encoding="utf-8")
        assert "eyJ" not in text, f"{path.name} contains a JWT-looking literal"
        if path.suffix in {".js", ".html", ".css"}:
            for host in re.findall(r"https?://([A-Za-z0-9.\-\[\]:]+)", text):
                assert re.match(r"^(localhost|127\.0\.0\.1)(:\d+)?$", host), f"{path.name}: external host {host}"


@pytest.mark.skipif(NODE is None, reason="node is not on PATH")
@pytest.mark.parametrize("js", JS_FILES, ids=lambda p: p.name)
def test_js_syntax_valid(js: Path) -> None:
    result = subprocess.run([str(NODE), "--check", str(js)], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


_LIB_SCRIPT = r"""
const fs = require('fs'), vm = require('vm'), assert = require('assert');
const token = process.argv[3];
const ctx = vm.createContext({
  atob: (s) => Buffer.from(s, 'base64').toString('binary'),
  TextDecoder, crypto: require('crypto').webcrypto, Date, Math, console,
});
vm.runInContext(fs.readFileSync(process.argv[2], 'utf8'), ctx);
const V = ctx.VUI;
const claims = V.decodeJwtClaims(token);
assert.strictEqual(claims.name, 'Zoë Ünïcode');
assert.strictEqual(claims.role, 'patient');
for (const bad of ['garbage', 'a.b', 'a.!!!.c', '', null, undefined, 42, 'x.' + Buffer.from('[1]').toString('base64url') + '.z'])
  assert.strictEqual(V.decodeJwtClaims(bad), null);
assert.strictEqual(V.buildQuery({a: '1', b: '', c: null, d: undefined, e: 'x+y z', f: 0}), 'a=1&e=x%2By%20z&f=0');
assert.strictEqual(V.buildQuery({}), '');
const seen = new Set();
for (let i = 0; i < 500; i++) {
  const k = V.newIdempotencyKey();
  assert.ok(/^[A-Za-z0-9._:-]{1,128}$/.test(k), k);
  seen.add(k);
}
assert.strictEqual(seen.size, 500);
const j = (o) => JSON.stringify(o);
assert.strictEqual(j(V.pageState(0, 5, 0, 0)), j({from: 0, to: 0, hasPrev: false, hasNext: false}));
assert.strictEqual(j(V.pageState(5, 5, 2, 7)), j({from: 6, to: 7, hasPrev: true, hasNext: false}));
assert.strictEqual(j(V.pageState(0, 5, 5, 12)), j({from: 1, to: 5, hasPrev: false, hasNext: true}));
assert.strictEqual(V.pageState(0, 5, 5, null).hasNext, true);
assert.strictEqual(V.pageState(0, 5, 3, null).hasNext, false);
assert.strictEqual(V.sameResponse({status: 404, text: 'a'}, {status: 404, text: 'a'}), true);
assert.strictEqual(V.sameResponse({status: 404, text: 'a'}, {status: 404, text: 'b'}), false);
assert.strictEqual(V.sameResponse({status: 404, text: 'a'}, {status: 403, text: 'a'}), false);
assert.strictEqual(V.uppercaseEmail('Ab@x.com'), 'AB@X.COM');
assert.strictEqual(V.localDateString(new Date(2026, 0, 5, 23, 59)), '2026-01-05');
assert.strictEqual(V.localDateString(new Date(2026, 10, 30)), '2026-11-30');
assert.strictEqual(V.isUuid(V.newUuid()), true);
assert.strictEqual(V.isUuid('nope'), false);
assert.strictEqual(V.isoOffset(1000, 0), '1970-01-01T00:00:01.000Z');
const e = V.describeExp(100, 40000);
assert.strictEqual(e.secondsRemaining, 60);
assert.strictEqual(V.describeExp('x').iso, null);
console.log('lib ok');
"""


def _jwt_with_unicode() -> str:
    def b64(obj: dict[str, Any]) -> str:
        return base64.urlsafe_b64encode(json.dumps(obj, ensure_ascii=False).encode()).decode().rstrip("=")

    return f"{b64({'alg': 'none'})}.{b64({'name': 'Zoë Ünïcode', 'role': 'patient'})}.sig"


@pytest.mark.skipif(NODE is None, reason="node is not on PATH")
def test_lib_helpers_in_node(tmp_path: Path) -> None:
    script = tmp_path / "lib_check.js"
    script.write_text(_LIB_SCRIPT, encoding="utf-8")
    result = subprocess.run(
        [str(NODE), str(script), str(UI_DIR / "lib.js"), _jwt_with_unicode()],
        capture_output=True,
        text=True,
        cwd=str(tmp_path),
    )
    assert result.returncode == 0, result.stderr + result.stdout
    assert "lib ok" in result.stdout


@pytest.mark.skipif(NODE is None, reason="node is not on PATH")
def test_ui_scripts_run_in_a_fake_dom() -> None:
    """Runtime smoke (fake DOM + stub fetch): all scripts load, every panel
    builds, the safe builder refuses unsafe attributes, and login / refresh /
    replay / me / list flows log PASS badges. Not a visual browser check."""
    smoke = Path(__file__).resolve().parent / "fake_dom_smoke.js"
    result = subprocess.run([str(NODE), str(smoke), str(UI_DIR)], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr + result.stdout
    assert "smoke ok" in result.stdout
