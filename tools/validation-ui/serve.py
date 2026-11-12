#!/usr/bin/env python3
"""Same-origin static server + /api/* reverse proxy for the Phase 2.1 validation UI.

Why it exists: the SmartHealth gateway has no CORS, so a page loaded from any
other origin cannot call it from the browser. This server serves the UI files
and forwards every ``/api/*`` request to the gateway, so the page and the API
share one origin (http://127.0.0.1:8099 by default).

Usage::

    .venv/bin/python tools/validation-ui/serve.py            # or: python3 ...
    GATEWAY_URL=http://localhost:8000 VALIDATION_UI_PORT=8099 python3 ...
    python3 tools/validation-ui/serve.py --port 8100 --gateway-url http://localhost:8000

Standard library only (no third-party dependency).

Security rules (see the README of this directory):

* Binds 127.0.0.1 only. There is deliberately NO host option (not on the CLI,
  not in the environment, not as a function parameter).
* Every request must carry ``Host: 127.0.0.1:<port>`` or ``localhost:<port>``
  and, when an ``Origin`` header is present, a matching one; anything else is
  answered 403 before any other handling (DNS-rebinding and cross-site guard).
  The proxy never adds CORS headers.
* Only an explicit allowlist of static files is served, always resolved against
  this directory and never from request-derived text.
* Only request targets starting with the literal ``/api/`` are forwarded, and
  only to the single configured gateway. Redirects are never followed and
  4xx/5xx responses are never rewritten.
* Logs carry ``METHOD path status`` only, never headers, query strings, bodies
  or tokens.
"""

from __future__ import annotations

import argparse
import http.client
import json
import os
import sys
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

# The loopback address is a constant on purpose: nothing may widen the bind.
BIND_HOST = "127.0.0.1"
DEFAULT_PORT = 8099
DEFAULT_GATEWAY_URL = "http://localhost:8000"
BODY_LIMIT_BYTES = 1_048_576
# Read at request time (module global) so tests can monkeypatch it.
UPSTREAM_TIMEOUT = 30.0

BASE_DIR = Path(__file__).resolve().parent

_JS = "text/javascript; charset=utf-8"
STATIC_ROUTES: dict[str, tuple[str, str]] = {
    "/": ("index.html", "text/html; charset=utf-8"),
    "/index.html": ("index.html", "text/html; charset=utf-8"),
    "/lib.js": ("lib.js", _JS),
    "/core.js": ("core.js", _JS),
    "/panels_booking.js": ("panels_booking.js", _JS),
    "/panels_ops.js": ("panels_ops.js", _JS),
    "/style.css": ("style.css", "text/css; charset=utf-8"),
}

CONTENT_SECURITY_POLICY = (
    "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; "
    "img-src 'self' data:; base-uri 'none'; form-action 'none'; frame-ancestors 'none'"
)

HOP_BY_HOP = frozenset(
    {
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "trailers",
        "transfer-encoding",
        "upgrade",
    }
)
# Never forwarded to the gateway as sent: Host and Content-Length are
# recomputed by http.client; Accept-Encoding is dropped so the body comes back
# as plain bytes this server can relay untouched.
_REQUEST_DROPPED = HOP_BY_HOP | {"host", "content-length", "accept-encoding"}
# Response headers this server owns (send_response already emits Server/Date).
_RESPONSE_DROPPED = HOP_BY_HOP | {"content-length", "server", "date"}

_BODY_METHODS = frozenset({"POST", "PUT", "PATCH"})
_DRAIN_CEILING = 16 * 1024 * 1024


def parse_gateway_url(gateway_url: str) -> urllib.parse.ParseResult:
    """Validate and parse the gateway URL (http/https, a host, no userinfo)."""
    try:
        parsed = urllib.parse.urlparse(gateway_url)
        hostname = parsed.hostname
        parsed.port  # noqa: B018 - raises ValueError for a malformed port
    except ValueError as exc:
        raise ValueError(f"invalid gateway URL: {exc}") from exc
    if parsed.scheme not in ("http", "https"):
        raise ValueError("gateway URL must use http or https")
    if not hostname:
        raise ValueError("gateway URL must include a host")
    if parsed.username is not None or parsed.password is not None or "@" in parsed.netloc:
        raise ValueError("gateway URL must not contain credentials")
    return parsed


class ProxyHandler(BaseHTTPRequestHandler):
    """One handler for static files, /_ui/config and the /api/* proxy.

    The per-server settings below are filled in by ``make_server`` on a
    subclass, after the socket is bound (the allowed Host values need the
    real port, which matters when port 0 was requested).
    """

    server_version = "ValidationUI/1.0"
    allowed_hosts: frozenset[str] = frozenset()
    allowed_origins: frozenset[str] = frozenset()
    gateway_url: str = DEFAULT_GATEWAY_URL
    gateway: urllib.parse.ParseResult = urllib.parse.urlparse(DEFAULT_GATEWAY_URL)

    # ---- logging: never headers, query strings, bodies or tokens -----------
    def log_request(self, code: object = "-", size: object = "-") -> None:
        path = (self.path or "-").split("?", 1)[0]
        sys.stderr.write(f"{self.command} {path} {code}\n")

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        return None

    def log_error(self, format: str, *args: object) -> None:  # noqa: A002
        sys.stderr.write("protocol error (request rejected)\n")

    # ---- response helpers ---------------------------------------------------
    def _send_json(self, status: int, payload: object, *, extra: list[tuple[str, str]] | None = None) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        for name, value in extra or []:
            self.send_header(name, value)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def _raw_target(self) -> str:
        """The request target exactly as received (BaseHTTPRequestHandler
        rewrites a leading '//' in ``self.path``; the request line is raw)."""
        parts = self.requestline.split(" ")
        if len(parts) == 3:
            return parts[1]
        return self.path

    # ---- request entry point --------------------------------------------------
    def _handle(self) -> None:
        host = (self.headers.get("Host") or "").strip().lower()
        if host not in self.allowed_hosts:
            self._send_json(403, {"detail": "forbidden host"})
            return
        origin = self.headers.get("Origin")
        if origin is not None and origin.strip().lower() not in self.allowed_origins:
            self._send_json(403, {"detail": "forbidden origin"})
            return

        target = self._raw_target()
        route = target.split("?", 1)[0]

        if route in STATIC_ROUTES:
            self._serve_static(route)
            return
        if route == "/favicon.ico":
            self.send_response(204)
            self.end_headers()
            return
        if route == "/_ui/config":
            if self.command not in ("GET", "HEAD"):
                self._send_json(405, {"detail": "method not allowed"}, extra=[("Allow", "GET, HEAD")])
                return
            self._send_json(200, {"gateway_url": self.gateway_url})
            return
        if target.startswith("/api/"):
            self._proxy(target)
            return
        self._send_json(404, {"detail": "not found"})

    do_GET = do_POST = do_PUT = do_PATCH = do_DELETE = do_HEAD = do_OPTIONS = _handle

    # ---- static files ---------------------------------------------------------
    def _serve_static(self, route: str) -> None:
        if self.command not in ("GET", "HEAD"):
            self._send_json(405, {"detail": "method not allowed"}, extra=[("Allow", "GET, HEAD")])
            return
        file_name, content_type = STATIC_ROUTES[route]
        try:
            data = (BASE_DIR / file_name).read_bytes()
        except OSError:
            self._send_json(404, {"detail": "not found"})
            return
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header("Content-Security-Policy", CONTENT_SECURITY_POLICY)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(data)

    # ---- reverse proxy --------------------------------------------------------
    def _read_body(self) -> bytes | None:
        """The request body, or None after an error response was sent."""
        if self.headers.get("Transfer-Encoding") is not None:
            self._send_json(411, {"detail": "chunked request bodies are not supported; send Content-Length"})
            return None
        raw_length = self.headers.get("Content-Length")
        if raw_length is None:
            return b""
        try:
            length = int(raw_length.strip())
        except ValueError:
            length = -1
        if length < 0:
            self._send_json(400, {"detail": "invalid Content-Length"})
            return None
        if length > BODY_LIMIT_BYTES:
            # Drain (discard) a bounded amount so the client can read the 413
            # instead of hitting a connection reset while it is still sending.
            remaining = min(length, _DRAIN_CEILING)
            while remaining > 0:
                chunk = self.rfile.read(min(65536, remaining))
                if not chunk:
                    break
                remaining -= len(chunk)
            self._send_json(413, {"detail": f"request body exceeds {BODY_LIMIT_BYTES} bytes"})
            return None
        return self.rfile.read(length) if length else b""

    def _connection_listed(self) -> set[str]:
        listed: set[str] = set()
        for value in self.headers.get_all("Connection") or []:
            listed.update(token.strip().lower() for token in value.split(",") if token.strip())
        return listed

    def _proxy(self, target: str) -> None:
        body = self._read_body()
        if body is None:
            return
        gateway = self.gateway
        base_path = gateway.path.rstrip("/")
        dropped = _REQUEST_DROPPED | self._connection_listed()
        gateway_label = f"{gateway.scheme}://{gateway.netloc}"
        conn_cls = http.client.HTTPSConnection if gateway.scheme == "https" else http.client.HTTPConnection
        conn = conn_cls(gateway.hostname, gateway.port, timeout=UPSTREAM_TIMEOUT)
        try:
            try:
                conn.putrequest(self.command, base_path + target, skip_accept_encoding=True)
                for name, value in self.headers.items():
                    if name.lower() in dropped:
                        continue
                    conn.putheader(name, value)
                if body or self.command in _BODY_METHODS:
                    conn.putheader("Content-Length", str(len(body)))
                conn.endheaders(body or None)
                upstream = conn.getresponse()
                payload = upstream.read()
                headers = upstream.getheaders()
                status, reason = upstream.status, upstream.reason
            except (http.client.InvalidURL, ValueError):
                self._send_json(400, {"detail": "request could not be forwarded (invalid URL or header)"})
                return
            except TimeoutError:
                self._send_json(504, {"detail": f"gateway at {gateway_label} timed out"})
                return
            except (OSError, http.client.HTTPException):
                self._send_json(502, {"detail": f"gateway at {gateway_label} is unreachable"})
                return
        finally:
            conn.close()

        no_body = status in (204, 304) or 100 <= status < 200
        self.send_response(status, reason)
        for name, value in headers:
            lowered = name.lower()
            if lowered in _RESPONSE_DROPPED:
                continue
            self.send_header(name, value)
        if self.command == "HEAD":
            upstream_length = next((v for n, v in headers if n.lower() == "content-length"), None)
            if upstream_length is not None and not no_body:
                self.send_header("Content-Length", upstream_length)
        elif not no_body:
            self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        if self.command != "HEAD" and not no_body:
            self.wfile.write(payload)


def make_server(gateway_url: str = DEFAULT_GATEWAY_URL, port: int = DEFAULT_PORT) -> ThreadingHTTPServer:
    """A ThreadingHTTPServer on (127.0.0.1, port); port 0 picks a free port."""
    gateway = parse_gateway_url(gateway_url)

    class BoundHandler(ProxyHandler):
        pass

    server = ThreadingHTTPServer((BIND_HOST, port), BoundHandler)
    server.daemon_threads = True
    bound_port = server.server_address[1]
    hosts = (f"127.0.0.1:{bound_port}", f"localhost:{bound_port}")
    BoundHandler.allowed_hosts = frozenset(hosts)
    BoundHandler.allowed_origins = frozenset(f"http://{h}" for h in hosts)
    BoundHandler.gateway_url = gateway_url
    BoundHandler.gateway = gateway
    return server


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Serve the Phase 2.1 validation UI and proxy /api/* to the gateway (loopback only).",
        epilog="Environment: GATEWAY_URL, VALIDATION_UI_PORT (the flags win).",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("VALIDATION_UI_PORT") or DEFAULT_PORT),
        help=f"port to listen on, 127.0.0.1 only (default env VALIDATION_UI_PORT else {DEFAULT_PORT})",
    )
    parser.add_argument(
        "--gateway-url",
        default=os.environ.get("GATEWAY_URL") or DEFAULT_GATEWAY_URL,
        help=f"gateway base URL (default env GATEWAY_URL else {DEFAULT_GATEWAY_URL})",
    )
    args = parser.parse_args(argv)
    try:
        server = make_server(args.gateway_url, args.port)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    except OSError as exc:
        print(f"error: cannot listen on {BIND_HOST}:{args.port}: {exc}", file=sys.stderr)
        return 1
    port = server.server_address[1]
    print(f"validation UI on http://{BIND_HOST}:{port}/  ->  gateway {args.gateway_url}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
