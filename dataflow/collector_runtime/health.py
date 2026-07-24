import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Lock, Thread
from typing import Callable


class WorkloadHealth:
    def __init__(self, stale_after_seconds: int, clock: Callable[[], float] = time.monotonic):
        self._stale_after_seconds = stale_after_seconds
        self._clock = clock
        self._ready = False
        self._last_heartbeat = clock()
        self._lock = Lock()

    def mark_ready(self) -> None:
        with self._lock:
            self._ready = True

    def heartbeat(self) -> None:
        with self._lock:
            self._last_heartbeat = self._clock()

    def status(self, path: str) -> tuple[int, bytes]:
        if path not in {"/live", "/ready"}:
            return 404, b"not found\n"
        with self._lock:
            ready = self._ready
            stale = self._clock() - self._last_heartbeat > self._stale_after_seconds
        if stale:
            return 503, b"stale\n"
        if path == "/ready" and not ready:
            return 503, b"not ready\n"
        return (200, b"ready\n") if path == "/ready" else (200, b"live\n")


def start_health_server(health: WorkloadHealth, port: int) -> ThreadingHTTPServer:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            status, body = health.status(self.path)
            self.send_response(status)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:
            return

    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    Thread(target=server.serve_forever, name="health-server", daemon=True).start()
    return server
