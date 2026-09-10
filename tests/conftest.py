"""Shared fixtures: a scriptable in-process ingest server.

Tests run against a real HTTP server rather than a mocked transport wherever the
behavior under test involves status codes, headers or connection reuse, because
those are exactly the places a client can be subtly wrong.
"""

from __future__ import annotations

import contextlib
import http.server
import threading
from collections import deque
from typing import Deque, Dict, List, Optional, Tuple

import pytest

from prostometrics import _clock


class RecordedRequest:
    __slots__ = ("body", "headers", "path")

    def __init__(self, path: str, headers: Dict[str, str], body: bytes) -> None:
        self.path = path
        self.headers = headers
        self.body = body

    @property
    def text(self) -> str:
        return self.body.decode("utf-8")

    @property
    def lines(self) -> List[str]:
        return [line for line in self.text.split("\n") if line]

    def event_lines(self) -> List[str]:
        return [line for line in self.lines if not line.startswith(("H|", "S|"))]

    def series_lines(self) -> List[str]:
        return [line for line in self.lines if line.startswith("S|")]

    def header_line(self) -> str:
        return next(line for line in self.lines if line.startswith("H|"))


ScriptedResponse = Tuple[int, bytes, Dict[str, str]]


class DecodedEvent:
    """One event with its dictionary reference resolved back to a metric name."""

    __slots__ = ("kind", "labels", "metric", "session", "timestamp", "value")

    def __init__(self, session: str, kind: str, metric: str, labels: List[str], value: str, timestamp: int) -> None:
        self.session = session
        self.kind = kind
        self.metric = metric
        self.labels = labels
        self.value = value
        self.timestamp = timestamp

    def __repr__(self) -> str:
        return f"DecodedEvent({self.kind}|{self.metric}|{self.labels}|{self.value})"


class FakeIngester:
    """An ingest endpoint that records requests and can be scripted to fail."""

    def __init__(self) -> None:
        self.requests: List[RecordedRequest] = []
        self.scripted: Deque[ScriptedResponse] = deque()
        self._lock = threading.Lock()
        self._server: Optional[http.server.ThreadingHTTPServer] = None
        self._thread: Optional[threading.Thread] = None

    # -- lifecycle -----------------------------------------------------------

    def start(self) -> None:
        ingester = self

        class Handler(http.server.BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def do_POST(self) -> None:
                length = int(self.headers.get("Content-Length", "0"))
                body = self.rfile.read(length)
                headers = {key.lower(): value for key, value in self.headers.items()}
                status, payload, extra = ingester._handle(self.path, headers, body)
                self.send_response(status)
                for key, value in extra.items():
                    self.send_header(key, value)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                if payload:
                    self.wfile.write(payload)

            def log_message(self, *args: object) -> None:
                return

        class Server(http.server.ThreadingHTTPServer):
            daemon_threads = True
            block_on_close = False

        self._server = Server(("127.0.0.1", 0), Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=5)

    @property
    def url(self) -> str:
        assert self._server is not None
        host, port = self._server.server_address[:2]
        return f"http://{host}:{port}/api/i/batch"

    # -- scripting -----------------------------------------------------------

    def enqueue(self, status: int, body: bytes = b"", headers: Optional[Dict[str, str]] = None) -> None:
        """Queue one response, consumed by the next request."""
        with self._lock:
            self.scripted.append((status, body, dict(headers or {})))

    def enqueue_many(
        self, count: int, status: int, body: bytes = b"", headers: Optional[Dict[str, str]] = None
    ) -> None:
        for _ in range(count):
            self.enqueue(status, body, headers)

    def _handle(self, path: str, headers: Dict[str, str], body: bytes) -> ScriptedResponse:
        with self._lock:
            self.requests.append(RecordedRequest(path, headers, body))
            if self.scripted:
                return self.scripted.popleft()
        decoded = body.decode("utf-8").split("\n")
        accepted = len([line for line in decoded if line and not line.startswith(("H|", "S|"))])
        return (
            202,
            b"",
            {"X-PM-Accepted": str(accepted), "X-PM-Dropped": "0", "X-PM-Rejected": "0"},
        )

    # -- assertions ----------------------------------------------------------

    def all_event_lines(self) -> List[str]:
        with self._lock:
            return [line for request in self.requests for line in request.event_lines()]

    def decoded_events(self) -> List[DecodedEvent]:
        """Resolve every recorded event back to its metric name and labels.

        The wire format sends a metric name once per dictionary session, so
        assertions about which metric an event belongs to have to replay the
        dictionary the way the server does.
        """
        with self._lock:
            requests = list(self.requests)

        dictionaries: Dict[str, Dict[int, Tuple[str, List[str]]]] = {}
        decoded: List[DecodedEvent] = []
        for request in requests:
            session = ""
            for line in request.lines:
                parts = line.split("|")
                if line.startswith("H|"):
                    session = parts[3]
                    dictionaries.setdefault(session, {})
                    continue
                if line.startswith("S|"):
                    dictionaries.setdefault(session, {})[int(parts[1])] = (parts[2], parts[3:])
                    continue
                series = dictionaries.get(session, {}).get(int(parts[1]))
                if series is None:
                    continue
                decoded.append(
                    DecodedEvent(
                        session=session,
                        kind=parts[0],
                        metric=series[0],
                        labels=series[1],
                        value=parts[2],
                        timestamp=int(parts[3]),
                    )
                )
        return decoded

    def request_count(self) -> int:
        with self._lock:
            return len(self.requests)

    def wait_for_requests(self, count: int, timeout: float = 5.0) -> bool:
        deadline = threading.Event()
        waited = 0.0
        step = 0.02
        while waited < timeout:
            if self.request_count() >= count:
                return True
            deadline.wait(step)
            waited += step
        return self.request_count() >= count


@pytest.fixture()
def ingester() -> FakeIngester:
    server = FakeIngester()
    server.start()
    try:
        yield server
    finally:
        server.stop()


@pytest.fixture(autouse=True)
def real_clock():
    """Guarantee no test leaks a frozen clock into the next one."""
    yield
    _clock.reset_sources()


class FakeClock:
    """A controllable monotonic and wall clock."""

    def __init__(self, monotonic: float = 1000.0, wall: float = 1_700_000_000.0) -> None:
        self._monotonic = monotonic
        self._wall = wall

    def install(self) -> None:
        _clock.set_sources(lambda: self._monotonic, lambda: self._wall)

    def advance(self, seconds: float) -> None:
        self._monotonic += seconds
        self._wall += seconds


@pytest.fixture()
def make_transport():
    """Build HTTP transports and guarantee their sockets are closed after the test."""
    from prostometrics._transport import HTTPTransport

    created = []

    def factory(endpoint: str, **kwargs) -> HTTPTransport:
        transport = HTTPTransport(endpoint=endpoint, **kwargs)
        created.append(transport)
        return transport

    try:
        yield factory
    finally:
        for transport in created:
            transport.close()


@pytest.fixture()
def manual_client(ingester):
    """A client whose worker thread is parked so tests can drive flushes directly.

    Timing-sensitive behavior — backoff, replay pacing, the authentication grace
    — is asserted by stepping the client deliberately rather than by sleeping,
    so the tests are both fast and free of flakes.
    """
    from prostometrics import Client

    created = []

    def factory(**kwargs):
        kwargs.setdefault("api_key", "12_secret")
        kwargs.setdefault("endpoint", ingester.url)
        client = Client(kwargs.pop("workload", "api"), **kwargs)
        client._closing = True
        client._wake.set()
        worker = client._worker
        if worker is not None:
            worker.join(timeout=5)
        client._closing = False
        created.append(client)
        return client

    try:
        yield factory
    finally:
        for client in created:
            client._closing = True
            client._closed = True
            with contextlib.suppress(Exception):
                client._config.transport.close()
