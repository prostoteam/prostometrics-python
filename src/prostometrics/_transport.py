"""HTTP delivery of encoded batches.

Uses ``http.client`` from the standard library rather than a third-party HTTP
package. A metrics client is installed alongside whatever the application
already depends on, so pulling in a pinned HTTP stack would create version
conflicts in exactly the projects it is meant to be invisible to.

The connection is owned by the worker thread and reused across batches. It is
never touched from application threads, so it needs no locking.
"""

from __future__ import annotations

import contextlib
import email.utils
import gzip
import http.client
import json
import logging
import os
import socket
import ssl
from typing import Dict, Optional, Tuple
from urllib.parse import urlsplit

from ._clock import monotonic_ms, wall_seconds
from ._constants import (
    ACCEPTED_HEADER_NAME,
    BATCH_ID_HEADER_NAME,
    COMPRESS_MIN_BYTES,
    DEFAULT_STOP_RESPONSE_CODES,
    DEFAULT_STOP_STATUS_CODES,
    DICTIONARY_RESYNC_WARNING_THRESHOLD,
    DICTIONARY_RESYNC_WARNING_WINDOW_MS,
    DROPPED_HEADER_NAME,
    MAX_ERROR_BODY_BYTES,
    REJECTED_HEADER_NAME,
    WORKLOAD_HEADER_NAME,
)
from ._dictionary import (
    DictionaryState,
    analyze_payload_v5,
    encode_payload_v5,
    rollback_dictionary,
    should_reset_dictionary,
    snapshot_dictionary,
)
from ._errors import HTTPTransportError, StopIngestError
from ._payload import Payload
from ._workload import validate_workload

_UNKNOWN_DICTIONARY_CODE = "unknown_series_dictionary"


def _compress_body(body: bytes) -> bytes:
    """Return the bytes to send, compressed when that is worth doing.

    A batch body is the same handful of shapes repeated line after line -- the
    series ids, the second, the metric names, the paths -- and gives up about
    three quarters of its size to gzip. The lowest level is deliberate: it keeps
    most of that saving for a fraction of the processor time, and this runs in
    the caller's application rather than ours.

    gzip is the choice because it is the only one every client can reach without
    carrying a compressor: browsers offer it and nothing else for outgoing data,
    and the standard library has it in Python, Go and Node alike.

    A body too small to be worth it, or one that failed to compress, is returned
    as it came: the encoding is an optimisation and never a reason to lose a
    batch.
    """
    if len(body) < COMPRESS_MIN_BYTES:
        return body
    try:
        compressed = gzip.compress(body, compresslevel=1)
    except Exception:  # pragma: no cover - defensive
        return body
    # Already-compressed content can grow. Nothing a batch carries does, but
    # sending more bytes than we were given never makes sense.
    return compressed if len(compressed) < len(body) else body


class HTTPTransport:
    """Sends batches to an ingest endpoint over a reused HTTP connection."""

    def __init__(
        self,
        endpoint: str,
        api_key: str = "",
        headers: Optional[Dict[str, str]] = None,
        logger: Optional[logging.Logger] = None,
        verbose: bool = False,
        stop_status_codes: Tuple[int, ...] = DEFAULT_STOP_STATUS_CODES,
        stop_response_codes: Tuple[str, ...] = DEFAULT_STOP_RESPONSE_CODES,
    ) -> None:
        self.endpoint = endpoint
        self.api_key = api_key
        self.headers = dict(headers or {})
        self.logger = logger if logger is not None else logging.getLogger("prostometrics")
        self.verbose = verbose
        self.stop_status_codes = tuple(stop_status_codes)
        self.stop_response_codes = tuple(code.strip().lower() for code in stop_response_codes)

        parts = urlsplit(endpoint)
        self._secure = parts.scheme != "http"
        self._host = parts.hostname or ""
        self._port = parts.port
        self._path = parts.path or "/"
        if parts.query:
            self._path = f"{self._path}?{parts.query}"

        self._connection: Optional[http.client.HTTPConnection] = None
        self._dictionary: Optional[DictionaryState] = None
        self._resync_window_start = 0
        self._resync_count = 0

    def send(self, payload: Payload, workload: str, timeout: float) -> None:
        """Encode and deliver one batch, recovering from a dictionary miss."""
        if not self.endpoint:
            raise HTTPTransportError(self.endpoint, detail="empty endpoint")
        workload = validate_workload(workload)
        if payload.is_empty():
            return

        if should_reset_dictionary(self._dictionary):
            self._dictionary = DictionaryState()

        state = self._dictionary
        assert state is not None
        try:
            self._encode_and_send(payload, state, workload, timeout, force_definitions=False)
            return
        except HTTPTransportError as err:
            if err.status_code == 413:
                # An oversized body will not get smaller by resending; start a
                # fresh dictionary so the next batch is not encoded against a
                # session the server may already have discarded.
                self._dictionary = DictionaryState()
                raise
            if err.status_code != 409 or err.response_code != _UNKNOWN_DICTIONARY_CODE:
                raise

        # The server could not resolve the dictionary this batch referenced.
        # Restart the session and restate every definition before retrying once.
        self._dictionary = DictionaryState()
        try:
            self._encode_and_send(payload, self._dictionary, workload, timeout, force_definitions=True)
        except HTTPTransportError as retry_err:
            if retry_err.status_code == 413:
                self._dictionary = DictionaryState()
            raise
        self._note_dictionary_resync()

    def _encode_and_send(
        self,
        payload: Payload,
        state: DictionaryState,
        workload: str,
        timeout: float,
        force_definitions: bool,
    ) -> None:
        """Encode one batch and send it, keeping the dictionary honest on failure.

        The session may only remember a series definition once the request
        carrying it has been accepted. Any other outcome rolls the registration
        back, so a retry re-sends the definitions with the events that need them.
        """
        snapshot = snapshot_dictionary(state)
        body = encode_payload_v5(payload, state, force_definitions=force_definitions)
        if not body:
            return
        try:
            self._send_body(body, payload.batch_id, workload, timeout)
        except BaseException:
            rollback_dictionary(state, snapshot)
            raise

    def reset_after_fork(self) -> None:
        """Abandon inherited connection and dictionary state in a forked child.

        The socket is closed by descriptor rather than through the socket object
        so that no TLS shutdown record is written to a connection the parent
        process is still using. The dictionary session is dropped as well: two
        processes sharing a session ID while diverging in revision would make
        the server reject both.
        """
        connection = self._connection
        self._connection = None
        self._dictionary = None
        self._resync_window_start = 0
        self._resync_count = 0
        if connection is None or connection.sock is None:
            return
        try:
            descriptor = connection.sock.detach()
        except Exception:
            return
        if descriptor is not None and descriptor >= 0:
            with contextlib.suppress(OSError):
                os.close(descriptor)

    def close(self) -> None:
        connection = self._connection
        self._connection = None
        if connection is not None:
            with contextlib.suppress(Exception):
                connection.close()

    def _connect(self, timeout: float) -> http.client.HTTPConnection:
        if self._connection is not None:
            return self._connection
        if self._secure:
            self._connection = http.client.HTTPSConnection(
                self._host,
                self._port,
                timeout=timeout,
                context=ssl.create_default_context(),
            )
        else:
            self._connection = http.client.HTTPConnection(self._host, self._port, timeout=timeout)
        return self._connection

    def _drop_connection(self) -> None:
        connection = self._connection
        self._connection = None
        if connection is not None:
            with contextlib.suppress(Exception):
                connection.close()

    def _send_body(self, body: bytes, batch_id: str, workload: str, timeout: float) -> None:
        raw_length = len(body)
        encoded = _compress_body(body)
        headers = {
            "Content-Type": "text/plain; charset=utf-8",
            "Content-Length": str(len(encoded)),
            "Connection": "keep-alive",
        }
        if encoded is not body:
            headers["Content-Encoding"] = "gzip"
        headers.update(self.headers)
        if self.api_key:
            headers["Authorization"] = self.api_key
        headers[WORKLOAD_HEADER_NAME] = workload
        if batch_id.strip():
            headers[BATCH_ID_HEADER_NAME] = batch_id.strip()

        try:
            connection = self._connect(timeout)
            connection.request("POST", self._path, body=encoded, headers=headers)
            response = connection.getresponse()
            status = response.status
            reason = response.reason or ""
            response_headers = {key.lower(): value for key, value in response.getheaders()}
            raw_body = response.read()
        except (OSError, socket.timeout, http.client.HTTPException) as err:
            # A half-used connection cannot be reused safely, whatever the cause.
            self._drop_connection()
            raise HTTPTransportError(
                self.endpoint,
                batch_id=batch_id,
                detail=f"{type(err).__name__}: {err}",
                request_bytes=raw_length,
            ) from err

        accepted = _parse_count(response_headers.get(ACCEPTED_HEADER_NAME.lower()))
        dropped = _parse_count(response_headers.get(DROPPED_HEADER_NAME.lower()))
        rejected = _parse_count(response_headers.get(REJECTED_HEADER_NAME.lower()))

        if 200 <= status < 300:
            # A 202 can still carry losses, so the counts decide what to report.
            if dropped > 0 or rejected > 0:
                self.logger.warning(
                    "prostometrics: ingest accepted partial batch batch_id=%s accepted=%d dropped=%d "
                    "rejected=%d endpoint=%s",
                    batch_id,
                    max(accepted, 0),
                    max(dropped, 0),
                    max(rejected, 0),
                    self.endpoint,
                )
            return

        detail = _compact(raw_body[:MAX_ERROR_BODY_BYTES])
        response_code = _extract_response_code(detail)
        diagnostics = analyze_payload_v5(body, self._dictionary)
        error = HTTPTransportError(
            self.endpoint,
            batch_id=batch_id,
            status_code=status,
            status=f"{status} {reason}".strip(),
            response_code=response_code,
            detail=detail,
            accepted=accepted,
            dropped=dropped,
            rejected=rejected,
            retry_after_ms=_parse_retry_after(response_headers.get("retry-after")),
            request_bytes=len(body),
            dictionary_session=diagnostics.dictionary_session,
            dictionary_revision=diagnostics.dictionary_revision,
            dictionary_series=diagnostics.dictionary_series,
            series_definitions=diagnostics.series_definitions,
            event_lines=diagnostics.event_lines,
        )

        if status in self.stop_status_codes or (response_code and response_code in self.stop_response_codes):
            raise StopIngestError(
                f"prostometrics: stop ingesting after HTTP {status}",
                code=status,
                cause=error,
            )
        raise error

    def _note_dictionary_resync(self) -> None:
        """Log a recovered dictionary miss, warning only if they keep happening.

        One resync is routine. Repeated ones point at ingester cache churn or a
        load balancer spreading a session across instances, which is worth
        surfacing even when the client itself is coping.
        """
        now = monotonic_ms()
        if self._resync_window_start == 0 or now - self._resync_window_start > DICTIONARY_RESYNC_WARNING_WINDOW_MS:
            self._resync_window_start = now
            self._resync_count = 0
        self._resync_count += 1
        if self.verbose:
            self.logger.debug("prostometrics: ingester dictionary miss recovered by resync")
            return
        if self._resync_count == DICTIONARY_RESYNC_WARNING_THRESHOLD:
            self.logger.warning(
                "prostometrics: repeated ingester dictionary resyncs count=%d window_ms=%d; "
                "check ingester cache churn or load balancing",
                self._resync_count,
                DICTIONARY_RESYNC_WARNING_WINDOW_MS,
            )


def _parse_count(raw: Optional[str]) -> int:
    if raw is None or not raw.strip():
        return -1
    try:
        return int(raw.strip())
    except ValueError:
        return -1


def _parse_retry_after(raw: Optional[str]) -> int:
    """Return the Retry-After delay in milliseconds, in seconds or date form."""
    if raw is None:
        return 0
    text = raw.strip()
    if not text:
        return 0
    if text.isdigit():
        return int(text) * 1000
    try:
        when = email.utils.parsedate_to_datetime(text)
    except (TypeError, ValueError):
        return 0
    delta = int(when.timestamp()) - wall_seconds()
    return delta * 1000 if delta > 0 else 0


def _compact(raw: bytes) -> str:
    return " ".join(raw.decode("utf-8", "replace").split())


def _extract_response_code(detail: str) -> str:
    if not detail:
        return ""
    try:
        parsed = json.loads(detail)
    except (ValueError, TypeError):
        return ""
    if isinstance(parsed, dict):
        code = parsed.get("code")
        if isinstance(code, str):
            return code.strip().lower()
    return ""
