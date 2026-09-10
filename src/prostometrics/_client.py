"""The metrics client.

Design in one paragraph: every metric call appends to a bounded in-memory queue
and returns. A single background thread owns everything else — batching, the
dictionary session, the HTTP connection, retries and the outage buffer. A thread
was chosen over ``asyncio`` deliberately: it behaves identically in a threaded
application such as Django or Celery and in an event-loop application such as
FastAPI, so one client serves both without the caller choosing a variant.
"""

from __future__ import annotations

import contextlib
import logging
import math
import random
import secrets
import threading
from typing import Dict, List, Optional, Set, Tuple

from . import _fork
from ._api_key import looks_like_client_key, refusal_hint
from ._batch_builder import BatchBuilder
from ._clock import monotonic_ms, wall_seconds
from ._config import Config, Transport, build_config
from ._constants import (
    DEFAULT_AUTH_GRACE_RETRY_INTERVAL_MS,
    DEFAULT_AUTH_GRACE_WINDOW_MS,
    DEFAULT_CLIENT_BACKOFF_JITTER_WINDOW_MS,
    DEFAULT_CLIENT_BACKOFF_MAX_DELAY_MS,
    DEFAULT_FLUSH_INTERVAL_MS,
    DEFAULT_FLUSH_TIMEOUT_MS,
    DEFAULT_MAX_BATCH_BYTES,
    DEFAULT_MAX_BATCH_SERIES,
    DEFAULT_MAX_BATCH_SIZE,
    DEFAULT_MAX_TOTAL_SERIES,
    DEFAULT_OUTAGE_BUFFER_MAX_AGE_MS,
    DEFAULT_OUTAGE_BUFFER_MAX_BYTES,
    DEFAULT_OUTAGE_BUFFER_MAX_EVENTS,
    DEFAULT_QUEUE_SIZE,
    DEFAULT_RECOVERY_JITTER_WINDOW_MS,
    DEFAULT_REPLAY_INTERVAL_MS,
    DEFAULT_RETRY_BASE_DELAY_MS,
    DEFAULT_RETRY_FLUSH_MAX_SENDS,
    DEFAULT_RETRY_JITTER_WINDOW_MS,
    DEFAULT_RETRY_MAX_DELAY_MS,
    DEFAULT_RETRY_QUEUE_SIZE,
    HTTP_STATUS_UNAUTHORIZED,
    LOCAL_DROP_LOG_INTERVAL_MS,
    MAX_COUNTER_VALUE,
    MAX_METRIC_BYTES,
    MAX_SAMPLE_VALUE,
    RESPONSE_CODE_UNAUTHORIZED,
    RETRYABLE_STATUS_CODES,
)
from ._errors import HTTPTransportError, StopIngestError
from ._labels import build_labels, is_wire_safe
from ._payload import (
    COUNTER,
    FAILURE_SAMPLE,
    SUCCESS,
    SUCCESS_SAMPLE,
    TOP,
    TOTAL,
    UNIQUE,
    VALUE,
    VALUE_SPARSE,
    Event,
    Payload,
    definition_wire_size,
    event_wire_size,
)
from ._ring_buffer import RingBuffer
from ._series import series_key
from ._top_item import valid_top_item
from ._unique_id import canonical_unique_id
from ._value_rate_limiter import ValueRateLimiter
from ._version import version

# Drop reasons, grouped by the statistics counter each one feeds.
_REASON_QUEUE_FULL = "queue_full"
_REASON_RATE_LIMIT = "value_rate_limit"
_REASON_INVALID_EVENT = "invalid_event"
_REASON_INVALID_UNIQUE_ID = "invalid_unique_id"
_REASON_INVALID_TOP_ITEM = "invalid_top_item"
_REASON_INVALID_LABELS = "invalid_labels"
_REASON_INVALID_METRIC = "invalid_metric"
_REASON_INVALID_VALUE = "invalid_value"
_REASON_BUFFER_FULL = "outage_buffer_full"
_REASON_BUFFER_EXPIRED = "outage_buffer_expired"
_REASON_BUFFER_TOO_LARGE = "outage_buffer_batch_too_large"
_REASON_SEND_FAILED = "send_failed"
_REASON_TOTAL_SERIES_LIMIT = "total_series_limit"
_REASON_TOTAL_DELTA_TOO_LARGE = "total_delta_too_large"

_INVALID_REASONS = frozenset(
    {
        _REASON_INVALID_EVENT,
        _REASON_INVALID_UNIQUE_ID,
        _REASON_INVALID_TOP_ITEM,
        _REASON_INVALID_LABELS,
        _REASON_INVALID_METRIC,
        _REASON_INVALID_VALUE,
        _REASON_TOTAL_SERIES_LIMIT,
        _REASON_TOTAL_DELTA_TOO_LARGE,
    }
)


class Stats:
    """A snapshot of client-side delivery health."""

    __slots__ = (
        "buffered_batches",
        "buffered_bytes",
        "buffered_events",
        "ingest_disabled",
        "invalid_dropped",
        "queue_depth",
        "queue_dropped",
        "rate_limited_dropped",
        "retry_dropped",
        "send_dropped",
    )

    def __init__(
        self,
        queue_dropped: int,
        invalid_dropped: int,
        rate_limited_dropped: int,
        retry_dropped: int,
        send_dropped: int,
        queue_depth: int,
        buffered_events: int,
        buffered_bytes: int,
        buffered_batches: int,
        ingest_disabled: bool,
    ) -> None:
        self.queue_dropped = queue_dropped
        self.invalid_dropped = invalid_dropped
        self.rate_limited_dropped = rate_limited_dropped
        self.retry_dropped = retry_dropped
        self.send_dropped = send_dropped
        self.queue_depth = queue_depth
        self.buffered_events = buffered_events
        self.buffered_bytes = buffered_bytes
        self.buffered_batches = buffered_batches
        self.ingest_disabled = ingest_disabled

    def __repr__(self) -> str:
        fields = ", ".join(f"{name}={getattr(self, name)!r}" for name in self.__slots__)
        return f"Stats({fields})"


class _RetryBatch:
    __slots__ = ("attempts", "buffered_at_ms", "estimated_bytes", "event_count", "next_attempt_ms", "payload")

    def __init__(self, payload: Payload, attempts: int, next_attempt_ms: int, buffered_at_ms: int) -> None:
        self.payload = payload
        self.attempts = attempts
        self.next_attempt_ms = next_attempt_ms
        self.buffered_at_ms = buffered_at_ms
        self.event_count = payload.event_count()
        self.estimated_bytes = payload.estimated_bytes()


class Client:
    """Records metrics and delivers them in the background.

    Metric methods never raise and never block on I/O. Construction does raise,
    because a missing key or an invalid workload is a caller mistake that should
    surface immediately rather than as silently absent metrics.
    """

    def __init__(
        self,
        workload: str,
        *,
        api_key: Optional[str] = None,
        endpoint: Optional[str] = None,
        transport: Optional[Transport] = None,
        logger: Optional[logging.Logger] = None,
        verbose: bool = False,
        silent: bool = False,
        headers: Optional[Dict[str, str]] = None,
    ) -> None:
        self._config: Config = build_config(
            workload,
            api_key=api_key,
            endpoint=endpoint,
            transport=transport,
            logger=logger,
            verbose=verbose,
            silent=silent,
            headers=headers,
        )
        self.workload = self._config.workload
        self._logger = self._config.logger
        self._verbose = self._config.verbose

        # Shared between application threads and the worker.
        self._lock = threading.Lock()
        self._queue: RingBuffer[Event] = RingBuffer(DEFAULT_QUEUE_SIZE)
        self._value_rate_limiter = ValueRateLimiter()
        self._drop_counts: Dict[str, int] = {}
        self._drop_log_at: Dict[str, int] = {}
        self._queue_dropped = 0
        self._invalid_dropped = 0
        self._rate_limited_dropped = 0
        self._retry_dropped = 0
        self._send_dropped = 0
        self._buffered_events = 0
        self._buffered_bytes = 0
        self._buffered_batches = 0

        # Owned by the worker thread only.
        self._retry_queue: List[_RetryBatch] = []
        self._retry_events = 0
        self._retry_bytes = 0
        self._totals: Dict[str, float] = {}
        self._transient_backoff_attempt = 0
        self._next_send_attempt_ms = 0
        self._next_replay_attempt_ms = 0
        self._recovery_confirmed = False
        self._batch_seq = 0

        self._closing = False
        self._closed = False
        self._stop_sending = False
        self._close_deadline_ms = 0
        self._auth_grace_until_ms = monotonic_ms() + DEFAULT_AUTH_GRACE_WINDOW_MS
        self._auth_grace_logged = False
        self._auth_refusal_pending = False
        self._auth_refusal_reported = False
        self._batch_session = _session_prefix()

        self._wake = threading.Event()
        self._worker: Optional[threading.Thread] = None
        self._start_worker()
        _fork.register(self)

        if self._verbose:
            self._logger.debug("prostometrics: client version %s workload=%s", version(), self.workload)

    # ---------------------------------------------------------------- recording

    def count(self, metric: str, delta: float = 1, *labels: str, **keyword_labels: object) -> None:
        """Increase a counter by ``delta``."""
        self._enqueue(COUNTER, metric, delta, labels, keyword_labels)

    def count_unique(self, unique_id: object, metric: str, *labels: str, **keyword_labels: object) -> None:
        """Count distinct identifiers approximately."""
        encoded = canonical_unique_id(unique_id)
        if encoded is None:
            self._record_drop(_REASON_INVALID_UNIQUE_ID, metric)
            return
        self._enqueue(UNIQUE, metric, 0.0, labels, keyword_labels, unique_id=encoded)

    def count_top(self, unique_id: object, metric: str, item: str) -> None:
        """Record that the person with unique_id touched item, for a top list ranking items by distinct people."""
        encoded = canonical_unique_id(unique_id)
        if encoded is None:
            self._record_drop(_REASON_INVALID_UNIQUE_ID, metric)
            return
        # Refused rather than repaired: the item is stored as sent, so a
        # trimmed or sanitized one would rank something the caller never wrote.
        if not valid_top_item(item):
            self._record_drop(_REASON_INVALID_TOP_ITEM, metric)
            return
        self._enqueue(TOP, metric, 0.0, (), {}, unique_id=encoded, item=item)

    def total(self, metric: str, total: float, *labels: str, **keyword_labels: object) -> None:
        """Record the current reading of a monotonically increasing total."""
        self._enqueue(TOTAL, metric, total, labels, keyword_labels)

    def value(self, metric: str, value: float, *labels: str, **keyword_labels: object) -> None:
        """Record a numeric observation."""
        if not self._allow_value(metric):
            return
        self._enqueue(VALUE, metric, value, labels, keyword_labels)

    def value_sparse(self, metric: str, value: float, *labels: str, **keyword_labels: object) -> None:
        """Record a value whose last observation carries across empty buckets."""
        if not self._allow_value(metric):
            return
        self._enqueue(VALUE_SPARSE, metric, value, labels, keyword_labels)

    def success(self, metric: str, ok: bool, *labels: str, **keyword_labels: object) -> None:
        """Record whether an operation succeeded.

        The sample is 100 for success and 0 for failure, so the metric's
        average is the success rate and the service shows it as a percentage.
        """
        if not self._allow_value(metric):
            return
        self._enqueue(SUCCESS, metric, SUCCESS_SAMPLE if ok else FAILURE_SAMPLE, labels, keyword_labels)

    # ------------------------------------------------------------------- status

    def stats(self) -> Stats:
        """Return a snapshot of queue depth, drops and buffered data."""
        with self._lock:
            return Stats(
                queue_dropped=self._queue_dropped,
                invalid_dropped=self._invalid_dropped,
                rate_limited_dropped=self._rate_limited_dropped,
                retry_dropped=self._retry_dropped,
                send_dropped=self._send_dropped,
                queue_depth=len(self._queue),
                buffered_events=self._buffered_events,
                buffered_bytes=self._buffered_bytes,
                buffered_batches=self._buffered_batches,
                ingest_disabled=self._stop_sending,
            )

    @property
    def closed(self) -> bool:
        return self._closed

    # ------------------------------------------------------------------ closing

    def close(self, timeout: float = 5.0) -> None:
        """Stop accepting metrics and deliver what is already queued.

        Blocks until the queue and the outage buffer are drained or ``timeout``
        elapses, whichever comes first. Anything still buffered when the timeout
        expires is lost, which is the documented trade: shutdown must be bounded.
        """
        if self._closed:
            return
        try:
            self._close(timeout)
        finally:
            self._report_pending_auth_refusal()

    def _close(self, timeout: float) -> None:
        self._close_deadline_ms = monotonic_ms() + int(max(0.0, timeout) * 1000)
        self._closing = True
        self._wake.set()
        worker = self._worker
        drained = True
        if worker is not None and worker is not threading.current_thread():
            worker.join(timeout=max(0.0, timeout) + 1.0)
            drained = not worker.is_alive()
        self._closed = True
        if not drained:
            # The worker is still inside a request on this transport. Closing it
            # from here would tear the connection out from under that thread and
            # corrupt http.client's per-connection state, so the socket is left
            # to the daemon thread and the interpreter.
            self._logger.warning(
                "prostometrics: close timed out after %.1fs with delivery still in progress; "
                "buffered metrics may be lost",
                timeout,
            )
            return
        # A transport without close() is legitimate, and a failure closing one
        # must not turn an orderly shutdown into an exception.
        with contextlib.suppress(Exception):
            self._config.transport.close()  # type: ignore[attr-defined]

    def _report_pending_auth_refusal(self) -> None:
        """Say out loud that the key was never accepted.

        A process that ends before the startup grace does — the five-line script
        someone writes to check the key works — would otherwise exit on the
        reassuring "retrying" line, having delivered nothing and raised nothing.
        """
        if not self._auth_refusal_pending or self._auth_refusal_reported:
            return
        self._auth_refusal_reported = True
        stats = self.stats()
        self._logger.warning(
            "prostometrics: shutting down while the API key was still refused; undelivered "
            "events: %d; check that the key is correct and active%s",
            stats.buffered_events + stats.queue_depth,
            refusal_hint(self._config.api_key),
        )

    def __enter__(self) -> Client:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    # ------------------------------------------------------------- hot path

    def _allow_value(self, metric: str) -> bool:
        """Apply the value sample cap before the caller's labels are built."""
        if not metric or self._closed or self._closing or self._stop_sending:
            return False
        with self._lock:
            allowed = self._value_rate_limiter.allow(monotonic_ms())
        if not allowed:
            self._record_drop(_REASON_RATE_LIMIT, metric)
            return False
        return True

    def _enqueue(
        self,
        kind: str,
        metric: str,
        value: float,
        labels: Tuple[str, ...],
        keyword_labels: Dict[str, object],
        unique_id: Optional[str] = None,
        item: Optional[str] = None,
    ) -> None:
        try:
            if self._closed or self._closing or self._stop_sending:
                return

            name = metric.strip() if metric else ""
            if not name or len(name) > MAX_METRIC_BYTES or not is_wire_safe(name):
                self._record_drop(_REASON_INVALID_METRIC, metric)
                return
            # Only a non-ASCII name can exceed the byte limit while passing the
            # character check above, so the encode stays off the common path.
            if not name.isascii() and len(name.encode("utf-8")) > MAX_METRIC_BYTES:
                self._record_drop(_REASON_INVALID_METRIC, metric)
                return

            if kind != UNIQUE and kind != TOP and not _valid_sample(kind, value):
                self._record_drop(_REASON_INVALID_VALUE, name)
                return

            if labels or keyword_labels:
                normalized = build_labels(labels, keyword_labels)
                if normalized is None:
                    self._record_drop(_REASON_INVALID_LABELS, name)
                    return
            else:
                normalized = []

            event = Event(
                type=kind,
                metric=name,
                value=float(value),
                labels=normalized,
                timestamp=wall_seconds(),
                unique_id=unique_id,
                item=item,
            )

            with self._lock:
                if not self._queue.push(event):
                    self._queue_dropped += 1
                    total = self._queue_dropped
                    should_log = self._should_log_drop_locked(_REASON_QUEUE_FULL)
                    depth = -1
                else:
                    total = 0
                    should_log = False
                    depth = len(self._queue)

            if should_log:
                self._logger.warning(
                    "prostometrics: local metric drop reason=%s metric=%s total=%d",
                    _REASON_QUEUE_FULL,
                    name,
                    total,
                )
            if depth >= DEFAULT_MAX_BATCH_SIZE:
                self._wake.set()
        except Exception:
            # A metric call must never raise into application code.
            self._record_drop(_REASON_INVALID_EVENT, metric)

    def _record_drop(self, reason: str, metric: str) -> None:
        with self._lock:
            if reason == _REASON_RATE_LIMIT:
                self._rate_limited_dropped += 1
                total = self._rate_limited_dropped
            elif reason in _INVALID_REASONS:
                self._invalid_dropped += 1
                total = self._invalid_dropped
            elif reason == _REASON_QUEUE_FULL:
                self._queue_dropped += 1
                total = self._queue_dropped
            else:
                self._retry_dropped += 1
                total = self._retry_dropped
            should_log = self._should_log_drop_locked(reason)
        if should_log:
            self._logger.warning(
                "prostometrics: local metric drop reason=%s metric=%s total=%d",
                reason,
                str(metric).strip(),
                total,
            )

    def _should_log_drop_locked(self, reason: str) -> bool:
        """Rate-limit drop warnings to one per reason per minute.

        Under sustained loss the interesting fact is that loss is happening and
        its running total, not one line per dropped event.
        """
        self._drop_counts[reason] = self._drop_counts.get(reason, 0) + 1
        now = monotonic_ms()
        last = self._drop_log_at.get(reason, 0)
        if last != 0 and now - last < LOCAL_DROP_LOG_INTERVAL_MS:
            return False
        self._drop_log_at[reason] = now
        return True

    # -------------------------------------------------------------- worker loop

    def _start_worker(self) -> None:
        self._wake = threading.Event()
        worker = threading.Thread(
            target=self._worker_loop,
            name=f"prostometrics-{self.workload}",
            daemon=True,
        )
        self._worker = worker
        worker.start()

    def _worker_loop(self) -> None:
        interval = DEFAULT_FLUSH_INTERVAL_MS / 1000.0
        try:
            # close() sets the wake event, so a close racing the first wait is
            # observed on the next loop rather than after a full interval.
            while True:
                self._wake.wait(interval)
                self._wake.clear()
                if self._closing:
                    break
                self._tick()
            self._drain_on_close()
        except BaseException:  # pragma: no cover - defensive
            self._logger.exception("prostometrics: worker thread stopped unexpectedly")

    def _tick(self) -> None:
        try:
            self._flush_one_batch()
            self._flush_retry_queue()
        except Exception:  # pragma: no cover - defensive
            self._logger.exception("prostometrics: flush cycle failed")

    def _take_events(self) -> List[Event]:
        # The lock is the one every caller takes to record a metric, so it is
        # held for the shift alone: measuring bytes and building series keys for
        # thousands of events under it would stall application threads for
        # milliseconds at every flush, in a client whose contract is that a
        # metric call does not block.
        with self._lock:
            taken = [
                event for event in (self._queue.shift() for _ in range(DEFAULT_MAX_BATCH_SIZE)) if event is not None
            ]

        # A batch is full on whichever of three ceilings it reaches first,
        # because the endpoint refuses a batch whole on any of them. Events
        # alone bound neither the bytes -- a few thousand long top-list items
        # are megabytes -- nor the series definitions, which a first flush
        # carries one of per series it touches, and which travel in the same
        # body.
        events: List[Event] = []
        batch_bytes = 0
        batch_series: Set[str] = set()
        for index, event in enumerate(taken):
            if batch_bytes >= DEFAULT_MAX_BATCH_BYTES or len(batch_series) >= DEFAULT_MAX_BATCH_SERIES:
                # Whatever did not fit goes back at the front of the queue, in
                # order, for the next flush -- dropping it here would lose
                # events the caller successfully recorded.
                with self._lock:
                    self._queue.unshift_all(taken[index:])
                break
            events.append(event)
            batch_bytes += event_wire_size(event)
            key = series_key(event.metric, event.labels)
            if key not in batch_series:
                batch_series.add(key)
                batch_bytes += definition_wire_size(event.metric, event.labels)
        return events

    def _flush_one_batch(self, ignore_client_backoff: bool = False) -> bool:
        """Send at most one batch drawn from the queue.

        Returns ``True`` when a batch was accepted, so the close-time drain can
        stop early rather than spinning against an endpoint that is refusing.
        """
        events = self._take_events()
        if not events:
            return False
        payload = self._build_payload(events)
        if payload is None:
            return False
        return self._send_payload(payload, attempt=1, ignore_client_backoff=ignore_client_backoff)

    def _build_payload(self, events: List[Event]) -> Optional[Payload]:
        builder = BatchBuilder()
        for event in events:
            if event.type == TOTAL:
                converted = self._apply_total(event)
                if converted is None:
                    continue
                builder.add(converted)
                continue
            builder.add(event)
        payload = builder.build()
        if payload is None:
            return None
        payload.batch_id = self._next_batch_id()
        return payload

    def _apply_total(self, event: Event) -> Optional[Event]:
        """Convert a monotonic total reading into a counter delta.

        The first reading of a series only establishes the baseline. A reading
        below the previous one means the source restarted its counter, so the
        baseline is reset instead of emitting a negative delta.

        The protocol's counter ceiling applies to the delta that goes on the
        wire, not to the caller's reading. A cumulative total legitimately grows
        past that ceiling — bytes sent by a long-running process passes it in a
        few terabytes — and rejecting the reading would silence the metric
        permanently.
        """
        key = series_key(event.metric, event.labels)
        previous = self._totals.get(key)
        if previous is None:
            if len(self._totals) >= DEFAULT_MAX_TOTAL_SERIES:
                self._record_drop(_REASON_TOTAL_SERIES_LIMIT, event.metric)
                return None
            self._totals[key] = event.value
            return None
        if event.value < previous:
            self._totals[key] = event.value
            return None
        delta = event.value - previous
        self._totals[key] = event.value
        if delta <= 0:
            return None
        if delta > MAX_COUNTER_VALUE:
            self._record_drop(_REASON_TOTAL_DELTA_TOO_LARGE, event.metric)
            return None
        return Event(
            type=COUNTER,
            metric=event.metric,
            value=delta,
            labels=event.labels,
            timestamp=event.timestamp,
            unique_id=None,
        )

    def _next_batch_id(self) -> str:
        self._batch_seq += 1
        return f"{self._batch_session}-{self._batch_seq:x}"

    # ------------------------------------------------------------------ sending

    def _send_payload(
        self,
        payload: Payload,
        attempt: int,
        from_retry: bool = False,
        buffered_at_ms: int = 0,
        ignore_client_backoff: bool = False,
    ) -> bool:
        if payload.is_empty() or self._stop_sending:
            return False
        if not payload.batch_id:
            payload.batch_id = self._next_batch_id()
        if self._verbose and not from_retry:
            self._logger.debug("prostometrics: flushing %d events", payload.event_count())
        if not ignore_client_backoff and self._defer_for_client_backoff(payload, attempt, buffered_at_ms):
            return False

        try:
            self._config.transport.send(payload, self.workload, DEFAULT_FLUSH_TIMEOUT_MS / 1000.0)
        except StopIngestError as err:
            if self._defer_for_auth_grace(payload, attempt, err, buffered_at_ms):
                return False
            self._stop_sending = True
            if _is_auth_stop(err):
                self._logger.error(
                    "prostometrics: ingest disabled: the API key was not accepted; check that it is "
                    "correct and active, then restart: %s%s",
                    err,
                    refusal_hint(self._config.api_key),
                )
            else:
                self._logger.error("prostometrics: ingest disabled after non-retryable response: %s", err)
            return False
        except HTTPTransportError as err:
            if _is_retryable(err):
                self._note_transient_failure(err)
                self._enqueue_retry(payload, attempt, err, buffered_at_ms)
                return False
            self._record_send_loss(payload, _REASON_SEND_FAILED)
            self._logger.warning("prostometrics: flush failed: %s; %s", err, self._failure_details(payload))
            return False
        except Exception as err:  # pragma: no cover - defensive
            self._record_send_loss(payload, _REASON_SEND_FAILED)
            self._logger.warning("prostometrics: flush failed: %s; %s", err, self._failure_details(payload))
            return False

        # A batch got through, so whatever the service refused earlier has
        # resolved and shutdown has nothing left to warn about.
        self._auth_refusal_pending = False
        self._reset_transient_backoff()
        return True

    def _defer_for_client_backoff(self, payload: Payload, attempt: int, buffered_at_ms: int) -> bool:
        """Hold a batch while a client-wide transient backoff is still running."""
        if self._next_send_attempt_ms == 0 or monotonic_ms() >= self._next_send_attempt_ms:
            return False
        self._enqueue_retry_at(payload, max(0, attempt - 1), self._next_send_attempt_ms, buffered_at_ms)
        return True

    def _defer_for_auth_grace(self, payload: Payload, attempt: int, err: BaseException, buffered_at_ms: int) -> bool:
        """Retry a refused key briefly right after start, instead of giving up.

        A key created moments before the process started may still be
        propagating, and that is exactly the flow onboarding teaches. Outside
        the window a 401 stays terminal, because by then it means the key is
        wrong or revoked rather than new.
        """
        if monotonic_ms() >= self._auth_grace_until_ms or not _is_auth_stop(err):
            return False
        # Waiting helps only a key that may still be propagating. A client key is
        # refused by construction, so holding the batch would trade an immediate,
        # accurate error for half a minute of misleading reassurance.
        if looks_like_client_key(self._config.api_key):
            return False
        self._auth_refusal_pending = True
        delay = max(DEFAULT_AUTH_GRACE_RETRY_INTERVAL_MS, _retry_after_ms(err))
        next_attempt = monotonic_ms() + delay
        # Gating live batches on the same instant as the queued retry keeps the
        # whole client to one authentication attempt per interval while the key
        # settles, rather than one attempt per flush.
        self._next_send_attempt_ms = next_attempt
        if not self._auth_grace_logged:
            self._auth_grace_logged = True
            self._logger.warning(
                "prostometrics: API key not accepted yet, retrying for up to %ds; a key created "
                "moments ago takes a few seconds to become usable",
                DEFAULT_AUTH_GRACE_WINDOW_MS // 1000,
            )
        self._enqueue_retry_at(payload, attempt, next_attempt, buffered_at_ms)
        return True

    def _note_transient_failure(self, err: BaseException) -> None:
        self._transient_backoff_attempt += 1
        delay = max(_client_backoff_delay(self._transient_backoff_attempt), _retry_after_ms(err))
        self._next_send_attempt_ms = monotonic_ms() + delay

    def _reset_transient_backoff(self) -> None:
        self._transient_backoff_attempt = 0
        self._next_send_attempt_ms = 0

    def _record_send_loss(self, payload: Payload, reason: str) -> None:
        """Account for a batch that failed permanently and will not be retried."""
        events = payload.event_count()
        with self._lock:
            self._send_dropped += events
            total = self._send_dropped
            should_log = self._should_log_drop_locked(reason)
        if should_log:
            self._logger.warning(
                "prostometrics: batch discarded reason=%s events=%d total=%d",
                reason,
                events,
                total,
            )

    def _failure_details(self, payload: Payload) -> str:
        with self._lock:
            depth = len(self._queue)
            send_dropped = self._send_dropped
        return (
            f"batch_id={payload.batch_id} events={payload.event_count()} "
            f"counters={len(payload.counters)} values={len(payload.values)} "
            f"uniques={len(payload.uniques)} tops={len(payload.tops)} queue_depth={depth} send_dropped={send_dropped}"
        )

    # ------------------------------------------------------------ outage buffer

    def _enqueue_retry(self, payload: Payload, attempt: int, err: BaseException, buffered_at_ms: int) -> None:
        delay = max(_retry_delay(attempt), _retry_after_ms(err))
        self._enqueue_retry_at(payload, attempt, monotonic_ms() + delay, buffered_at_ms)

    def _enqueue_retry_at(self, payload: Payload, attempts: int, next_attempt_ms: int, buffered_at_ms: int) -> None:
        """Buffer a batch for replay, evicting oldest-first when full.

        The buffer is bounded three ways — age, event count and estimated bytes —
        because a producer's shape decides which limit binds first, and the
        client must stay within a predictable memory ceiling under all of them.
        """
        now = monotonic_ms()
        candidate = _RetryBatch(
            payload=payload,
            attempts=attempts,
            next_attempt_ms=next_attempt_ms,
            buffered_at_ms=buffered_at_ms if buffered_at_ms > 0 else now,
        )
        if (
            candidate.event_count > DEFAULT_OUTAGE_BUFFER_MAX_EVENTS
            or candidate.estimated_bytes > DEFAULT_OUTAGE_BUFFER_MAX_BYTES
        ):
            self._drop_retry_batch(candidate, _REASON_BUFFER_TOO_LARGE)
            return

        if not self._recovery_confirmed:
            # The age limit applies only until a replay proves recovery started;
            # after that the snapshot is allowed to age while it drains.
            self._prune_expired(now - DEFAULT_OUTAGE_BUFFER_MAX_AGE_MS)
            if candidate.buffered_at_ms < now - DEFAULT_OUTAGE_BUFFER_MAX_AGE_MS:
                self._drop_retry_batch(candidate, _REASON_BUFFER_EXPIRED)
                return

        while self._would_overflow(candidate):
            if not self._retry_queue:
                self._drop_retry_batch(candidate, _REASON_BUFFER_FULL)
                return
            oldest = min(range(len(self._retry_queue)), key=lambda i: self._retry_queue[i].buffered_at_ms)
            self._drop_retry_batch(self._retry_take(oldest), _REASON_BUFFER_FULL)

        self._retry_append(candidate)
        if self._verbose:
            self._logger.debug(
                "prostometrics: queued retry batch_id=%s attempt=%d delay_ms=%d",
                payload.batch_id,
                attempts + 1,
                max(0, next_attempt_ms - now),
            )

    def _retry_append(self, item: _RetryBatch) -> None:
        """Buffer one batch, keeping the running usage totals in step.

        The totals are maintained incrementally rather than recomputed. Eviction
        runs one batch at a time, so summing the whole queue per check made
        filling a 4096-batch buffer quadratic in exactly the situation — a
        sustained outage — where the worker should be doing least work.
        """
        self._retry_queue.append(item)
        self._retry_events += item.event_count
        self._retry_bytes += item.estimated_bytes
        self._sync_buffer_stats()

    def _retry_take(self, index: int) -> _RetryBatch:
        """Remove one buffered batch and return it."""
        item = self._retry_queue.pop(index)
        self._retry_events -= item.event_count
        self._retry_bytes -= item.estimated_bytes
        self._sync_buffer_stats()
        return item

    def _would_overflow(self, candidate: _RetryBatch) -> bool:
        if len(self._retry_queue) >= DEFAULT_RETRY_QUEUE_SIZE:
            return True
        return (
            self._retry_events + candidate.event_count > DEFAULT_OUTAGE_BUFFER_MAX_EVENTS
            or self._retry_bytes + candidate.estimated_bytes > DEFAULT_OUTAGE_BUFFER_MAX_BYTES
        )

    def _prune_expired(self, cutoff_ms: int) -> None:
        if not self._retry_queue:
            return
        kept: List[_RetryBatch] = []
        for item in self._retry_queue:
            if item.buffered_at_ms < cutoff_ms:
                self._retry_events -= item.event_count
                self._retry_bytes -= item.estimated_bytes
                self._drop_retry_batch(item, _REASON_BUFFER_EXPIRED)
                continue
            kept.append(item)
        self._retry_queue = kept
        self._sync_buffer_stats()

    def _drop_retry_batch(self, item: _RetryBatch, reason: str) -> None:
        with self._lock:
            self._retry_dropped += item.event_count
            total = self._retry_dropped
            should_log = self._should_log_drop_locked(reason)
        if should_log:
            self._logger.warning(
                "prostometrics: buffered metrics dropped reason=%s events=%d total=%d",
                reason,
                item.event_count,
                total,
            )

    def _sync_buffer_stats(self) -> None:
        """Publish the worker's buffer totals where stats() can read them."""
        events = self._retry_events
        size = self._retry_bytes
        batches = len(self._retry_queue)
        with self._lock:
            self._buffered_events = events
            self._buffered_bytes = size
            self._buffered_batches = batches

    def _flush_retry_queue(self) -> None:
        """Replay buffered batches, oldest first, at the documented replay rate."""
        if not self._retry_queue or self._stop_sending:
            return
        now = monotonic_ms()
        if not self._recovery_confirmed:
            self._prune_expired(now - DEFAULT_OUTAGE_BUFFER_MAX_AGE_MS)
            if not self._retry_queue:
                return
        # A batch popped while either gate is closed would be re-buffered
        # untouched by _send_payload, so checking both here avoids the churn.
        if self._next_replay_attempt_ms > now or self._next_send_attempt_ms > now:
            return

        processed = 0
        limit = min(len(self._retry_queue), DEFAULT_RETRY_FLUSH_MAX_SENDS)
        while self._retry_queue and processed < limit:
            index = _oldest_due_index(self._retry_queue, now)
            if index < 0:
                break
            item = self._retry_take(index)
            if self._send_payload(item.payload, item.attempts + 1, from_retry=True, buffered_at_ms=item.buffered_at_ms):
                self._recovery_confirmed = True
            processed += 1

        if processed > 0:
            self._next_replay_attempt_ms = monotonic_ms() + DEFAULT_REPLAY_INTERVAL_MS
        if not self._retry_queue:
            self._recovery_confirmed = False

    # ---------------------------------------------------------------- shutdown

    def _deadline_passed(self) -> bool:
        return self._close_deadline_ms != 0 and monotonic_ms() >= self._close_deadline_ms

    def _drain_on_close(self) -> None:
        """Deliver what is queued, then replay the buffer, until the deadline."""
        while not self._deadline_passed() and not self._stop_sending:
            with self._lock:
                pending = len(self._queue)
            if pending == 0:
                break
            self._flush_one_batch(ignore_client_backoff=True)
        self._drain_retry_queue_on_close()

    def _drain_retry_queue_on_close(self) -> None:
        while self._retry_queue and not self._stop_sending and not self._deadline_passed():
            now = monotonic_ms()
            if not self._recovery_confirmed:
                self._prune_expired(now - DEFAULT_OUTAGE_BUFFER_MAX_AGE_MS)
                if not self._retry_queue:
                    break
            index = min(range(len(self._retry_queue)), key=lambda i: self._retry_queue[i].buffered_at_ms)
            item = self._retry_queue[index]
            due = max(item.next_attempt_ms, self._next_replay_attempt_ms)
            if not self._wait_until(due):
                return
            self._retry_take(index)
            if not self._send_payload(
                item.payload,
                item.attempts + 1,
                from_retry=True,
                buffered_at_ms=item.buffered_at_ms,
                ignore_client_backoff=True,
            ):
                return
            self._recovery_confirmed = True
            self._next_replay_attempt_ms = monotonic_ms() + DEFAULT_REPLAY_INTERVAL_MS
        if not self._retry_queue:
            self._recovery_confirmed = False

    def _wait_until(self, due_ms: int) -> bool:
        """Sleep until ``due_ms``, returning ``False`` if the deadline arrives first."""
        while True:
            now = monotonic_ms()
            if now >= due_ms:
                return True
            if self._deadline_passed():
                return False
            remaining = due_ms - now
            if self._close_deadline_ms:
                remaining = min(remaining, max(0, self._close_deadline_ms - now))
            if remaining <= 0:
                return False
            self._wake.wait(remaining / 1000.0)
            self._wake.clear()

    # --------------------------------------------------------------------- fork

    def _reinit_after_fork(self) -> None:
        """Rebuild per-process state in a freshly forked child.

        A forked child inherits no threads, a lock that may be held forever, a
        socket shared with its parent, and buffered events the parent will send
        itself. All four are replaced here; without this the child looks healthy
        and silently reports nothing.
        """
        self._lock = threading.Lock()
        if self._closed:
            # A client the application already closed stays closed. Restarting
            # its worker here would hand the child a live thread and an open
            # connection for something the caller deliberately shut down.
            return
        self._queue = RingBuffer(DEFAULT_QUEUE_SIZE)
        self._value_rate_limiter = ValueRateLimiter()
        self._retry_queue = []
        self._retry_events = 0
        self._retry_bytes = 0
        self._totals = {}
        self._buffered_events = 0
        self._buffered_bytes = 0
        self._buffered_batches = 0
        self._transient_backoff_attempt = 0
        self._next_send_attempt_ms = 0
        self._next_replay_attempt_ms = 0
        self._recovery_confirmed = False
        self._batch_seq = 0
        self._batch_session = _session_prefix()
        self._drop_counts = {}
        self._drop_log_at = {}
        self._closing = False
        self._close_deadline_ms = 0
        with contextlib.suppress(AttributeError):
            self._config.transport.reset_after_fork()
        self._start_worker()


def _session_prefix() -> str:
    """Return the per-process half of this client's batch ids.

    Batch idempotency is scoped by API key, workload and batch id, so two
    processes colliding here would have one process's batch answered from the
    other's cached outcome and its events silently discarded. The width matches
    the dictionary session id for the same reason.
    """
    return secrets.token_hex(8)


def _valid_sample(kind: str, value: float) -> bool:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return False
    if not math.isfinite(numeric) or numeric < 0:
        return False
    if kind == TOTAL:
        # A cumulative reading has no protocol ceiling of its own; the delta it
        # produces is what must fit, and _apply_total checks that.
        return True
    limit = MAX_SAMPLE_VALUE if kind in (VALUE, VALUE_SPARSE, SUCCESS) else MAX_COUNTER_VALUE
    return numeric <= limit


def _transport_error_of(err: BaseException) -> Optional[HTTPTransportError]:
    """Unwrap the transport error a stop response carries as its cause."""
    if isinstance(err, HTTPTransportError):
        return err
    if isinstance(err, StopIngestError) and isinstance(err.cause_error, HTTPTransportError):
        return err.cause_error
    return None


def _is_auth_stop(err: BaseException) -> bool:
    """Report whether a terminal response was an authentication failure.

    The body code decides when present, so a 401 carrying some other terminal
    code is not mistaken for a rejected key.
    """
    transport_error = _transport_error_of(err)
    if transport_error is not None:
        code = transport_error.response_code.strip().lower()
        if code:
            return code == RESPONSE_CODE_UNAUTHORIZED
        return transport_error.status_code == HTTP_STATUS_UNAUTHORIZED
    return isinstance(err, StopIngestError) and err.code == HTTP_STATUS_UNAUTHORIZED


def _is_retryable(err: HTTPTransportError) -> bool:
    if err.status_code == 0:
        # No status means the request never completed: a socket or TLS failure.
        return True
    return err.status_code in RETRYABLE_STATUS_CODES


def _retry_after_ms(err: BaseException) -> int:
    transport_error = _transport_error_of(err)
    return max(0, transport_error.retry_after_ms) if transport_error is not None else 0


def _retry_delay(attempt: int) -> int:
    delay = DEFAULT_RETRY_BASE_DELAY_MS
    for _ in range(1, max(1, attempt)):
        if delay >= DEFAULT_RETRY_MAX_DELAY_MS:
            break
        delay = min(delay * 2, DEFAULT_RETRY_MAX_DELAY_MS)
    return delay + _jitter(DEFAULT_RETRY_JITTER_WINDOW_MS)


def _client_backoff_delay(attempt: int) -> int:
    safe_attempt = max(1, attempt)
    delay = DEFAULT_RETRY_BASE_DELAY_MS
    for _ in range(1, safe_attempt):
        if delay >= DEFAULT_CLIENT_BACKOFF_MAX_DELAY_MS:
            break
        delay = min(delay * 2, DEFAULT_CLIENT_BACKOFF_MAX_DELAY_MS)
    # The first failure spreads recovery across a wide window so that many
    # clients recovering from one outage do not retry in lockstep.
    window = DEFAULT_RECOVERY_JITTER_WINDOW_MS if safe_attempt == 1 else DEFAULT_CLIENT_BACKOFF_JITTER_WINDOW_MS
    return min(delay + _jitter(window), DEFAULT_CLIENT_BACKOFF_MAX_DELAY_MS)


def _jitter(window_ms: int) -> int:
    return random.randrange(window_ms) if window_ms > 0 else 0


def _oldest_due_index(queue: List[_RetryBatch], now_ms: int) -> int:
    oldest = -1
    for index, item in enumerate(queue):
        if item.next_attempt_ms > now_ms:
            continue
        if oldest < 0 or item.buffered_at_ms < queue[oldest].buffered_at_ms:
            oldest = index
    return oldest
