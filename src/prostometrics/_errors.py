"""Exception types raised by the client.

Only configuration errors surface to the caller. Delivery failures are handled
inside the worker thread and reported through the logger, because a metric call
must never raise into application code.
"""

from __future__ import annotations

from typing import Optional


class ProstometricsError(Exception):
    """Base class for every error raised by this package."""


class NoTransportError(ProstometricsError):
    def __init__(self) -> None:
        super().__init__("prostometrics: no transport configured")


class ClientClosedError(ProstometricsError):
    def __init__(self) -> None:
        super().__init__("prostometrics: client closed")


class InvalidWorkloadError(ProstometricsError):
    def __init__(self, detail: str = "") -> None:
        suffix = f": {detail}" if detail else ""
        super().__init__(f"prostometrics: invalid workload{suffix}")


class MissingAPIKeyError(ProstometricsError):
    def __init__(self) -> None:
        super().__init__(
            "prostometrics: API key is required; pass api_key= or set the PROSTOMETRICS_API_KEY environment variable"
        )


class APIKeyAuthorizationConflictError(ProstometricsError):
    def __init__(self) -> None:
        super().__init__("prostometrics: API key conflicts with custom Authorization header")


class StopIngestError(ProstometricsError):
    """Raised for a response that must disable ingest for the process lifetime."""

    def __init__(self, message: str, code: int = 0, cause: Optional[BaseException] = None) -> None:
        super().__init__(message)
        self.code = code
        self.cause_error = cause


class HTTPTransportError(ProstometricsError):
    """A failed ingest request, carrying everything needed to classify it."""

    def __init__(
        self,
        endpoint: str,
        *,
        method: str = "POST",
        batch_id: str = "",
        status_code: int = 0,
        status: str = "",
        response_code: str = "",
        detail: str = "",
        accepted: int = -1,
        dropped: int = -1,
        rejected: int = -1,
        retry_after_ms: int = 0,
        request_bytes: int = 0,
        dictionary_session: str = "",
        dictionary_revision: int = 0,
        dictionary_series: int = 0,
        series_definitions: int = 0,
        event_lines: int = 0,
    ) -> None:
        status_part = status if status else "failed"
        detail_part = f": {detail}" if detail else ""
        request_suffix = f" (request_bytes={request_bytes})" if request_bytes > 0 else ""
        super().__init__(f"{method} {endpoint}: {status_part}{detail_part}{request_suffix}")
        self.method = method
        self.endpoint = endpoint
        self.batch_id = batch_id
        self.status_code = status_code
        self.status = status
        self.response_code = response_code
        self.detail = detail
        self.accepted = accepted
        self.dropped = dropped
        self.rejected = rejected
        self.retry_after_ms = retry_after_ms
        self.request_bytes = request_bytes
        self.dictionary_session = dictionary_session
        self.dictionary_revision = dictionary_revision
        self.dictionary_series = dictionary_series
        self.series_definitions = series_definitions
        self.event_lines = event_lines


def is_stop_ingest_error(err: BaseException) -> bool:
    return isinstance(err, StopIngestError)
