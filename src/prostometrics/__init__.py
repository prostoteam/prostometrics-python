"""Prostometrics client for Python.

Typical use, with one client for the whole process::

    import prostometrics

    prostometrics.init("billing-api", api_key="...")
    prostometrics.count("requests", 1, method="GET")

Every metric call appends to an in-memory queue and returns; a background thread
batches and delivers. Calls never raise and never block on the network, so they
are safe inside request handlers and hot loops.
"""

from __future__ import annotations

import logging
import threading
from typing import Dict, Optional

from ._client import Client, Stats
from ._config import Config, Transport, endpoint_from_host, ensure_ingest_path
from ._errors import (
    APIKeyAuthorizationConflictError,
    ClientClosedError,
    HTTPTransportError,
    InvalidWorkloadError,
    MissingAPIKeyError,
    NoTransportError,
    ProstometricsError,
    StopIngestError,
    is_stop_ingest_error,
)
from ._labels import label
from ._transport import HTTPTransport
from ._version import __version__, version

__all__ = [
    "APIKeyAuthorizationConflictError",
    "Client",
    "ClientClosedError",
    "Config",
    "HTTPTransport",
    "HTTPTransportError",
    "InvalidWorkloadError",
    "MissingAPIKeyError",
    "NoTransportError",
    "ProstometricsError",
    "Stats",
    "StopIngestError",
    "Transport",
    "__version__",
    "count",
    "count_top",
    "count_unique",
    "default",
    "endpoint_from_host",
    "ensure_ingest_path",
    "init",
    "is_stop_ingest_error",
    "label",
    "success",
    "total",
    "value",
    "value_sparse",
    "version",
]

_default_lock = threading.Lock()
_default_client: Optional[Client] = None


def init(
    workload: str,
    *,
    api_key: Optional[str] = None,
    endpoint: Optional[str] = None,
    transport: Optional[Transport] = None,
    logger: Optional[logging.Logger] = None,
    verbose: bool = False,
    silent: bool = False,
    headers: Optional[Dict[str, str]] = None,
) -> Client:
    """Create the process-wide client used by the module-level metric functions.

    ``api_key`` falls back to the ``PROSTOMETRICS_API_KEY`` environment
    variable, which is how most Python applications already carry secrets.
    Calling ``init`` again replaces the previous client and closes it.
    """
    global _default_client
    client = Client(
        workload,
        api_key=api_key,
        endpoint=endpoint,
        transport=transport,
        logger=logger,
        verbose=verbose,
        silent=silent,
        headers=headers,
    )
    with _default_lock:
        previous = _default_client
        _default_client = client
    if previous is not None:
        previous.close(timeout=2.0)
    return client


def default() -> Optional[Client]:
    """Return the client created by :func:`init`, if there is one."""
    with _default_lock:
        return _default_client


def count(metric: str, delta: float = 1, *labels: str, **keyword_labels: object) -> None:
    """Increase a counter on the default client."""
    client = _default_client
    if client is not None:
        client.count(metric, delta, *labels, **keyword_labels)


def count_unique(unique_id: object, metric: str, *labels: str, **keyword_labels: object) -> None:
    """Count distinct identifiers on the default client."""
    client = _default_client
    if client is not None:
        client.count_unique(unique_id, metric, *labels, **keyword_labels)


def count_top(unique_id: object, metric: str, item: str) -> None:
    """Record that a person touched an item, for a top list, on the default client."""
    client = _default_client
    if client is not None:
        client.count_top(unique_id, metric, item)


def total(metric: str, current_total: float, *labels: str, **keyword_labels: object) -> None:
    """Record a monotonic total reading on the default client."""
    client = _default_client
    if client is not None:
        client.total(metric, current_total, *labels, **keyword_labels)


def value(metric: str, sample: float, *labels: str, **keyword_labels: object) -> None:
    """Record a numeric observation on the default client."""
    client = _default_client
    if client is not None:
        client.value(metric, sample, *labels, **keyword_labels)


def value_sparse(metric: str, sample: float, *labels: str, **keyword_labels: object) -> None:
    """Record a carrying value observation on the default client."""
    client = _default_client
    if client is not None:
        client.value_sparse(metric, sample, *labels, **keyword_labels)


def success(metric: str, ok: bool, *labels: str, **keyword_labels: object) -> None:
    """Record whether an operation succeeded, on the default client."""
    client = _default_client
    if client is not None:
        client.success(metric, ok, *labels, **keyword_labels)
