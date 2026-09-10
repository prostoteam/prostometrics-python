"""Client configuration and its defaults.

Logging goes through the standard library ``logging`` module so that client
diagnostics land in whatever handlers the host application already configured —
Django, gunicorn, uvicorn and systemd all pick them up without extra wiring.
With no handlers configured at all, Python still prints warnings to stderr,
which matches the Go and Node clients.
"""

from __future__ import annotations

import logging
import os
from typing import Dict, Optional, Protocol, runtime_checkable
from urllib.parse import urlsplit, urlunsplit

from ._constants import API_KEY_ENV_VAR, DEFAULT_ENDPOINT_HOST, DEFAULT_INGEST_PATH, ENDPOINT_ENV_VAR
from ._errors import APIKeyAuthorizationConflictError, MissingAPIKeyError, NoTransportError
from ._payload import Payload
from ._workload import validate_workload

LOGGER_NAME = "prostometrics"


@runtime_checkable
class Transport(Protocol):
    """Anything able to deliver one encoded batch.

    Implement this to route batches somewhere other than the ingest endpoint —
    a test double, or a local relay.
    """

    def send(self, payload: Payload, workload: str, timeout: float) -> None: ...

    def reset_after_fork(self) -> None: ...


class Config:
    """Resolved settings for one client."""

    __slots__ = ("api_key", "endpoint", "headers", "logger", "transport", "verbose", "workload")

    def __init__(
        self,
        workload: str,
        endpoint: str,
        api_key: str,
        transport: Transport,
        logger: logging.Logger,
        verbose: bool,
        headers: Dict[str, str],
    ) -> None:
        self.workload = workload
        self.endpoint = endpoint
        self.api_key = api_key
        self.transport = transport
        self.logger = logger
        self.verbose = verbose
        self.headers = headers


def endpoint_from_host(host: str) -> str:
    """Turn a bare host into a full ingest endpoint URL."""
    trimmed = host.strip()
    if not trimmed:
        return ""
    if not trimmed.startswith(("http://", "https://")):
        trimmed = f"https://{trimmed}"
    return ensure_ingest_path(trimmed)


def ensure_ingest_path(endpoint: str) -> str:
    """Append the batch ingest path to an endpoint that carries no path."""
    if not endpoint:
        return ""
    parts = urlsplit(endpoint)
    if not parts.netloc:
        return endpoint
    if parts.path in ("", "/"):
        parts = parts._replace(path=DEFAULT_INGEST_PATH)
    return urlunsplit(parts)


def resolve_logger(logger: Optional[logging.Logger], silent: bool) -> logging.Logger:
    if silent:
        quiet = logging.getLogger(f"{LOGGER_NAME}.silent")
        # getLogger returns one shared object, so attaching a handler on every
        # construction would grow its handler list without bound.
        if not any(isinstance(handler, logging.NullHandler) for handler in quiet.handlers):
            quiet.addHandler(logging.NullHandler())
        quiet.propagate = False
        quiet.disabled = True
        return quiet
    return logger if logger is not None else logging.getLogger(LOGGER_NAME)


def build_config(
    workload: str,
    *,
    api_key: Optional[str] = None,
    endpoint: Optional[str] = None,
    transport: Optional[Transport] = None,
    logger: Optional[logging.Logger] = None,
    verbose: bool = False,
    silent: bool = False,
    headers: Optional[Dict[str, str]] = None,
) -> Config:
    """Validate inputs and fill in defaults, raising on anything unusable.

    Configuration problems raise here, at construction, rather than being
    swallowed like delivery failures: a missing key is a mistake the caller can
    still fix, and failing quietly would hide it until metrics never appeared.
    """
    workload = validate_workload(workload)

    resolved_headers = dict(headers or {})
    resolved_logger = resolve_logger(logger, silent)

    resolved_endpoint = endpoint if endpoint is not None else os.environ.get(ENDPOINT_ENV_VAR, "")
    resolved_endpoint = resolved_endpoint.strip()
    if not resolved_endpoint and transport is None:
        resolved_endpoint = endpoint_from_host(DEFAULT_ENDPOINT_HOST)
    elif resolved_endpoint:
        resolved_endpoint = ensure_ingest_path(resolved_endpoint)

    resolved_key = api_key if api_key is not None else os.environ.get(API_KEY_ENV_VAR, "")
    resolved_key = resolved_key.strip()

    if transport is None:
        if not resolved_endpoint:
            raise NoTransportError()
        if _has_authorization_header(resolved_headers):
            if resolved_key:
                raise APIKeyAuthorizationConflictError()
        elif not resolved_key:
            raise MissingAPIKeyError()

        from ._transport import HTTPTransport

        transport = HTTPTransport(
            endpoint=resolved_endpoint,
            api_key=resolved_key,
            headers=resolved_headers,
            logger=resolved_logger,
            verbose=verbose,
        )

    return Config(
        workload=workload,
        endpoint=resolved_endpoint,
        api_key=resolved_key,
        transport=transport,
        logger=resolved_logger,
        verbose=verbose,
        headers=resolved_headers,
    )


def _has_authorization_header(headers: Dict[str, str]) -> bool:
    return any(name.lower() == "authorization" and str(value).strip() for name, value in headers.items())
