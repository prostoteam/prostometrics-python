from __future__ import annotations

import logging

import pytest

from prostometrics._config import build_config, endpoint_from_host, ensure_ingest_path, resolve_logger
from prostometrics._constants import API_KEY_ENV_VAR, ENDPOINT_ENV_VAR
from prostometrics._errors import APIKeyAuthorizationConflictError, InvalidWorkloadError, MissingAPIKeyError


def test_endpoint_from_host_defaults_to_https_and_the_batch_path():
    assert endpoint_from_host("prostometrics.ru") == "https://prostometrics.ru/api/i/batch"
    assert endpoint_from_host("http://localhost:8085") == "http://localhost:8085/api/i/batch"
    assert endpoint_from_host("") == ""


def test_ensure_ingest_path_leaves_an_explicit_path_alone():
    assert ensure_ingest_path("https://example.com/custom") == "https://example.com/custom"
    assert ensure_ingest_path("https://example.com/") == "https://example.com/api/i/batch"


def test_api_key_falls_back_to_the_environment(monkeypatch):
    monkeypatch.setenv(API_KEY_ENV_VAR, "12_secret")
    config = build_config("api")
    assert config.api_key == "12_secret"


def test_explicit_api_key_wins_over_the_environment(monkeypatch):
    monkeypatch.setenv(API_KEY_ENV_VAR, "12_from_env")
    assert build_config("api", api_key="34_explicit").api_key == "34_explicit"


def test_endpoint_falls_back_to_the_environment(monkeypatch):
    monkeypatch.setenv(ENDPOINT_ENV_VAR, "http://localhost:8085")
    config = build_config("api", api_key="12_k")
    assert config.endpoint == "http://localhost:8085/api/i/batch"


def test_missing_api_key_is_reported_at_construction(monkeypatch):
    monkeypatch.delenv(API_KEY_ENV_VAR, raising=False)
    with pytest.raises(MissingAPIKeyError):
        build_config("api")


def test_api_key_and_authorization_header_together_are_rejected(monkeypatch):
    monkeypatch.delenv(API_KEY_ENV_VAR, raising=False)
    with pytest.raises(APIKeyAuthorizationConflictError):
        build_config("api", api_key="12_k", headers={"Authorization": "other"})


def test_authorization_header_alone_is_accepted(monkeypatch):
    monkeypatch.delenv(API_KEY_ENV_VAR, raising=False)
    config = build_config("api", headers={"Authorization": "12_k"})
    assert config.api_key == ""


def test_invalid_workload_is_reported_at_construction():
    with pytest.raises(InvalidWorkloadError):
        build_config("bad workload", api_key="12_k")


def test_silent_disables_the_logger():
    assert resolve_logger(None, silent=True).disabled is True
    assert resolve_logger(None, silent=False).name == "prostometrics"
    custom = logging.getLogger("custom")
    assert resolve_logger(custom, silent=False) is custom


def test_silent_logger_does_not_accumulate_handlers():
    """getLogger returns one shared object, so handlers must not pile up."""
    first = resolve_logger(None, silent=True)
    handlers = len(first.handlers)
    for _ in range(5):
        resolve_logger(None, silent=True)
    assert len(first.handlers) == handlers


def test_config_carries_the_trimmed_workload():
    assert build_config("  billing-api  ", api_key="12_k").workload == "billing-api"
