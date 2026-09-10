"""End-to-end check under a real preforking application server.

``tests/test_fork.py`` covers the fork mechanics directly. This test covers the
arrangement customers actually deploy: gunicorn imports the application in a
master process, forks workers, serves requests from them, and stops them with
SIGTERM. It is skipped when gunicorn is not installed.
"""

from __future__ import annotations

import os
import re
import signal
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

import pytest

pytest.importorskip("gunicorn", reason="gunicorn is an optional test dependency")

_WORKERS = 4
_REQUESTS = 8
_TESTS_DIR = str(Path(__file__).parent)

pytestmark = pytest.mark.skipif(not hasattr(os, "fork"), reason="fork is POSIX only")


def _read_startup(process: subprocess.Popen, workers: int, timeout: float = 30.0) -> int:
    """Return the bound port once every worker has reported that it booted."""
    deadline = time.monotonic() + timeout
    port_pattern = re.compile(r"Listening at: http://127\.0\.0\.1:(\d+)")
    port = 0
    booted = 0
    assert process.stderr is not None
    while time.monotonic() < deadline and (port == 0 or booted < workers):
        line = process.stderr.readline()
        if not line:
            if process.poll() is not None:
                raise AssertionError("gunicorn exited during startup")
            continue
        match = port_pattern.search(line)
        if match:
            port = int(match.group(1))
        if "Booting worker" in line:
            booted += 1
    assert port and booted == workers, f"gunicorn startup incomplete: port={port} workers={booted}"
    return port


def test_metrics_flow_from_every_preforked_worker(ingester):
    environment = {
        **os.environ,
        "PROSTOMETRICS_ENDPOINT": ingester.url,
        "PROSTOMETRICS_API_KEY": "12_secret",
        "PYTHONUNBUFFERED": "1",
    }
    command = [
        sys.executable,
        "-m",
        "gunicorn",
        "--preload",  # import in the master, then fork: the case fork handling exists for
        "--workers",
        str(_WORKERS),
        "--bind",
        "127.0.0.1:0",
        "--graceful-timeout",
        "10",
        "--pythonpath",
        _TESTS_DIR,
        "--config",
        str(Path(_TESTS_DIR) / "_prefork_conf.py"),
        "_prefork_app:app",
    ]
    process = subprocess.Popen(
        command,
        env=environment,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        port = _read_startup(process, _WORKERS)
        for _ in range(_REQUESTS):
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/", timeout=10) as response:
                assert response.status == 200

        # SIGTERM is how a real deployment stops workers, and it is what gives
        # each one its chance to flush before the process goes away.
        process.send_signal(signal.SIGTERM)
        process.wait(timeout=30)
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=10)
        if process.stderr is not None:
            process.stderr.close()

    events = ingester.decoded_events()
    started = [event for event in events if event.metric == "prefork.worker_started"]
    served = [event for event in events if event.metric == "prefork.requests"]

    assert len(started) == _WORKERS, f"every worker must deliver its startup metric, got {started}"
    assert len({event.session for event in started}) == _WORKERS, "each worker needs its own dictionary session"
    assert sum(int(event.value) for event in served) == _REQUESTS
    assert {event.labels[0] for event in started} == {f"worker={event.labels[0].split('=')[1]}" for event in started}
