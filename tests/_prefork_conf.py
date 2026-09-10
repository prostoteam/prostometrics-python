"""gunicorn configuration for the prefork integration test.

``post_fork`` runs in every worker, so the startup metric it records proves that
each forked child can deliver — independently of which worker the kernel happens
to hand the incoming requests to.
"""

from __future__ import annotations

import prostometrics


def post_fork(server, worker):
    prostometrics.count("prefork.worker_started", 1, worker=str(worker.pid))
