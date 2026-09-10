"""A minimal WSGI application used by the gunicorn prefork integration test.

The client is created at import time, which under ``gunicorn --preload`` means
it is created in the master process and then inherited by every forked worker.
That is the arrangement the fork handling exists for.
"""

from __future__ import annotations

import os

import prostometrics

prostometrics.init("prefork-test")


def app(environ, start_response):
    prostometrics.count("prefork.requests", 1, worker=str(os.getpid()))
    body = b"ok"
    start_response("200 OK", [("Content-Type", "text/plain"), ("Content-Length", str(len(body)))])
    return [body]
