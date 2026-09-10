"""Process lifecycle handling: fork and interpreter exit.

Neither the Go nor the Node client needs this file. Python does, because Python
web applications are normally run by a prefork server — gunicorn, uWSGI, Celery
— which starts one process and copies it into several workers. A forked child
inherits the parent's client object but none of its threads, so without the
handling here a client that works in development goes permanently silent in
production while still looking healthy.

Clients are held weakly, so registering here never keeps one alive.
"""

from __future__ import annotations

import atexit
import contextlib
import os
import threading
import weakref
from typing import TYPE_CHECKING, Any, Set

if TYPE_CHECKING:  # pragma: no cover
    from ._client import Client

_lock = threading.Lock()
_clients: weakref.WeakSet[Any] = weakref.WeakSet()
_installed = False


def register(client: Client) -> None:
    """Track a client for fork recovery and for flushing at interpreter exit."""
    global _installed
    with _lock:
        _clients.add(client)
        if _installed:
            return
        _installed = True
    _install_handlers()


def _install_handlers() -> None:
    # A lock held by another thread at the moment of fork stays held forever in
    # the child. Acquiring around the fork and releasing on both sides is the
    # supported way to keep this registry usable in the child.
    if hasattr(os, "register_at_fork"):
        os.register_at_fork(
            before=_lock.acquire,
            after_in_parent=_release,
            after_in_child=_release,
        )
        os.register_at_fork(after_in_child=_reinit_children)
    atexit.register(_flush_at_exit)


def _release() -> None:
    with contextlib.suppress(RuntimeError):  # pragma: no cover - already released
        _lock.release()


def _reinit_children() -> None:
    for client in _snapshot():
        with contextlib.suppress(Exception):  # pragma: no cover - a broken client must not break the fork
            client._reinit_after_fork()


def _flush_at_exit() -> None:
    """Give each live client a bounded chance to deliver what it has queued.

    Short-lived scripts are a normal way to use this library, and they would
    otherwise exit before the first flush interval elapsed.
    """
    for client in _snapshot():
        with contextlib.suppress(Exception):  # pragma: no cover - shutdown must not raise
            client.close(timeout=2.0)


def _snapshot() -> Set[Any]:
    try:
        with _lock:
            return set(_clients)
    except Exception:  # pragma: no cover
        return set()
