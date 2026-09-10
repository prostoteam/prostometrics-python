"""Behavior under prefork servers.

A forked child inherits the parent's client object but none of its threads. If
that is not repaired the child looks healthy and reports nothing, which is the
single most likely way this library fails in a real Python deployment.
"""

from __future__ import annotations

import os
import threading

import pytest

from prostometrics import Client

_POSIX_ONLY = pytest.mark.skipif(not hasattr(os, "fork"), reason="fork is POSIX only")


def _session_ids(ingester) -> set:
    return {request.header_line().split("|")[3] for request in ingester.requests}


@_POSIX_ONLY
def test_a_forked_child_delivers_its_own_metrics(ingester):
    client = Client("api", api_key="12_secret", endpoint=ingester.url)
    client.count("parent_before_fork", 1)

    pid = os.fork()
    if pid == 0:
        code = 0
        try:
            worker = client._worker
            if worker is None or not worker.is_alive():
                code = 2
            else:
                client.count("child_metric", 1)
                client.close(timeout=5.0)
        except BaseException:
            code = 1
        finally:
            os._exit(code)

    _, status = os.waitpid(pid, 0)
    assert os.waitstatus_to_exitcode(status) == 0, "the child must keep a live worker thread"
    client.close(timeout=5.0)

    assert ingester.request_count() == 2
    assert len(_session_ids(ingester)) == 2, "parent and child must not share a dictionary session"


@_POSIX_ONLY
def test_a_child_does_not_resend_metrics_the_parent_already_owns(ingester):
    client = Client("api", api_key="12_secret", endpoint=ingester.url)
    client.count("queued_before_fork", 1)

    pid = os.fork()
    if pid == 0:
        code = 0
        try:
            if client.stats().queue_depth != 0:
                code = 3
            client.close(timeout=5.0)
        except BaseException:
            code = 1
        finally:
            os._exit(code)

    _, status = os.waitpid(pid, 0)
    assert os.waitstatus_to_exitcode(status) == 0, "the child must start with an empty queue"
    client.close(timeout=5.0)

    events = ingester.all_event_lines()
    assert len(events) == 1, "the pre-fork event belongs to the parent alone"


@_POSIX_ONLY
def test_four_prefork_workers_each_report_under_their_own_session(ingester):
    """The shape gunicorn and uWSGI actually produce: one master, N children."""
    client = Client("api", api_key="12_secret", endpoint=ingester.url)

    children = []
    for index in range(4):
        pid = os.fork()
        if pid == 0:
            code = 0
            try:
                client.count("worker_requests", index + 1, worker=str(index))
                client.close(timeout=5.0)
            except BaseException:
                code = 1
            finally:
                os._exit(code)
        children.append(pid)

    for pid in children:
        _, status = os.waitpid(pid, 0)
        assert os.waitstatus_to_exitcode(status) == 0

    client.close(timeout=5.0)

    assert ingester.request_count() == 4
    assert len(_session_ids(ingester)) == 4, "each worker needs its own dictionary session"
    delivered = sorted(int(line.split("|")[2]) for line in ingester.all_event_lines())
    assert delivered == [1, 2, 3, 4]


def test_reinit_replaces_the_lock_and_restarts_the_worker(ingester):
    """The repair itself, asserted without forking so it runs everywhere."""
    client = Client("api", api_key="12_secret", endpoint=ingester.url)
    try:
        original_lock = client._lock
        original_worker = client._worker
        client.count("before", 1)

        # Simulate what the child inherits: a held lock and a dead worker.
        client._lock.acquire()
        client._reinit_after_fork()

        assert client._lock is not original_lock
        assert client._worker is not original_worker
        assert client._worker is not None and client._worker.is_alive()
        assert client.stats().queue_depth == 0
        assert client._batch_seq == 0

        client.count("after", 1)
        assert client.stats().queue_depth == 1
    finally:
        client.close(timeout=3.0)


def test_reinit_mints_a_new_dictionary_session(ingester):
    client = Client("api", api_key="12_secret", endpoint=ingester.url)
    try:
        client.count("before", 1)
        client._wake.set()
        assert ingester.wait_for_requests(1)
        first_session = ingester.requests[0].header_line().split("|")[3]

        client._reinit_after_fork()
        client.count("after", 1)
        client._wake.set()
        assert ingester.wait_for_requests(2)
        second_session = ingester.requests[1].header_line().split("|")[3]

        assert first_session != second_session
    finally:
        client.close(timeout=3.0)


def test_atexit_registration_does_not_keep_clients_alive(ingester):
    """Clients are held weakly, so the registry never leaks a closed client."""
    import gc
    import weakref

    from prostometrics import _fork

    client = Client("api", api_key="12_secret", endpoint=ingester.url)
    reference = weakref.ref(client)
    client.close(timeout=3.0)
    del client
    gc.collect()

    assert reference() is None
    assert all(item is not None for item in _fork._snapshot())


def test_worker_threads_do_not_accumulate(ingester):
    before = threading.active_count()
    for _ in range(5):
        Client("api", api_key="12_secret", endpoint=ingester.url).close(timeout=3.0)
    assert threading.active_count() <= before + 1


def test_fork_does_not_resurrect_a_closed_client(ingester):
    """A client the application shut down stays shut down in the child."""
    client = Client("api", api_key="12_secret", endpoint=ingester.url)
    client.close(timeout=3.0)

    client._reinit_after_fork()

    assert client.closed is True
    worker = client._worker
    assert worker is None or not worker.is_alive(), "a closed client must not get a new worker"

    before = ingester.request_count()
    client.count("after_close", 1)
    assert ingester.request_count() == before
