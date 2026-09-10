# Prostometrics Python Client

Python client for sending application metrics to Prostometrics.

## Install

```bash
pip install prostometrics
```

No runtime dependencies. Python 3.9 and newer.

## Quick start

```python
import os
import prostometrics

prostometrics.init("payments-api", api_key=os.environ["PROSTOMETRICS_API_KEY"])

prostometrics.count("requests", 1, method="GET", status="200")
prostometrics.count_unique(user_id, "users.dau", plan="pro")
prostometrics.count_top(user_id, "article.reads", article.slug)
prostometrics.total("bytes_sent_kb", 2048, interface="eth0")
prostometrics.value("latency_ms", 123.4, route="/login")
prostometrics.value_sparse("capacity_kb", 1024 * 1024, mount="/")
prostometrics.success("payment", error is None, provider="stripe")
```

`api_key` falls back to the `PROSTOMETRICS_API_KEY` environment variable, so in
most deployments `prostometrics.init("payments-api")` is enough.

## Metric methods

- `count` increases a counter.
- `count_unique` counts distinct identifiers approximately.
- `count_top` records that the person with `unique_id` touched `item` — an
  article, a product, an endpoint. The item is stored as sent (up to 256
  bytes), and the product ranks items by how many distinct ids touched them
  over any date range. It takes no labels.
- `total` records the current reading of a monotonically increasing total, and
  reports the difference from the previous reading.
- `value` records a numeric observation.
- `value_sparse` records a value whose last observation should carry across
  missing time buckets.
- `success` records a yes-or-no outcome — a payment went through, a job
  finished. It sends 100 for success and 0 for failure, so the metric's average
  is the success rate and the dashboard shows it as a percentage with no setup.
  Count failures separately with `count` if you also want how many.

Each is available on a `Client` instance and as a module-level function that
uses the client created by `init`.

## Labels

Labels are keyword arguments:

```python
prostometrics.count("requests", 1, method="GET", status="200")
```

The `"name=value"` string form used by the Go and Node clients is also accepted,
and the two can be mixed:

```python
prostometrics.count("requests", 1, "method=GET", status="200")
prostometrics.count("requests", 1, prostometrics.label("route", "/a=b"))
```

Use `prostometrics.label(name, value)` when a value may contain characters the
wire format reserves; it replaces them with `_`. Labels are sorted before
sending, so the order they are passed in never splits one series into two.

Up to 8 labels per series. Keep label values to a small, bounded set of
possibilities — a label carrying a user id or a request id multiplies one metric
into an unbounded number of stored series.

## Using a client directly

```python
from prostometrics import Client

client = Client("payments-api", api_key="...")
client.count("jobs.processed", 1, queue="default")
client.close()
```

`Client` is also a context manager:

```python
with Client("batch-job", api_key="...") as client:
    client.count("rows.imported", len(rows))
```

## Configuration

| Argument | Default | Purpose |
|---|---|---|
| `api_key` | `PROSTOMETRICS_API_KEY` | Prostometrics API key |
| `endpoint` | `PROSTOMETRICS_ENDPOINT`, else the public endpoint | Ingest endpoint URL |
| `logger` | `logging.getLogger("prostometrics")` | Where client diagnostics go |
| `verbose` | `False` | Adds recovery, retry, version and flush diagnostics at DEBUG |
| `silent` | `False` | Disables all client logging |
| `headers` | `{}` | Extra request headers |
| `transport` | HTTP transport | Custom delivery, mainly for tests |

Warnings that indicate metric loss or disabled ingestion are logged at WARNING.
Routine recovery, including successful dictionary resynchronization, is logged
at DEBUG and only when `verbose` is enabled. With no logging configured at all,
Python still prints warnings to stderr.

## Behavior

Metric calls append to a bounded in-memory queue and return. A single background
thread does the batching, network I/O, retries and buffering. Calls never raise
into application code and never block on the network, so they are safe inside
request handlers and hot loops.

A background thread — rather than `asyncio` — means the same client works
unchanged in threaded applications such as Django, Flask and Celery and in
event-loop applications such as FastAPI and aiohttp.

During temporary outages the client buffers up to 30 minutes of metrics in
memory and replays them gradually with their original timestamps, so a delayed
batch is written into the time bucket it was recorded in. The buffer is bounded
and is lost when the process exits.

A rejected API key disables ingestion for the rest of the process, with one
exception: for the first 30 seconds after the client is created, a rejection is
retried instead. A key created moments earlier takes a few seconds to become
usable, so a process started right after the key was pasted in keeps its first
metrics rather than going quiet.

Two things narrow that window rather than widening it. A client key — the
`_pk_` kind, which belongs in a browser and can never authenticate here — is
refused immediately and named as such, because no amount of waiting will make
it work. And a process that ends while a rejection is still unresolved says so
during `close()`, naming the events it never delivered, so a short script
written to check that a key works cannot exit quietly on the "retrying" line
alone.

## Prefork servers

Under gunicorn, uWSGI or Celery the process is forked into workers. The client
detects this and rebuilds its worker thread, its connection and its dictionary
session in each child, so metrics keep flowing whether the client was created
before or after the fork.

Creating the client per worker — in gunicorn's `post_fork` hook, or at first
use inside the worker — is still the clearer arrangement when you have the
choice.

```python
# gunicorn.conf.py
def post_fork(server, worker):
    import prostometrics

    prostometrics.init("payments-api")
```

## Shutdown

Queued metrics are flushed on interpreter exit automatically, with a two-second
budget. Call `close()` explicitly when you want a longer budget or a definite
point of delivery:

```python
client.close(timeout=10.0)
```

## Delivery statistics

```python
stats = client.stats()
print(stats.queue_depth, stats.buffered_events, stats.ingest_disabled)
```

Reports queue depth, buffered events and bytes, per-reason drop counts and
whether ingestion has been disabled.
