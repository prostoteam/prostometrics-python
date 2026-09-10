"""Protocol and client tuning constants.

Values mirror the Go and Node clients so that the three official clients behave
identically under the same conditions. Times are milliseconds unless a name says
otherwise.
"""

from __future__ import annotations

from typing import Final, Tuple

DEFAULT_QUEUE_SIZE: Final = 64 * 1024
DEFAULT_MAX_BATCH_SIZE: Final = 512
DEFAULT_MAX_SERIES_PER_BATCH: Final = 2048
DEFAULT_MAX_DICTIONARY_SERIES: Final = DEFAULT_MAX_SERIES_PER_BATCH
DEFAULT_MAX_TOTAL_SERIES: Final = DEFAULT_MAX_SERIES_PER_BATCH

DEFAULT_FLUSH_INTERVAL_MS: Final = 500
DEFAULT_FLUSH_TIMEOUT_MS: Final = 5_000

DEFAULT_RETRY_QUEUE_SIZE: Final = 4096
DEFAULT_RETRY_FLUSH_MAX_SENDS: Final = 1
DEFAULT_RETRY_BASE_DELAY_MS: Final = 1_000
DEFAULT_RETRY_MAX_DELAY_MS: Final = 8_000
DEFAULT_RETRY_JITTER_WINDOW_MS: Final = 1_000

DEFAULT_OUTAGE_BUFFER_MAX_AGE_MS: Final = 30 * 60 * 1_000
DEFAULT_OUTAGE_BUFFER_MAX_EVENTS: Final = 256 * 1024
DEFAULT_OUTAGE_BUFFER_MAX_BYTES: Final = 64 * 1024 * 1024
DEFAULT_REPLAY_INTERVAL_MS: Final = 1_000

DEFAULT_RECOVERY_JITTER_WINDOW_MS: Final = 30 * 1_000
DEFAULT_CLIENT_BACKOFF_MAX_DELAY_MS: Final = 30_000
DEFAULT_CLIENT_BACKOFF_JITTER_WINDOW_MS: Final = 5_000

DEFAULT_ENDPOINT_HOST: Final = "prostometrics.ru"
DEFAULT_INGEST_PATH: Final = "/api/i/batch"

API_KEY_ENV_VAR: Final = "PROSTOMETRICS_API_KEY"
ENDPOINT_ENV_VAR: Final = "PROSTOMETRICS_ENDPOINT"

HTTP_STATUS_UNAUTHORIZED: Final = 401
DEFAULT_STOP_STATUS_CODES: Final[Tuple[int, ...]] = (HTTP_STATUS_UNAUTHORIZED,)
RESPONSE_CODE_UNAUTHORIZED: Final = "unauthorized"
DEFAULT_STOP_RESPONSE_CODES: Final[Tuple[str, ...]] = (
    RESPONSE_CODE_UNAUTHORIZED,
    "unsupported_protocol_version",
)
RETRYABLE_STATUS_CODES: Final[Tuple[int, ...]] = (408, 429, 500, 502, 503, 504)

# A key created moments before the process started may not have reached the
# ingester's served key set yet, so the first requests of a brand-new project
# can be refused. Retrying those for a short window turns that race into a
# delay instead of a process that never reports a single metric. See the
# startup authentication grace in the ingest protocol specification.
DEFAULT_AUTH_GRACE_WINDOW_MS: Final = 30_000
DEFAULT_AUTH_GRACE_RETRY_INTERVAL_MS: Final = 2_000

BATCH_ID_HEADER_NAME: Final = "X-PM-Batch-Id"
WORKLOAD_HEADER_NAME: Final = "X-PM-Workload"
ACCEPTED_HEADER_NAME: Final = "X-PM-Accepted"
DROPPED_HEADER_NAME: Final = "X-PM-Dropped"
REJECTED_HEADER_NAME: Final = "X-PM-Rejected"

PROTOCOL_VERSION: Final = 5
TIMESTAMP_UNIT: Final = "s"

WORKLOAD_MAX_LEN: Final = 64
MAX_METRIC_BYTES: Final = 100
MAX_LABELS_PER_SERIES: Final = 8
MAX_LABEL_BYTES: Final = 512
RESERVED_LABEL_NAME: Final = "workload"

MAX_COUNTER_VALUE: Final = 4_294_967_295.0
MAX_SAMPLE_VALUE: Final = float(4_294_967_295 // 10)
MAX_UNIQUE_ID: Final = (1 << 64) - 1
MAX_TOP_ITEM_BYTES: Final = 256

MAX_VALUE_EVENTS_PER_SECOND: Final = 20_000
LOCAL_DROP_LOG_INTERVAL_MS: Final = 60_000
MAX_ERROR_BODY_BYTES: Final = 4096
DICTIONARY_RESYNC_WARNING_THRESHOLD: Final = 3
DICTIONARY_RESYNC_WARNING_WINDOW_MS: Final = 5 * 60 * 1_000
