"""Protocol v5 dictionary session state and batch encoding.

A dictionary session maps series definitions to small integer IDs so repeated
events do not resend metric names and labels. The session is identified by a
client-chosen ID and a revision that increments whenever new series IDs are
introduced.
"""

from __future__ import annotations

import math
import secrets
from collections.abc import Sequence
from typing import Dict, List, Optional, Tuple

from ._constants import DEFAULT_MAX_DICTIONARY_SERIES, PROTOCOL_VERSION, TIMESTAMP_UNIT
from ._payload import Payload
from ._series import SeriesDefinition, series_key

DictionarySnapshot = Tuple[int, int]


class DictionaryState:
    __slots__ = ("revision", "series", "series_ids", "session_id")

    def __init__(self) -> None:
        self.session_id = secrets.token_hex(16)
        self.revision = 0
        self.series_ids: Dict[str, int] = {}
        self.series: List[SeriesDefinition] = []


def should_reset_dictionary(state: Optional[DictionaryState]) -> bool:
    return state is None or len(state.series) >= DEFAULT_MAX_DICTIONARY_SERIES


def snapshot_dictionary(state: DictionaryState) -> DictionarySnapshot:
    """Record the dictionary position before encoding a batch."""
    return (len(state.series), state.revision)


def rollback_dictionary(state: DictionaryState, snapshot: DictionarySnapshot) -> None:
    """Forget series definitions the server never received.

    Encoding registers each new series in the session and bumps the revision.
    If the request carrying those ``S|`` lines then fails, the server has never
    seen them, but the client would remember them and omit the definitions from
    the retry — leaving event lines pointing at series the server cannot
    resolve. Undoing the registration makes a retry re-send them.
    """
    series_count, revision = snapshot
    for definition in state.series[series_count:]:
        state.series_ids.pop(series_key(definition.metric, definition.labels), None)
    del state.series[series_count:]
    state.revision = revision


def encode_payload_v5(
    payload: Optional[Payload],
    state: DictionaryState,
    force_definitions: bool = False,
) -> bytes:
    """Encode one batch, mutating ``state`` with any newly introduced series.

    With ``force_definitions`` the whole dictionary is restated, which is what a
    session restart after a server-side dictionary miss requires.
    """
    if payload is None or payload.is_empty():
        return b""

    new_series_ids: List[int] = []
    series_changed = False
    # The last element is what follows the timestamp: nothing for most kinds,
    # the item for a top-list event.
    encoded: List[Tuple[str, int, str, int, str]] = []

    def series_id_for(metric: str, labels: Sequence[str]) -> int:
        nonlocal series_changed
        key = series_key(metric, labels)
        existing = state.series_ids.get(key)
        if existing is not None:
            return existing
        assigned = len(state.series)
        state.series_ids[key] = assigned
        state.series.append(SeriesDefinition(metric, labels))
        new_series_ids.append(assigned)
        series_changed = True
        return assigned

    for counter in payload.counters:
        encoded.append(
            (
                "c",
                series_id_for(counter.metric, counter.labels),
                str(round_away_from_zero(counter.value)),
                counter.timestamp,
                "",
            )
        )
    for value in payload.values:
        encoded.append(
            (
                "s" if value.sparse else "o" if value.success else "v",
                series_id_for(value.metric, value.labels),
                format_sample(value.value),
                value.timestamp,
                "",
            )
        )
    for unique in payload.uniques:
        encoded.append(("u", series_id_for(unique.metric, unique.labels), unique.unique_id, unique.timestamp, ""))
    for top in payload.tops:
        # The item is the last field so that a "|" inside it needs no escaping;
        # it goes on the wire exactly as the caller passed it.
        encoded.append(("t", series_id_for(top.metric, top.labels), top.unique_id, top.timestamp, "|" + top.item))

    if series_changed:
        state.revision += 1

    lines: List[str] = [f"H|{PROTOCOL_VERSION}|{TIMESTAMP_UNIT}|{state.session_id}|{state.revision}"]
    if force_definitions:
        for index, definition in enumerate(state.series):
            lines.append(_series_line(index, definition))
    elif series_changed:
        for index in new_series_ids:
            lines.append(_series_line(index, state.series[index]))

    for metric_type, series_id, rendered, timestamp, tail in encoded:
        lines.append(f"{metric_type}|{series_id}|{rendered}|{timestamp}{tail}")

    return ("\n".join(lines) + "\n").encode("utf-8")


def _series_line(series_id: int, definition: SeriesDefinition) -> str:
    parts = [f"S|{series_id}|{definition.metric}"]
    parts.extend(item for item in definition.labels if item)
    return "|".join(parts)


def round_away_from_zero(value: float) -> int:
    """Round to the nearest integer, halves away from zero.

    Python's built-in ``round`` rounds halves to even, which would make counter
    deltas disagree with the Go and Node clients on exact ``.5`` values.
    """
    if not math.isfinite(value):
        return 0
    if value >= 0:
        return int(value + 0.5)
    return -int(-value + 0.5)


def format_sample(value: float) -> str:
    """Render a value sample the way the other official clients render it.

    An integral sample is written without a trailing ``.0`` so that batches
    encoded by the three clients are byte-identical for the same input.

    The value is coerced to float first because ``int.is_integer`` only exists
    from Python 3.12, and a caller assembling a Payload by hand can hand an int
    straight to the encoder.
    """
    sample = float(value)
    if not math.isfinite(sample):
        return "0"
    if sample.is_integer() and abs(sample) < 1e16:
        return str(int(sample))
    return repr(sample)


class LinePayloadDiagnostics:
    __slots__ = (
        "dictionary_revision",
        "dictionary_series",
        "dictionary_session",
        "event_lines",
        "series_definitions",
    )

    def __init__(
        self,
        dictionary_session: str,
        dictionary_revision: int,
        dictionary_series: int,
        series_definitions: int,
        event_lines: int,
    ) -> None:
        self.dictionary_session = dictionary_session
        self.dictionary_revision = dictionary_revision
        self.dictionary_series = dictionary_series
        self.series_definitions = series_definitions
        self.event_lines = event_lines


def analyze_payload_v5(body: bytes, state: Optional[DictionaryState]) -> LinePayloadDiagnostics:
    """Summarize an encoded body so a failure can be reported with its shape."""
    session = ""
    revision = 0
    definitions = 0
    events = 0
    for line in body.decode("utf-8", "replace").split("\n"):
        if not line:
            continue
        if line.startswith("H|"):
            parts = line.split("|")
            session = parts[3] if len(parts) > 3 else ""
            try:
                revision = int(parts[4]) if len(parts) > 4 else 0
            except ValueError:
                revision = 0
            continue
        if line.startswith("S|"):
            definitions += 1
            continue
        events += 1
    return LinePayloadDiagnostics(
        dictionary_session=session,
        dictionary_revision=revision,
        dictionary_series=len(state.series) if state else 0,
        series_definitions=definitions,
        event_lines=events,
    )
