"""Byte-level parity with the official Node client's encoder.

``tests/golden/protocol_v5.json`` is produced by running the Node client's own
``encodeLinePayloadV5`` over a fixed set of inputs — see
``tests/golden/generate.mjs``. Comparing against it catches the failure mode
that unit tests cannot: an encoder that is internally consistent but disagrees
with the other official clients about what goes on the wire.

The fixtures exercise the encoder, so their labels are already in the order the
encoder receives them. Label ordering itself is the normalizer's job and is
covered in ``test_labels.py``.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from prostometrics._dictionary import DictionaryState, encode_payload_v5
from prostometrics._payload import CounterEvent, Payload, UniqueEvent, ValueEvent

_GOLDEN = json.loads((Path(__file__).parent / "golden" / "protocol_v5.json").read_text(encoding="utf-8"))


def _payload_from(batch: dict) -> Payload:
    payload = Payload()
    payload.batch_id = "fixed-batch"
    for counter in batch["counters"]:
        payload.counters.append(
            CounterEvent(counter["metric"], counter["value"], list(counter["labels"]), counter["timestamp"])
        )
    for value in batch["values"]:
        payload.values.append(
            ValueEvent(
                value["metric"],
                value["value"],
                value["sparse"],
                list(value["labels"]),
                value["timestamp"],
                success=value.get("success", False),
            )
        )
    for unique in batch["uniques"]:
        payload.uniques.append(
            UniqueEvent(unique["metric"], unique["uniqueID"], list(unique["labels"]), unique["timestamp"])
        )
    return payload


@pytest.mark.parametrize("case", _GOLDEN["cases"], ids=lambda case: case["name"])
def test_encoder_matches_the_node_client_byte_for_byte(case):
    state = DictionaryState()
    produced = [
        encode_payload_v5(_payload_from(batch), state).decode("utf-8").replace(state.session_id, "<SESSION>")
        for batch in case["batches"]
    ]
    assert produced == case["bodies"]


def test_the_fixture_file_is_not_empty():
    assert len(_GOLDEN["cases"]) >= 9
    assert "prostometrics-node" in _GOLDEN["generator"]
