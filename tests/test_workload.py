from __future__ import annotations

import pytest

from prostometrics._errors import InvalidWorkloadError
from prostometrics._workload import validate_workload


@pytest.mark.parametrize("workload", ["api", "billing-api", "team/service", "a.b_c-9", " padded "])
def test_accepts_valid_workloads(workload):
    assert validate_workload(workload) == workload.strip()


@pytest.mark.parametrize("workload", ["", "   ", "has space", "unicode-Ж", "x" * 65, "pipe|name"])
def test_rejects_invalid_workloads(workload):
    with pytest.raises(InvalidWorkloadError):
        validate_workload(workload)
