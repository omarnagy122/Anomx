from __future__ import annotations

import pandas as pd
import pytest

from ingestion.kafka_producer import _slice_rows


def test_slice_rows_uses_one_based_start_and_limit():
    df = pd.DataFrame({"value": list(range(1, 11))})
    sliced = _slice_rows(df, start_row=4, limit=3)
    assert sliced["value"].tolist() == [4, 5, 6]


def test_slice_rows_rejects_invalid_start_row():
    df = pd.DataFrame({"value": [1, 2, 3]})
    with pytest.raises(ValueError):
        _slice_rows(df, start_row=0, limit=1)
