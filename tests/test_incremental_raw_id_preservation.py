from __future__ import annotations

import pandas as pd

from config import SENSOR_COLUMNS
from prediction.features import build_features, clean_raw_dataframe, latest_engine_features, score_risk
from prediction.prediction_pipeline import _select_new_feature_rows


def _row(raw_id: int, cycle: int) -> dict[str, object]:
    row = {"raw_id": raw_id, "source_file": "FD001", "engine_id": 1, "time_in_cycles": cycle}
    for col in SENSOR_COLUMNS:
        if col in ("engine_id", "time_in_cycles"):
            continue
        row[col] = float(cycle)
    return row


def test_raw_id_survives_clean_feature_filter_and_scoring():
    raw_df = pd.DataFrame([_row(101, 1), _row(102, 2), _row(103, 3)])

    cleaned = clean_raw_dataframe(raw_df)
    assert cleaned["raw_id"].tolist() == [101, 102, 103]

    features = build_features(cleaned)
    assert features["raw_id"].tolist() == [101, 102, 103]

    new_features = _select_new_feature_rows(features, {102, 103})
    assert new_features["raw_id"].tolist() == [102, 103]

    latest = latest_engine_features(new_features)
    scored = score_risk(latest)
    assert scored.iloc[0]["raw_id"] == 103
