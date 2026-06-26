from __future__ import annotations

import pandas as pd

from config import SENSOR_COLUMNS
from prediction.features import build_features, clean_raw_dataframe, latest_engine_features, score_risk


def _sample_raw() -> pd.DataFrame:
    rows = []
    for engine_id in [1, 2]:
        for cycle in range(1, 6):
            row = {"source_file": "FD001", "engine_id": engine_id, "time_in_cycles": cycle}
            for col in SENSOR_COLUMNS:
                if col in ("engine_id", "time_in_cycles"):
                    continue
                row[col] = float(cycle + engine_id)
            rows.append(row)
    return pd.DataFrame(rows)


def test_clean_raw_dataframe_keeps_raw_window_valid_and_ordered():
    raw = _sample_raw()
    raw.loc[0, "s2"] = None
    raw = pd.concat([raw, raw.head(1)], ignore_index=True)

    cleaned = clean_raw_dataframe(raw)

    assert len(cleaned) == 10
    assert cleaned["s2"].isna().sum() == 0
    assert cleaned.iloc[0]["engine_id"] == 1
    assert cleaned.iloc[-1]["engine_id"] == 2


def test_build_features_creates_rolling_and_delta_columns():
    cleaned = clean_raw_dataframe(_sample_raw())
    features = build_features(cleaned)

    for column in ["rolling_avg_s2", "rolling_std_s2", "delta_s2", "demo_rul"]:
        assert column in features.columns

    latest_engine_1 = features[(features["engine_id"] == 1) & (features["time_in_cycles"] == 5)].iloc[0]
    assert latest_engine_1["demo_rul"] == 0


def test_score_risk_outputs_one_result_per_latest_engine():
    cleaned = clean_raw_dataframe(_sample_raw())
    features = build_features(cleaned)
    latest = latest_engine_features(features)
    scored = score_risk(latest)

    assert len(scored) == 2
    assert {"risk_score", "predicted_rul", "risk_level", "recommended_action"}.issubset(scored.columns)
    assert scored["risk_score"].between(0, 100).all()
