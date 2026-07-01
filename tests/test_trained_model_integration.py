from __future__ import annotations

from pathlib import Path

import pandas as pd

from prediction import trained_model

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_probability_risk_mapping_boundaries(monkeypatch):
    monkeypatch.setattr(trained_model, "HIGH_PROBABILITY_THRESHOLD", 0.80)
    monkeypatch.setattr(trained_model, "MEDIUM_PROBABILITY_THRESHOLD", 0.50)

    assert trained_model.probability_to_risk_level(0.80) == "HIGH"
    assert trained_model.probability_to_risk_level(0.50) == "MEDIUM"
    assert trained_model.probability_to_risk_level(0.49) == "LOW"


def test_validate_processed_features_reports_missing_columns():
    available = {"raw_id", "source_file", "engine_id", "time_in_cycles", "s2"}
    try:
        trained_model.validate_processed_features(["s2", "s3"], available_columns=available)
    except ValueError as exc:
        assert "s3" in str(exc)
    else:
        raise AssertionError("Expected missing-column validation failure")


def test_score_with_trained_model_uses_positive_probability():
    class FakeModel:
        classes_ = [0, 1]

        def predict_proba(self, features):
            assert list(features.columns) == ["s2"]
            return [[0.10, 0.90], [0.75, 0.25]]

    processed = pd.DataFrame(
        {
            "raw_id": [1, 2],
            "source_file": ["FD001", "FD001"],
            "engine_id": [1, 2],
            "time_in_cycles": [100, 120],
            "s2": [642.1, 641.2],
        }
    )
    scored = trained_model.score_with_trained_model(FakeModel(), processed, ["s2"])

    assert scored["risk_score"].tolist() == [0.9, 0.25]
    assert scored["risk_level"].tolist() == ["HIGH", "LOW"]


def test_no_old_hardcoded_paths_or_wrong_tables():
    for relative_path in ["model/batch_inference.py", "model/config.py", "src/dashboard/app.py"]:
        source = (PROJECT_ROOT / relative_path).read_text(encoding="utf-8")
        assert "/media/data" not in source
        assert "~/airflow/dags" not in source
        assert "predictions_table" not in source
        assert "processed_sensor_data_table" not in source
