from __future__ import annotations

import pandas as pd

from prediction import prediction_pipeline as pipeline


def test_select_new_feature_rows_keeps_only_new_raw_ids():
    df = pd.DataFrame(
        {
            "raw_id": [1, 2, 3, 4],
            "source_file": ["FD001"] * 4,
            "engine_id": [1, 1, 1, 1],
            "time_in_cycles": [1, 2, 3, 4],
        }
    )
    selected = pipeline._select_new_feature_rows(df, {3, 4})
    assert selected["raw_id"].tolist() == [3, 4]


def test_no_new_raw_rows_finishes_success_without_saves(monkeypatch):
    calls: dict[str, object] = {}

    monkeypatch.setattr(
        pipeline,
        "get_checkpoint",
        lambda: {"pipeline_name": "prediction_pipeline", "last_processed_raw_id": 1000},
    )
    monkeypatch.setattr(pipeline, "create_prediction_run", lambda run_type, checkpoint_before=None: 99)
    monkeypatch.setattr(
        pipeline,
        "load_incremental_raw_data",
        lambda checkpoint_before, window_cycles: (pd.DataFrame(), pd.DataFrame(), 0),
    )

    def finish_prediction_run(run_id, status, raw_rows_used, notes=None, **kwargs):
        calls["finish"] = {
            "run_id": run_id,
            "status": status,
            "raw_rows_used": raw_rows_used,
            "notes": notes,
            **kwargs,
        }

    monkeypatch.setattr(pipeline, "finish_prediction_run", finish_prediction_run)
    monkeypatch.setattr(pipeline, "save_processed_features", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("save should not run")))
    monkeypatch.setattr(pipeline, "save_prediction_results", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("save should not run")))
    monkeypatch.setattr(pipeline, "save_alerts", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("save should not run")))
    monkeypatch.setattr(pipeline, "update_checkpoint", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("checkpoint should not update")))

    result = pipeline.run_prediction_pipeline(run_type="manual", window_cycles=50)

    assert result["status"] == "SUCCESS"
    assert result["raw_rows_used"] == 0
    assert result["checkpoint_before"] == 1000
    assert result["checkpoint_after"] == 1000
    assert calls["finish"]["status"] == "SUCCESS"
    assert calls["finish"]["raw_rows_used"] == 0
