from __future__ import annotations

import os

import numpy as np
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from config import PROJECT_ROOT
from prediction.prediction_pipeline import (
    _connect_postgres,
    _execute_finish_prediction_run,
    _execute_values,
    _real_dict_cursor,
    create_prediction_run,
    finish_prediction_run,
)

TRAINED_MODEL_VERSION = os.getenv("TRAINED_MODEL_VERSION", "xgboost-trained-v1")
MODEL_PATH = Path(
    os.getenv("MODEL_PATH", str(PROJECT_ROOT / "model" / "xgboost_predictive_model.pkl"))
).resolve()
HIGH_PROBABILITY_THRESHOLD = float(os.getenv("TRAINED_MODEL_HIGH_THRESHOLD", "0.80"))
MEDIUM_PROBABILITY_THRESHOLD = float(os.getenv("TRAINED_MODEL_MEDIUM_THRESHOLD", "0.50"))

REQUIRED_METADATA_COLUMNS = ["raw_id", "source_file", "engine_id", "time_in_cycles"]


def load_trained_model(model_path: Path | str = MODEL_PATH) -> Any:
    """Load the persisted XGBoost classifier from the project model directory."""
    import joblib

    resolved_path = Path(model_path).resolve()
    if not resolved_path.exists():
        raise FileNotFoundError(
            f"Trained model file was not found at {resolved_path}. "
            "Set MODEL_PATH or mount model/xgboost_predictive_model.pkl into the container."
        )
    return joblib.load(resolved_path)


def extract_model_features(model: Any) -> list[str]:
    """Return the exact feature order expected by the trained model."""
    if hasattr(model, "feature_names_in_"):
        return [str(feature) for feature in list(model.feature_names_in_)]

    if hasattr(model, "get_booster"):
        booster = model.get_booster()
        feature_names = getattr(booster, "feature_names", None)
        if feature_names:
            return [str(feature) for feature in feature_names]

    raise ValueError(
        "Could not extract feature names from the trained model. "
        "Retrain/export the model with named pandas columns or provide a wrapper with feature_names_in_."
    )


def load_processed_table_columns() -> set[str]:
    with _connect_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = 'processed_sensor_data';
                """
            )
            return {row[0] for row in cursor.fetchall()}


def validate_processed_features(feature_names: Iterable[str], available_columns: set[str] | None = None) -> None:
    available = available_columns if available_columns is not None else load_processed_table_columns()
    required = [*REQUIRED_METADATA_COLUMNS, *list(feature_names)]
    missing = [column for column in required if column not in available]
    if missing:
        raise ValueError(
            "processed_sensor_data is missing columns required by the trained model: "
            + ", ".join(missing)
        )


def load_latest_processed_rows(feature_names: list[str], limit: int | None = None) -> pd.DataFrame:
    """Load the latest processed row per source/engine from PostgreSQL.

    The trained classifier is run against the latest health state of every engine,
    matching the dashboard use case rather than re-scoring every historical cycle.
    """
    validate_processed_features(feature_names)

    select_columns = [*REQUIRED_METADATA_COLUMNS, *feature_names]
    quoted_columns = ", ".join(select_columns)
    limit_sql = ""
    params: tuple[Any, ...] = ()
    if limit and limit > 0:
        limit_sql = " LIMIT %s"
        params = (int(limit),)

    query = f"""
        WITH latest AS (
            SELECT DISTINCT ON (source_file, engine_id)
                {quoted_columns}
            FROM processed_sensor_data
            ORDER BY source_file, engine_id, time_in_cycles DESC, raw_id DESC
        )
        SELECT {quoted_columns}
        FROM latest
        ORDER BY source_file, engine_id{limit_sql};
    """

    with _connect_postgres() as conn:
        with conn.cursor(cursor_factory=_real_dict_cursor()) as cursor:
            cursor.execute(query, params)
            rows = cursor.fetchall()
    return pd.DataFrame(rows, columns=select_columns)


def probability_to_risk_level(probability: float) -> str:
    if probability >= HIGH_PROBABILITY_THRESHOLD:
        return "HIGH"
    if probability >= MEDIUM_PROBABILITY_THRESHOLD:
        return "MEDIUM"
    return "LOW"


def recommendation_for_risk(level: str) -> str:
    return {
        "HIGH": "Schedule urgent inspection during the nearest safe stop.",
        "MEDIUM": "Monitor this engine closely in the next prediction run.",
        "LOW": "Continue normal operation.",
    }[level]


def _positive_class_index(model: Any) -> int:
    classes = list(getattr(model, "classes_", []))
    if 1 in classes:
        return classes.index(1)
    if "1" in classes:
        return classes.index("1")
    if len(classes) >= 2:
        return 1
    return 0


def score_with_trained_model(model: Any, processed_df: pd.DataFrame, feature_names: list[str]) -> pd.DataFrame:
    if processed_df.empty:
        return processed_df.copy()

    scored = processed_df.copy()
    features = scored.reindex(columns=feature_names).apply(pd.to_numeric, errors="coerce").fillna(0.0)

    if hasattr(model, "predict_proba"):
        probabilities = np.asarray(model.predict_proba(features))
        positive_index = _positive_class_index(model)
        risk_scores = probabilities[:, positive_index]
    else:
        predictions = model.predict(features)
        risk_scores = pd.Series(predictions, index=scored.index).astype(float).clip(0.0, 1.0).to_numpy()

    scored["risk_score"] = pd.Series(risk_scores, index=scored.index).astype(float).round(6)
    scored["predicted_rul"] = None
    scored["risk_level"] = scored["risk_score"].apply(probability_to_risk_level)
    scored["recommended_action"] = scored["risk_level"].apply(recommendation_for_risk)
    return scored


def _execute_save_trained_prediction_results(
    cursor,
    run_id: int,
    predictions: pd.DataFrame,
    model_version: str = TRAINED_MODEL_VERSION,
) -> int:
    if predictions.empty:
        return 0

    rows = []
    for _, row in predictions.iterrows():
        predicted_rul = row.get("predicted_rul")
        rows.append(
            (
                run_id,
                int(row["raw_id"]),
                model_version,
                row["source_file"],
                int(row["engine_id"]),
                int(row["time_in_cycles"]),
                float(row["risk_score"]),
                None if pd.isna(predicted_rul) else float(predicted_rul),
                row["risk_level"],
                row["recommended_action"],
            )
        )

    sql = """
        INSERT INTO prediction_results (
            run_id, raw_id, model_version, source_file, engine_id, latest_cycle,
            risk_score, predicted_rul, risk_level, recommended_action
        )
        VALUES %s
        ON CONFLICT (raw_id, model_version)
        DO UPDATE SET
            run_id=EXCLUDED.run_id,
            source_file=EXCLUDED.source_file,
            engine_id=EXCLUDED.engine_id,
            latest_cycle=EXCLUDED.latest_cycle,
            risk_score=EXCLUDED.risk_score,
            predicted_rul=EXCLUDED.predicted_rul,
            risk_level=EXCLUDED.risk_level,
            recommended_action=EXCLUDED.recommended_action,
            prediction_time=NOW();
    """
    _execute_values(cursor, sql, rows, page_size=500)
    return len(rows)


def _execute_save_trained_alerts(
    cursor,
    run_id: int,
    predictions: pd.DataFrame,
    model_version: str = TRAINED_MODEL_VERSION,
) -> int:
    if predictions.empty:
        return 0

    alert_df = predictions[predictions["risk_level"] == "HIGH"]
    if alert_df.empty:
        return 0

    rows = []
    for _, row in alert_df.iterrows():
        message = (
            f"HIGH failure probability for engine {int(row['engine_id'])} "
            f"at cycle {int(row['time_in_cycles'])}: probability={float(row['risk_score']):.4f}"
        )
        rows.append(
            (
                run_id,
                int(row["raw_id"]),
                model_version,
                row["source_file"],
                int(row["engine_id"]),
                row["risk_level"],
                message,
            )
        )

    sql = """
        INSERT INTO alerts (run_id, raw_id, model_version, source_file, engine_id, severity, message)
        VALUES %s
        ON CONFLICT (raw_id, model_version)
        DO UPDATE SET
            run_id=EXCLUDED.run_id,
            source_file=EXCLUDED.source_file,
            engine_id=EXCLUDED.engine_id,
            severity=EXCLUDED.severity,
            message=EXCLUDED.message,
            is_resolved=FALSE,
            created_at=NOW();
    """
    _execute_values(cursor, sql, rows, page_size=500)
    return len(rows)


def load_schedule_times() -> list[str]:
    with _connect_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT time_1, time_2, time_3 FROM schedule_settings WHERE id = 1;")
            row = cursor.fetchone()
    return [value for value in row] if row else []


def is_scheduled_time(now: datetime | None = None) -> bool:
    current = (now or datetime.now()).strftime("%H:%M")
    return current in load_schedule_times()


def run_trained_model_prediction(
    run_type: str = "dashboard_manual",
    *,
    respect_schedule: bool = False,
    limit: int | None = None,
    model_path: Path | str = MODEL_PATH,
    model_version: str = TRAINED_MODEL_VERSION,
) -> dict[str, Any]:
    """Run the real trained model from processed_sensor_data into existing output tables."""
    if respect_schedule and not is_scheduled_time():
        current = datetime.now().strftime("%H:%M")
        notes = f"Skipped trained model run; current time {current} is not in schedule_settings."
        print(f"[trained-model] SKIPPED {notes}")
        return {"status": "SKIPPED", "raw_rows_used": 0, "prediction_rows": 0, "alert_rows": 0, "notes": notes}

    run_id = create_prediction_run(run_type)
    raw_rows_used = 0
    from_raw_id: int | None = None
    to_raw_id: int | None = None

    try:
        model = load_trained_model(model_path)
        feature_names = extract_model_features(model)
        processed_df = load_latest_processed_rows(feature_names, limit=limit)
        raw_rows_used = len(processed_df)

        if processed_df.empty:
            notes = "No rows found in processed_sensor_data; run the processing pipeline before trained prediction."
            finish_prediction_run(run_id, "SUCCESS", 0, notes)
            print(f"[trained-model] SUCCESS run_id={run_id} {notes}")
            return {
                "run_id": run_id,
                "status": "SUCCESS",
                "raw_rows_used": 0,
                "prediction_rows": 0,
                "alert_rows": 0,
                "model_version": model_version,
                "model_features": feature_names,
                "notes": notes,
            }

        from_raw_id = int(processed_df["raw_id"].min())
        to_raw_id = int(processed_df["raw_id"].max())
        predictions = score_with_trained_model(model, processed_df, feature_names)

        with _connect_postgres() as conn:
            try:
                with conn.cursor() as cursor:
                    prediction_rows = _execute_save_trained_prediction_results(cursor, run_id, predictions, model_version)
                    alert_rows = _execute_save_trained_alerts(cursor, run_id, predictions, model_version)
                    notes = (
                        f"trained_model={Path(model_path).name}; model_version={model_version}; "
                        f"features={len(feature_names)}; source_table=processed_sensor_data; "
                        f"scope=latest_per_source_engine; prediction_rows={prediction_rows}; alerts={alert_rows}"
                    )
                    _execute_finish_prediction_run(
                        cursor,
                        run_id,
                        "SUCCESS",
                        raw_rows_used,
                        notes,
                        from_raw_id=from_raw_id,
                        to_raw_id=to_raw_id,
                        checkpoint_after=None,
                    )
                conn.commit()
            except Exception:
                conn.rollback()
                raise

        print(f"[trained-model] SUCCESS run_id={run_id} {notes}")
        return {
            "run_id": run_id,
            "status": "SUCCESS",
            "raw_rows_used": raw_rows_used,
            "prediction_rows": prediction_rows,
            "alert_rows": alert_rows,
            "model_version": model_version,
            "model_features": feature_names,
            "from_raw_id": from_raw_id,
            "to_raw_id": to_raw_id,
            "notes": notes,
        }
    except Exception as exc:
        finish_prediction_run(
            run_id,
            "FAILED",
            raw_rows_used,
            str(exc),
            from_raw_id=from_raw_id,
            to_raw_id=to_raw_id,
        )
        print(f"[trained-model] FAILED run_id={run_id}: {exc}")
        raise
