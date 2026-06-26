from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values

sys.path.append(str(Path(__file__).resolve().parents[1]))
from config import (  # noqa: E402
    ALERT_HIGH_THRESHOLD,
    FEATURE_SENSOR_COLUMNS,
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
    PREDICTION_WINDOW_CYCLES,
    SENSOR_COLUMNS,
)
from prediction.features import (  # noqa: E402
    build_features,
    clean_raw_dataframe,
    latest_engine_features,
    score_risk,
)

PIPELINE_NAME = os.getenv("PREDICTION_PIPELINE_NAME", "prediction_pipeline")
MODEL_VERSION = os.getenv("MODEL_VERSION", "demo-v1")
RAW_SELECT_COLUMNS = ["id AS raw_id", "source_file", *SENSOR_COLUMNS, "inserted_at"]
RAW_DF_COLUMNS = ["raw_id", "source_file", *SENSOR_COLUMNS, "inserted_at"]
PROCESSED_COLUMNS = [
    "raw_id", "source_file", "engine_id", "time_in_cycles", "op_setting_1", "op_setting_2", "op_setting_3",
    "s2", "s3", "s4", "s7", "s8", "s9", "s11", "s12", "s13", "s14", "s15", "s17", "s20", "s21",
    *[
        feature
        for sensor in FEATURE_SENSOR_COLUMNS
        for feature in (f"rolling_avg_{sensor}", f"rolling_std_{sensor}", f"delta_{sensor}")
    ],
    "demo_rul",
]


def _connect_postgres():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", POSTGRES_HOST),
        port=int(os.getenv("POSTGRES_PORT", str(POSTGRES_PORT))),
        database=os.getenv("POSTGRES_DB", POSTGRES_DB),
        user=os.getenv("POSTGRES_USER", POSTGRES_USER),
        password=os.getenv("POSTGRES_PASSWORD", POSTGRES_PASSWORD),
    )


def ensure_checkpoint_row() -> None:
    with _connect_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO processing_checkpoints (pipeline_name, last_processed_raw_id)
                VALUES (%s, 0)
                ON CONFLICT (pipeline_name) DO NOTHING;
                """,
                (PIPELINE_NAME,),
            )
        conn.commit()


def get_checkpoint() -> dict[str, Any]:
    ensure_checkpoint_row()
    with _connect_postgres() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                SELECT pipeline_name, last_processed_raw_id, last_processed_inserted_at, updated_at
                FROM processing_checkpoints
                WHERE pipeline_name = %s;
                """,
                (PIPELINE_NAME,),
            )
            row = cursor.fetchone()
    if not row:
        return {"pipeline_name": PIPELINE_NAME, "last_processed_raw_id": 0, "last_processed_inserted_at": None}
    return dict(row)


def update_checkpoint(max_raw_id: int, max_inserted_at: Any | None) -> None:
    with _connect_postgres() as conn:
        with conn.cursor() as cursor:
            _execute_checkpoint_update(cursor, max_raw_id, max_inserted_at)
        conn.commit()


def _execute_checkpoint_update(cursor, max_raw_id: int, max_inserted_at: Any | None) -> None:
    cursor.execute(
        """
        INSERT INTO processing_checkpoints (
            pipeline_name, last_processed_raw_id, last_processed_inserted_at, updated_at
        )
        VALUES (%s, %s, %s, NOW())
        ON CONFLICT (pipeline_name)
        DO UPDATE SET
            last_processed_raw_id = GREATEST(
                processing_checkpoints.last_processed_raw_id,
                EXCLUDED.last_processed_raw_id
            ),
            last_processed_inserted_at = EXCLUDED.last_processed_inserted_at,
            updated_at = NOW();
        """,
        (PIPELINE_NAME, max_raw_id, max_inserted_at),
    )


def load_new_raw_rows(last_processed_raw_id: int) -> pd.DataFrame:
    query = f"""
        SELECT {", ".join(RAW_SELECT_COLUMNS)}
        FROM raw_sensor_data
        WHERE id > %s
        ORDER BY id;
    """
    with _connect_postgres() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, (last_processed_raw_id,))
            rows = cursor.fetchall()
    return pd.DataFrame(rows, columns=RAW_DF_COLUMNS)


def load_context_rows(last_processed_raw_id: int, new_raw_df: pd.DataFrame, context_cycles: int) -> pd.DataFrame:
    """Load a small historical context per engine/source for rolling features.

    Context rows are used only for calculations. Outputs are saved only for rows
    whose ``raw_id`` is in the new batch.
    """
    if new_raw_df.empty or last_processed_raw_id <= 0 or context_cycles <= 0:
        return pd.DataFrame(columns=RAW_DF_COLUMNS)

    pairs = (
        new_raw_df[["source_file", "engine_id"]]
        .drop_duplicates()
        .sort_values(["source_file", "engine_id"])
        .to_records(index=False)
        .tolist()
    )
    if not pairs:
        return pd.DataFrame(columns=RAW_DF_COLUMNS)

    values_sql = ", ".join(["(%s, %s)"] * len(pairs))
    pair_values: list[Any] = []
    for source_file, engine_id in pairs:
        pair_values.extend([source_file, int(engine_id)])

    query = f"""
        WITH target_pairs(source_file, engine_id) AS (
            VALUES {values_sql}
        ), ranked AS (
            SELECT r.id AS raw_id,
                   r.source_file,
                   {", ".join(["r." + c for c in SENSOR_COLUMNS])},
                   r.inserted_at,
                   ROW_NUMBER() OVER (
                       PARTITION BY r.source_file, r.engine_id
                       ORDER BY r.time_in_cycles DESC, r.id DESC
                   ) AS rn
            FROM raw_sensor_data r
            INNER JOIN target_pairs p
                ON p.source_file = r.source_file AND p.engine_id = r.engine_id
            WHERE r.id <= %s
        )
        SELECT {", ".join(RAW_DF_COLUMNS)}
        FROM ranked
        WHERE rn <= %s
        ORDER BY source_file, engine_id, time_in_cycles, raw_id;
    """
    params = [*pair_values, last_processed_raw_id, context_cycles]
    with _connect_postgres() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, params)
            rows = cursor.fetchall()
    return pd.DataFrame(rows, columns=RAW_DF_COLUMNS)


def load_incremental_raw_data(last_processed_raw_id: int, context_cycles: int) -> tuple[pd.DataFrame, pd.DataFrame, int]:
    new_raw_df = load_new_raw_rows(last_processed_raw_id)
    if new_raw_df.empty:
        return new_raw_df, pd.DataFrame(columns=RAW_DF_COLUMNS), 0

    context_df = load_context_rows(last_processed_raw_id, new_raw_df, context_cycles)
    combined_df = pd.concat([context_df, new_raw_df], ignore_index=True)
    return new_raw_df, combined_df, len(context_df)


def create_prediction_run(
    run_type: str,
    scheduled_time: str | None = None,
    checkpoint_before: int | None = None,
) -> int:
    scheduled_value = scheduled_time or datetime.now(timezone.utc).isoformat()
    with _connect_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO prediction_runs (run_type, scheduled_time, status, checkpoint_before)
                VALUES (%s, %s, 'RUNNING', %s)
                RETURNING run_id;
                """,
                (run_type, scheduled_value, checkpoint_before),
            )
            run_id = cursor.fetchone()[0]
        conn.commit()
    return int(run_id)


def finish_prediction_run(
    run_id: int,
    status: str,
    raw_rows_used: int,
    notes: str | None = None,
    *,
    from_raw_id: int | None = None,
    to_raw_id: int | None = None,
    checkpoint_after: int | None = None,
) -> None:
    with _connect_postgres() as conn:
        with conn.cursor() as cursor:
            _execute_finish_prediction_run(
                cursor,
                run_id,
                status,
                raw_rows_used,
                notes,
                from_raw_id=from_raw_id,
                to_raw_id=to_raw_id,
                checkpoint_after=checkpoint_after,
            )
        conn.commit()


def _execute_finish_prediction_run(
    cursor,
    run_id: int,
    status: str,
    raw_rows_used: int,
    notes: str | None,
    *,
    from_raw_id: int | None = None,
    to_raw_id: int | None = None,
    checkpoint_after: int | None = None,
) -> None:
    cursor.execute(
        """
        UPDATE prediction_runs
        SET status=%s,
            raw_rows_used=%s,
            from_raw_id=%s,
            to_raw_id=%s,
            checkpoint_after=%s,
            notes=%s,
            finished_at=NOW()
        WHERE run_id=%s;
        """,
        (status, raw_rows_used, from_raw_id, to_raw_id, checkpoint_after, notes, run_id),
    )


def _clean_value(value: Any) -> Any:
    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        return value.item()
    return value


def _execute_save_processed_features(cursor, feature_df: pd.DataFrame) -> int:
    if feature_df.empty:
        return 0

    rows = [tuple(_clean_value(row.get(col)) for col in PROCESSED_COLUMNS) for _, row in feature_df.iterrows()]
    update_cols = [col for col in PROCESSED_COLUMNS if col != "raw_id"]
    update_sql = ", ".join([f"{col}=EXCLUDED.{col}" for col in update_cols])
    sql = f"""
        INSERT INTO processed_sensor_data ({", ".join(PROCESSED_COLUMNS)})
        VALUES %s
        ON CONFLICT (raw_id)
        DO UPDATE SET {update_sql}, processed_at=NOW();
    """
    execute_values(cursor, sql, rows, page_size=500)
    return len(rows)


def save_processed_features(feature_df: pd.DataFrame) -> int:
    with _connect_postgres() as conn:
        with conn.cursor() as cursor:
            count = _execute_save_processed_features(cursor, feature_df)
        conn.commit()
    return count


def _execute_save_prediction_results(cursor, run_id: int, predictions: pd.DataFrame, model_version: str = MODEL_VERSION) -> int:
    if predictions.empty:
        return 0

    rows = []
    for _, row in predictions.iterrows():
        rows.append(
            (
                run_id,
                int(row["raw_id"]),
                model_version,
                row["source_file"],
                int(row["engine_id"]),
                int(row["time_in_cycles"]),
                float(row["risk_score"]),
                float(row["predicted_rul"]),
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
    execute_values(cursor, sql, rows, page_size=500)
    return len(rows)


def save_prediction_results(run_id: int, predictions: pd.DataFrame, model_version: str = MODEL_VERSION) -> int:
    with _connect_postgres() as conn:
        with conn.cursor() as cursor:
            count = _execute_save_prediction_results(cursor, run_id, predictions, model_version)
        conn.commit()
    return count


def _execute_save_alerts(cursor, run_id: int, predictions: pd.DataFrame, model_version: str = MODEL_VERSION) -> int:
    alert_df = predictions[predictions["risk_score"] >= ALERT_HIGH_THRESHOLD] if not predictions.empty else predictions
    if alert_df.empty:
        return 0

    rows = []
    for _, row in alert_df.iterrows():
        severity = row["risk_level"]
        rows.append(
            (
                run_id,
                int(row["raw_id"]),
                model_version,
                row["source_file"],
                int(row["engine_id"]),
                severity,
                f"{severity} risk for engine {int(row['engine_id'])} at cycle {int(row['time_in_cycles'])}: score={float(row['risk_score'])}",
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
    execute_values(cursor, sql, rows, page_size=500)
    return len(rows)


def save_alerts(run_id: int, predictions: pd.DataFrame, model_version: str = MODEL_VERSION) -> int:
    with _connect_postgres() as conn:
        with conn.cursor() as cursor:
            count = _execute_save_alerts(cursor, run_id, predictions, model_version)
        conn.commit()
    return count


def commit_successful_outputs(
    *,
    run_id: int,
    new_feature_df: pd.DataFrame,
    predictions: pd.DataFrame,
    raw_count: int,
    from_raw_id: int,
    to_raw_id: int,
    checkpoint_before: int,
    max_inserted_at: Any | None,
    notes: str,
) -> tuple[int, int, int]:
    """Atomically save outputs, update checkpoint, and mark the run successful.

    If any write fails, the transaction rolls back and the checkpoint is not moved.
    """
    with _connect_postgres() as conn:
        try:
            with conn.cursor() as cursor:
                processed_rows = _execute_save_processed_features(cursor, new_feature_df)
                prediction_rows = _execute_save_prediction_results(cursor, run_id, predictions)
                alert_rows = _execute_save_alerts(cursor, run_id, predictions)
                _execute_checkpoint_update(cursor, to_raw_id, max_inserted_at)
                _execute_finish_prediction_run(
                    cursor,
                    run_id,
                    "SUCCESS",
                    raw_count,
                    notes,
                    from_raw_id=from_raw_id,
                    to_raw_id=to_raw_id,
                    checkpoint_after=to_raw_id,
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return processed_rows, prediction_rows, alert_rows


def _select_new_feature_rows(feature_df: pd.DataFrame, new_raw_ids: set[int]) -> pd.DataFrame:
    if feature_df.empty or "raw_id" not in feature_df.columns:
        return pd.DataFrame(columns=feature_df.columns)
    return feature_df[feature_df["raw_id"].astype(int).isin(new_raw_ids)].copy()


def run_prediction_pipeline(run_type: str = "manual", window_cycles: int = PREDICTION_WINDOW_CYCLES) -> dict[str, Any]:
    checkpoint = get_checkpoint()
    checkpoint_before = int(checkpoint.get("last_processed_raw_id") or 0)
    run_id = create_prediction_run(run_type, checkpoint_before=checkpoint_before)

    from_raw_id: int | None = None
    to_raw_id: int | None = None
    raw_count = 0

    try:
        new_raw_df, combined_raw_df, context_rows = load_incremental_raw_data(checkpoint_before, window_cycles)
        raw_count = len(new_raw_df)

        if new_raw_df.empty:
            notes = f"No new raw rows to process; checkpoint={checkpoint_before}; context_cycles={window_cycles}"
            finish_prediction_run(run_id, "SUCCESS", 0, notes, checkpoint_after=checkpoint_before)
            print(f"[prediction] SUCCESS run_id={run_id} {notes}")
            return {
                "run_id": run_id,
                "status": "SUCCESS",
                "raw_rows_used": 0,
                "processed_rows": 0,
                "prediction_rows": 0,
                "alert_rows": 0,
                "checkpoint_before": checkpoint_before,
                "checkpoint_after": checkpoint_before,
            }

        from_raw_id = int(new_raw_df["raw_id"].min())
        to_raw_id = int(new_raw_df["raw_id"].max())
        max_inserted_at = new_raw_df["inserted_at"].max() if "inserted_at" in new_raw_df.columns else None
        new_raw_ids = set(new_raw_df["raw_id"].astype(int).tolist())

        clean_df = clean_raw_dataframe(combined_raw_df)
        feature_df = build_features(clean_df)
        new_feature_df = _select_new_feature_rows(feature_df, new_raw_ids)
        latest_new_df = latest_engine_features(new_feature_df)
        predictions = score_risk(latest_new_df)

        notes_without_counts = (
            f"new_raw_rows={raw_count}; context_rows={context_rows}; "
            f"from_raw_id={from_raw_id}; to_raw_id={to_raw_id}; checkpoint_before={checkpoint_before}; "
            f"checkpoint_after={to_raw_id}; context_cycles={window_cycles}"
        )
        processed_rows, prediction_rows, alert_rows = commit_successful_outputs(
            run_id=run_id,
            new_feature_df=new_feature_df,
            predictions=predictions,
            raw_count=raw_count,
            from_raw_id=from_raw_id,
            to_raw_id=to_raw_id,
            checkpoint_before=checkpoint_before,
            max_inserted_at=max_inserted_at,
            notes=(
                f"processed_rows={len(new_feature_df)}; prediction_rows={len(predictions)}; "
                f"alerts={len(predictions[predictions['risk_score'] >= ALERT_HIGH_THRESHOLD]) if not predictions.empty else 0}; "
                f"{notes_without_counts}"
            ),
        )

        notes = (
            f"processed_rows={processed_rows}; prediction_rows={prediction_rows}; alerts={alert_rows}; "
            f"{notes_without_counts}"
        )
        print(f"[prediction] SUCCESS run_id={run_id} {notes}")
        return {
            "run_id": run_id,
            "status": "SUCCESS",
            "raw_rows_used": raw_count,
            "processed_rows": processed_rows,
            "prediction_rows": prediction_rows,
            "alert_rows": alert_rows,
            "from_raw_id": from_raw_id,
            "to_raw_id": to_raw_id,
            "checkpoint_before": checkpoint_before,
            "checkpoint_after": to_raw_id,
            "context_rows": context_rows,
        }
    except Exception as exc:
        finish_prediction_run(
            run_id,
            "FAILED",
            raw_count,
            str(exc),
            from_raw_id=from_raw_id,
            to_raw_id=to_raw_id,
            checkpoint_after=checkpoint_before,
        )
        print(f"[prediction] FAILED run_id={run_id}: {exc}")
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description="Run incremental batch prediction from raw_sensor_data.")
    parser.add_argument("--run-type", default=os.getenv("PREDICTION_RUN_TYPE", "manual"))
    parser.add_argument(
        "--window-cycles",
        type=int,
        default=PREDICTION_WINDOW_CYCLES,
        help="Number of old cycles per engine/source to read as context for rolling features.",
    )
    args = parser.parse_args()
    run_prediction_pipeline(run_type=args.run_type, window_cycles=args.window_cycles)


if __name__ == "__main__":
    main()
