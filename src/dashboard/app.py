from __future__ import annotations

import datetime as dt
from typing import Any

import pandas as pd
import streamlit as st

from prediction.trained_model import run_trained_model_prediction
from prediction.prediction_pipeline import _connect_postgres, _real_dict_cursor

st.set_page_config(page_title="AnomX Dashboard", page_icon="🛠️", layout="wide")


def fetch_one_value(sql: str, default: Any = 0) -> Any:
    with _connect_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            row = cursor.fetchone()
    return row[0] if row else default


def fetch_dataframe(sql: str, params: tuple[Any, ...] = ()) -> pd.DataFrame:
    with _connect_postgres() as conn:
        with conn.cursor(cursor_factory=_real_dict_cursor()) as cursor:
            cursor.execute(sql, params)
            rows = cursor.fetchall()
    return pd.DataFrame(rows)


def execute_sql(sql: str, params: tuple[Any, ...] = ()) -> None:
    with _connect_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, params)
        conn.commit()


def format_probability(value: Any) -> str:
    try:
        probability = float(value)
    except (TypeError, ValueError):
        return "-"
    if probability <= 1.0:
        return f"{probability:.2%}"
    return f"{probability:.2f}"


def schedule_options() -> tuple[list[str], list[str]]:
    display: list[str] = []
    values: list[str] = []
    start = dt.datetime.strptime("00:00", "%H:%M")
    for index in range(48):
        current = start + dt.timedelta(minutes=30 * index)
        display.append(current.strftime("%I:%M %p"))
        values.append(current.strftime("%H:%M"))
    return display, values


def render_scheduler_sidebar() -> None:
    st.sidebar.header("Scheduler Settings")
    time_options_display, time_options_values = schedule_options()

    current = fetch_dataframe("SELECT time_1, time_2, time_3 FROM schedule_settings WHERE id = 1;")
    if current.empty:
        current_values = ["07:00", "12:00", "17:00"]
    else:
        current_values = [current.iloc[0]["time_1"], current.iloc[0]["time_2"], current.iloc[0]["time_3"]]

    indexes = [time_options_values.index(value) if value in time_options_values else 0 for value in current_values]
    selected_display = [
        st.sidebar.selectbox("First Run Time", time_options_display, index=indexes[0]),
        st.sidebar.selectbox("Second Run Time", time_options_display, index=indexes[1]),
        st.sidebar.selectbox("Third Run Time", time_options_display, index=indexes[2]),
    ]
    selected_values = [time_options_values[time_options_display.index(value)] for value in selected_display]

    if st.sidebar.button("Save Schedule", use_container_width=True):
        execute_sql(
            """
            UPDATE schedule_settings
            SET time_1 = %s, time_2 = %s, time_3 = %s, updated_at = NOW()
            WHERE id = 1;
            """,
            tuple(selected_values),
        )
        st.sidebar.success("Schedule saved.")


def render_metrics() -> None:
    metrics = {
        "Raw rows": "SELECT COUNT(*) FROM raw_sensor_data;",
        "Processed rows": "SELECT COUNT(*) FROM processed_sensor_data;",
        "Prediction results": "SELECT COUNT(*) FROM prediction_results;",
        "Active alerts": "SELECT COUNT(*) FROM alerts WHERE is_resolved = FALSE;",
    }
    cols = st.columns(len(metrics))
    for col, (label, sql) in zip(cols, metrics.items()):
        col.metric(label, f"{fetch_one_value(sql):,}")


def render_tables() -> None:
    st.subheader("Latest prediction runs")
    runs = fetch_dataframe(
        """
        SELECT run_id, run_type, status, raw_rows_used, started_at, finished_at, notes
        FROM prediction_runs
        ORDER BY run_id DESC
        LIMIT 10;
        """
    )
    st.dataframe(runs, use_container_width=True, hide_index=True)

    left, right = st.columns(2)
    with left:
        st.subheader("Latest predictions")
        predictions = fetch_dataframe(
            """
            SELECT pr.id, pr.run_id, pr.model_version, pr.source_file, pr.engine_id,
                   pr.latest_cycle, pr.risk_score, pr.risk_level, pr.recommended_action,
                   pr.prediction_time
            FROM prediction_results pr
            ORDER BY pr.prediction_time DESC, pr.id DESC
            LIMIT 25;
            """
        )
        if not predictions.empty and "risk_score" in predictions:
            predictions["risk_score"] = predictions["risk_score"].apply(format_probability)
        st.dataframe(predictions, use_container_width=True, hide_index=True)

    with right:
        st.subheader("Active alerts")
        alerts = fetch_dataframe(
            """
            SELECT id, run_id, model_version, source_file, engine_id, severity,
                   message, created_at
            FROM alerts
            WHERE is_resolved = FALSE
            ORDER BY created_at DESC, id DESC
            LIMIT 25;
            """
        )
        st.dataframe(alerts, use_container_width=True, hide_index=True)


def main() -> None:
    render_scheduler_sidebar()

    st.title("AnomX Predictive Maintenance Dashboard")
    st.caption("Dashboard → PostgreSQL → trained XGBoost model → prediction_results / alerts → dashboard display")

    if st.button("Run Prediction", type="primary", use_container_width=False):
        with st.spinner("Running trained model against processed_sensor_data..."):
            try:
                result = run_trained_model_prediction(run_type="dashboard_manual")
                st.success(
                    f"Prediction run #{result.get('run_id')} finished: "
                    f"{result.get('prediction_rows', 0)} predictions, {result.get('alert_rows', 0)} alerts."
                )
                st.json({k: v for k, v in result.items() if k != "model_features"})
            except Exception as exc:  # pragma: no cover - shown to operator in Streamlit
                st.error(f"Prediction failed: {exc}")

    render_metrics()
    render_tables()


if __name__ == "__main__":
    main()
