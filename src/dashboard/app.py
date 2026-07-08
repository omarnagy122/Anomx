from __future__ import annotations

import datetime as dt
from typing import Any

import pandas as pd
import streamlit as st

from prediction.trained_model import run_trained_model_prediction
from prediction.prediction_pipeline import _connect_postgres, _real_dict_cursor

st.set_page_config(page_title="AnomX Dashboard", page_icon="🛠️", layout="wide")

# ---------------------------------------------------------------------------
# Theme tokens (same hex values used in the design mockups / prototypes)
# ---------------------------------------------------------------------------
THEMES: dict[str, dict[str, str]] = {
    "light": {
        "page_bg": "#EDEEF0",
        "card_bg": "#FFFFFF",
        "toolbar_bg": "#DCDDE0",
        "text": "#1F2937",
        "text_secondary": "#6B7280",
        "border": "#D1D5DB",
        "accent": "#185FA5",
        "high": "#A32D2D",
        "medium": "#8A5A0F",
        "low": "#3B6D11",
    },
    "dark": {
        "page_bg": "#12161C",
        "card_bg": "#1B212B",
        "toolbar_bg": "#20242C",
        "text": "#E5E7EB",
        "text_secondary": "#8A909C",
        "border": "#2A313D",
        "accent": "#4A90D9",
        "high": "#D9534F",
        "medium": "#E0A458",
        "low": "#7FB069",
    },
}

_GEAR_PATH = (
    "M54.3 4h-8.6l-1.7 9.4a37 37 0 0 0-8.9 3.7L27 11.4l-6 6 5.7 8.1a37 37 0 0 0-3.7 "
    "8.9L13.6 36v8.6l9.4 1.7a37 37 0 0 0 3.7 8.9L21 63.3l6 6 8.1-5.7a37 37 0 0 0 8.9 "
    "3.7l1.7 9.4h8.6l1.7-9.4a37 37 0 0 0 8.9-3.7l8.1 5.7 6-6-5.7-8.1a37 37 0 0 0 "
    "3.7-8.9l9.4-1.7V36l-9.4-1.7a37 37 0 0 0-3.7-8.9l5.7-8.1-6-6-8.1 5.7a37 37 0 0 "
    "0-8.9-3.7L54.3 4ZM50 62a12 12 0 1 1 0-24 12 12 0 0 1 0 24Z"
)


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


def parse_engine_filter(text: str) -> set[int] | None:
    text = text.strip()
    if not text:
        return None
    ids: set[int] = set()
    for part in text.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            bounds = part.split("-")
            if len(bounds) == 2:
                try:
                    lo, hi = int(bounds[0]), int(bounds[1])
                except ValueError:
                    continue
                lo, hi = min(lo, hi), max(lo, hi)
                ids.update(range(lo, hi + 1))
                continue
        try:
            ids.add(int(part))
        except ValueError:
            continue
    return ids


# ---------------------------------------------------------------------------
# Widget-clearing callback.
#
# IMPORTANT: this must run as an on_click callback, not as code inside the
# `if st.button(...)` block. Streamlit raises StreamlitAPIException if you
# write to st.session_state[key] for a widget that was already instantiated
# earlier in the same script run. on_click runs BEFORE the widgets below it
# are re-created on the rerun, so it is safe there.
# ---------------------------------------------------------------------------
def _clear_prediction_filters() -> None:
    st.session_state["level_filter"] = "ALL"
    st.session_state["id_filter"] = ""


def inject_theme_css(theme: dict[str, str]) -> None:
    st.markdown(
        f"""
        <style>
        @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600&display=swap');

        .stApp {{
            background-color: {theme["page_bg"]} !important;
            color: {theme["text"]} !important;
        }}
        [data-testid="stSidebar"] {{
            background-color: {theme["card_bg"]} !important;
            border-right: 1px solid {theme["border"]} !important;
        }}
        [data-testid="stSidebar"] * {{
            color: {theme["text"]} !important;
        }}
        [data-testid="stSidebar"] [data-baseweb="select"] > div,
        [data-testid="stSidebar"] [data-baseweb="base-input"] {{
            background-color: {theme["card_bg"]} !important;
            border-color: {theme["border"]} !important;
        }}
        .main .block-container {{
            position: relative;
            z-index: 1;
        }}
        h1, h2, h3, h4, p, span, label {{
            color: {theme["text"]} !important;
        }}
        div.stButton > button[kind="primary"] {{
            background-color: {theme["accent"]} !important;
            border-color: {theme["accent"]} !important;
            color: #FFFFFF !important;
            border-radius: 6px !important;
        }}
        div.stButton > button {{
            background-color: {theme["card_bg"]} !important;
            color: {theme["text"]} !important;
            border-color: {theme["border"]} !important;
            border-radius: 6px !important;
        }}
        [data-testid="stDataFrame"] {{
            border: 0.5px solid {theme["border"]} !important;
            border-radius: 6px !important;
        }}
        [data-testid="stJson"] {{
            background-color: {theme["card_bg"]} !important;
            border: 0.5px solid {theme["border"]} !important;
            border-radius: 6px !important;
        }}
        [data-testid="stMetricValue"], .anomx-mono {{
            font-family: 'JetBrains Mono', monospace;
        }}
        .anomx-gear {{
            position: fixed;
            pointer-events: none;
            opacity: 0.08;
            color: {theme["text_secondary"]};
            z-index: 0;
        }}
        .anomx-gear-1 {{ top: -8%; right: -6%; width: 420px; animation: anomx-spin-cw 50s linear infinite; }}
        .anomx-gear-2 {{ bottom: -8%; left: -6%; width: 260px; animation: anomx-spin-ccw 34s linear infinite; }}
        @keyframes anomx-spin-cw {{ to {{ transform: rotate(360deg); }} }}
        @keyframes anomx-spin-ccw {{ to {{ transform: rotate(-360deg); }} }}
        @media (prefers-reduced-motion: reduce) {{
            .anomx-gear-1, .anomx-gear-2 {{ animation: none; }}
        }}
        </style>
        <svg class="anomx-gear anomx-gear-1" viewBox="0 0 100 100" fill="currentColor"
             xmlns="http://www.w3.org/2000/svg" aria-hidden="true">
          <path d="{_GEAR_PATH}" />
        </svg>
        <svg class="anomx-gear anomx-gear-2" viewBox="0 0 100 100" fill="currentColor"
             xmlns="http://www.w3.org/2000/svg" aria-hidden="true">
          <path d="{_GEAR_PATH}" />
        </svg>
        """,
        unsafe_allow_html=True,
    )


def render_theme_toggle() -> dict[str, str]:
    st.session_state.setdefault("theme", "dark")
    is_dark = st.sidebar.toggle("Dark mode", value=(st.session_state["theme"] == "dark"))
    st.session_state["theme"] = "dark" if is_dark else "light"
    return THEMES[st.session_state["theme"]]


def build_risk_donut_svg(counts: dict[str, int], theme: dict[str, str]) -> str:
    order = ["HIGH", "MEDIUM", "LOW"]
    total = sum(counts.get(level, 0) for level in order) or 1
    radius = 60
    circumference = 2 * 3.14159265 * radius
    offset_acc = 0.0
    segments = []
    for level in order:
        fraction = counts.get(level, 0) / total
        dash = fraction * circumference
        segments.append((level, dash, circumference - dash, -offset_acc))
        offset_acc += dash

    color_map = {"HIGH": theme["high"], "MEDIUM": theme["medium"], "LOW": theme["low"]}
    circles = "".join(
        f'<circle cx="80" cy="80" r="{radius}" fill="none" stroke="{color_map[level]}" '
        f'stroke-width="16" stroke-dasharray="{dash:.3f} {gap:.3f}" stroke-dashoffset="{offset:.3f}" '
        f'transform="rotate(-90 80 80)" stroke-linecap="butt" />'
        for level, dash, gap, offset in segments
    )
    return (
        f'<svg width="160" height="160" viewBox="0 0 160 160">'
        f'<circle cx="80" cy="80" r="{radius}" fill="none" stroke="{theme["border"]}" stroke-width="16" />'
        f'{circles}</svg>'
    )


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


def render_metrics(theme: dict[str, str]) -> None:
    metrics = [
        ("Raw rows", "SELECT COUNT(*) FROM raw_sensor_data;", "accent"),
        ("Processed rows", "SELECT COUNT(*) FROM processed_sensor_data;", "accent"),
        ("Prediction results", "SELECT COUNT(*) FROM prediction_results;", "accent"),
        ("Active alerts", "SELECT COUNT(*) FROM alerts WHERE is_resolved = FALSE;", "high"),
    ]
    cols = st.columns(len(metrics))
    for col, (label, sql, color_key) in zip(cols, metrics):
        value = fetch_one_value(sql)
        value_color = theme["high"] if color_key == "high" else theme["text"]
        col.markdown(
            f"""
            <div style="background:{theme['card_bg']};border-radius:6px;
                        border-left:3px solid {theme[color_key]};padding:10px 12px;">
              <p style="font-size:11px;color:{theme['text_secondary']};margin:0 0 5px;">{label}</p>
              <p class="anomx-mono" style="font-size:20px;font-weight:600;margin:0;color:{value_color};">
                {value:,}
              </p>
            </div>
            """,
            unsafe_allow_html=True,
        )


def render_risk_distribution(theme: dict[str, str]) -> None:
    st.subheader("Engines by risk level")
    latest = fetch_dataframe(
        """
        SELECT DISTINCT ON (engine_id) engine_id, risk_level
        FROM prediction_results
        ORDER BY engine_id, prediction_time DESC, id DESC;
        """
    )
    if latest.empty or "risk_level" not in latest:
        st.caption("No prediction results yet. Run a prediction first.")
        return

    counts = latest["risk_level"].value_counts().to_dict()
    total = int(sum(counts.values()))
    svg = build_risk_donut_svg(counts, theme)

    col_chart, col_legend = st.columns([1, 1.3])
    with col_chart:
        st.markdown(
            f'<div style="background:{theme["card_bg"]};border-radius:6px;padding:12px;'
            f'display:flex;justify-content:center;">{svg}</div>',
            unsafe_allow_html=True,
        )
    with col_legend:
        for level, color_key in [("HIGH", "high"), ("MEDIUM", "medium"), ("LOW", "low")]:
            count = int(counts.get(level, 0))
            pct = (count / total * 100) if total else 0.0
            st.markdown(
                f"""
                <div style="display:flex;align-items:center;justify-content:space-between;
                            padding:6px 0;border-bottom:0.5px solid {theme['border']};
                            font-size:13px;color:{theme['text']};">
                  <span style="display:flex;align-items:center;gap:6px;">
                    <span style="width:9px;height:9px;border-radius:2px;background:{theme[color_key]};"></span>
                    {level.title()}
                  </span>
                  <span class="anomx-mono" style="color:{theme['text_secondary']};">
                    {count} engines, {pct:.1f}%
                  </span>
                </div>
                """,
                unsafe_allow_html=True,
            )
        st.markdown(
            f'<p style="font-size:11px;color:{theme["text_secondary"]};margin-top:10px;">'
            "Threshold: HIGH &ge; 0.80, MEDIUM &ge; 0.50, else LOW</p>",
            unsafe_allow_html=True,
        )


def style_dataframe(
    df: pd.DataFrame,
    theme: dict[str, str],
    *,
    color_col: str | None = None,
    color_map: dict[str, str] | None = None,
    mono_cols: list[str] | None = None,
):
    styler = df.style
    if color_col and color_map and color_col in df.columns:
        def _color(value: Any) -> str:
            hex_color = color_map.get(str(value))
            return f"color: {hex_color}; font-weight: 600;" if hex_color else ""

        styler = styler.map(_color, subset=[color_col])
    existing_mono = [c for c in (mono_cols or []) if c in df.columns]
    if existing_mono:
        styler = styler.set_properties(subset=existing_mono, **{"font-family": "monospace"})
    return styler


def render_engine_deep_dive(theme: dict[str, str]) -> None:
    st.subheader("Engine Deep-Dive Analysis")

    engine_id_input = st.number_input("Enter Engine ID to analyze", min_value=1, step=1, value=1)

    history = fetch_dataframe(
        """
        SELECT prediction_time, risk_score, risk_level, latest_cycle, recommended_action
        FROM prediction_results
        WHERE engine_id = %s
        ORDER BY prediction_time ASC;
        """,
        (int(engine_id_input),),
    )

    if history.empty:
        st.warning(f"No prediction history found for Engine ID {engine_id_input}.")
        return

    col1, col2 = st.columns([2, 1])
    with col1:
        st.markdown("**Risk Score Trajectory**")
        chart_data = history.copy()
        chart_data = chart_data.set_index("prediction_time")
        st.line_chart(chart_data["risk_score"])

    with col2:
        latest_status = history.iloc[-1]
        st.markdown(f"**Latest Status for Engine {engine_id_input}**")
        st.metric("Current Risk Score", f"{latest_status['risk_score']:.2f}")
        st.markdown(f"**Risk Level:** `{latest_status['risk_level']}`")
        st.markdown(f"**Latest Cycle:** `{latest_status['latest_cycle']}`")
        st.markdown(f"**Recommendation:** {latest_status['recommended_action']}")

    st.markdown("**Historical Predictions List**")
    st.dataframe(
        history.sort_values(by="prediction_time", ascending=False),
        use_container_width=True,
        hide_index=True,
    )


def render_tables(theme: dict[str, str]) -> None:
    st.subheader("Latest predictions")

    filter_cols = st.columns([1, 2, 1])
    with filter_cols[0]:
        level_filter = st.selectbox("Risk level", ["ALL", "HIGH", "MEDIUM", "LOW"], key="level_filter")
    with filter_cols[1]:
        id_filter_text = st.text_input(
            "Engine ID or range", placeholder="e.g. 47 or 40-90", key="id_filter"
        )
    with filter_cols[2]:
        st.write("")
        # Fixed: on_click runs before the widgets above are re-instantiated,
        # so writing to their session_state keys here is safe. Writing to
        # them directly inside `if st.button(...):` raised
        # StreamlitAPIException in the previous version.
        st.button("Clear filters", use_container_width=True, on_click=_clear_prediction_filters)

    predictions = fetch_dataframe(
        """
        SELECT pr.id, pr.run_id, pr.model_version, pr.source_file, pr.engine_id,
               pr.latest_cycle, pr.risk_score, pr.risk_level, pr.recommended_action,
               pr.prediction_time
        FROM prediction_results pr
        ORDER BY pr.prediction_time DESC, pr.id DESC
        LIMIT 200;
        """
    )
    total_count = len(predictions)

    if not predictions.empty:
        if level_filter != "ALL" and "risk_level" in predictions:
            predictions = predictions[predictions["risk_level"] == level_filter]
        id_set = parse_engine_filter(id_filter_text)
        if id_set is not None and "engine_id" in predictions:
            predictions = predictions[predictions["engine_id"].isin(id_set)]

    st.caption(f"showing {len(predictions)} of {total_count} rows")

    if predictions.empty:
        st.info("No predictions match this filter.")
    else:
        risk_colors = {"HIGH": theme["high"], "MEDIUM": theme["medium"], "LOW": theme["low"]}
        st.dataframe(
            style_dataframe(
                predictions,
                theme,
                color_col="risk_level",
                color_map=risk_colors,
                mono_cols=["id", "run_id", "engine_id", "latest_cycle"],
            ),
            use_container_width=True,
            hide_index=True,
            column_config={
                "risk_score": st.column_config.ProgressColumn(
                    "Risk score",
                    help="Model probability, 0 to 1. HIGH >= 0.80, MEDIUM >= 0.50",
                    format="%.2f",
                    min_value=0.0,
                    max_value=1.0,
                ),
            },
        )
        st.download_button(
            "Export filtered predictions to CSV",
            data=predictions.to_csv(index=False).encode("utf-8"),
            file_name=f"anomx_predictions_{dt.datetime.now():%Y%m%d_%H%M%S}.csv",
            mime="text/csv",
        )

    left, right = st.columns(2)
    with left:
        st.subheader("Latest prediction runs")
        runs = fetch_dataframe(
            """
            SELECT run_id, run_type, status, raw_rows_used, started_at, finished_at, notes
            FROM prediction_runs
            ORDER BY run_id DESC
            LIMIT 10;
            """
        )
        status_colors = {"SUCCESS": theme["low"], "FAILED": theme["high"]}
        st.dataframe(
            style_dataframe(
                runs,
                theme,
                color_col="status",
                color_map=status_colors,
                mono_cols=["run_id", "raw_rows_used"],
            ),
            use_container_width=True,
            hide_index=True,
        )

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
        severity_colors = {"HIGH": theme["high"], "MEDIUM": theme["medium"], "LOW": theme["low"]}
        st.dataframe(
            style_dataframe(
                alerts,
                theme,
                color_col="severity",
                color_map=severity_colors,
                mono_cols=["id", "run_id", "engine_id"],
            ),
            use_container_width=True,
            hide_index=True,
        )

        if not alerts.empty:
            st.markdown("---")
            alert_ids = alerts["id"].tolist()
            selected_alert = st.selectbox("Select Alert ID to Action", alert_ids)
            if st.button("Mark Selected Alert as Resolved", type="secondary"):
                execute_sql("UPDATE alerts SET is_resolved = TRUE WHERE id = %s;", (int(selected_alert),))
                st.success(f"Alert #{selected_alert} resolved successfully!")
                st.rerun()


def main() -> None:
    theme = render_theme_toggle()
    inject_theme_css(theme)
    render_scheduler_sidebar()

    st.title("AnomX Predictive Maintenance Dashboard")
    st.caption("kafka -> postgresql -> xgboost -> prediction_results / alerts")

    if st.button("Run Prediction", type="primary", use_container_width=False):
        with st.spinner("Running trained model against processed_sensor_data..."):
            try:
                result = run_trained_model_prediction(run_type="dashboard_manual")
                st.success(
                    f"Prediction run #{result.get('run_id')} finished: "
                    f"{result.get('prediction_rows', 0)} predictions, {result.get('alert_rows', 0)} alerts."
                )
                with st.expander("Run details"):
                    st.json({k: v for k, v in result.items() if k != "model_features"})
            except Exception as exc:  # pragma: no cover - shown to operator in Streamlit
                st.error(f"Prediction failed: {exc}")

    tab_overview, tab_deep_dive, tab_data_logs = st.tabs(
        ["📊 Executive Overview", "🔍 Engine Deep-Dive", "📋 Predictions & Alerts"]
    )

    with tab_overview:
        render_metrics(theme)
        render_risk_distribution(theme)

    with tab_deep_dive:
        render_engine_deep_dive(theme)

    with tab_data_logs:
        render_tables(theme)


if __name__ == "__main__":
    main()
