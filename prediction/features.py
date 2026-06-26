from __future__ import annotations

import math

import pandas as pd

from config import FEATURE_SENSOR_COLUMNS, SENSOR_COLUMNS, USELESS_SENSOR_COLUMNS

METADATA_COLUMNS = ["raw_id", "inserted_at"]
BASE_COLUMNS = ["source_file", *SENSOR_COLUMNS]
SELECTED_SENSOR_COLUMNS = [column for column in SENSOR_COLUMNS if column not in USELESS_SENSOR_COLUMNS]
PROCESSED_COLUMNS = ["raw_id", "source_file", *SELECTED_SENSOR_COLUMNS]


def clean_raw_dataframe(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Clean a selected raw batch/window only, not the continuous ingestion stream.

    The optional ``raw_id`` column is preserved when the input came from PostgreSQL.
    This is what lets the batch pipeline process only rows that arrived after the
    last successful checkpoint.
    """
    if raw_df.empty:
        return raw_df.copy()

    df = raw_df.copy()
    optional_metadata = [column for column in METADATA_COLUMNS if column in df.columns]
    requested_columns = [*optional_metadata, *BASE_COLUMNS]

    for column in requested_columns:
        if column not in df.columns:
            df[column] = None

    df = df[requested_columns]

    if "raw_id" in df.columns:
        df["raw_id"] = pd.to_numeric(df["raw_id"], errors="coerce")

    df["source_file"] = df["source_file"].fillna("UNKNOWN").astype(str)
    df["engine_id"] = pd.to_numeric(df["engine_id"], errors="coerce")
    df["time_in_cycles"] = pd.to_numeric(df["time_in_cycles"], errors="coerce")

    required_columns = ["engine_id", "time_in_cycles"]
    if "raw_id" in df.columns:
        required_columns.append("raw_id")
    df = df.dropna(subset=required_columns)

    if "raw_id" in df.columns:
        df["raw_id"] = df["raw_id"].astype(int)
    df["engine_id"] = df["engine_id"].astype(int)
    df["time_in_cycles"] = df["time_in_cycles"].astype(int)

    numeric_columns = [column for column in SENSOR_COLUMNS if column not in ("engine_id", "time_in_cycles")]
    for column in numeric_columns:
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0.0)

    if "raw_id" in df.columns:
        df = df.drop_duplicates(subset=["raw_id"])
    else:
        df = df.drop_duplicates(subset=["source_file", "engine_id", "time_in_cycles"])

    sort_columns = ["source_file", "engine_id", "time_in_cycles"]
    if "raw_id" in df.columns:
        sort_columns.append("raw_id")
    df = df.sort_values(sort_columns).reset_index(drop=True)
    return df


def build_features(clean_df: pd.DataFrame, rolling_window: int = 5) -> pd.DataFrame:
    """Build ML-ready features from the selected incremental batch plus context."""
    if clean_df.empty:
        return clean_df.copy()

    df = clean_df.copy()
    group_cols = ["source_file", "engine_id"]

    for sensor in FEATURE_SENSOR_COLUMNS:
        df[f"rolling_avg_{sensor}"] = (
            df.groupby(group_cols)[sensor]
            .transform(lambda s: s.rolling(rolling_window, min_periods=1).mean())
            .fillna(0.0)
        )
        df[f"rolling_std_{sensor}"] = (
            df.groupby(group_cols)[sensor]
            .transform(lambda s: s.rolling(rolling_window, min_periods=1).std())
            .fillna(0.0)
        )
        df[f"delta_{sensor}"] = (
            df.groupby(group_cols)[sensor]
            .diff()
            .fillna(0.0)
        )

    # Demo RUL for historical C-MAPSS training windows only. In a true live factory
    # flow this can be replaced by a trained model output.
    max_cycle = df.groupby(group_cols)["time_in_cycles"].transform("max")
    df["demo_rul"] = (max_cycle - df["time_in_cycles"]).astype(int)

    return df.drop(columns=[c for c in USELESS_SENSOR_COLUMNS if c in df.columns]).fillna(0.0)


def latest_engine_features(feature_df: pd.DataFrame) -> pd.DataFrame:
    """Keep the latest row per engine/source for prediction output."""
    if feature_df.empty:
        return feature_df.copy()

    sort_columns = ["time_in_cycles"]
    if "raw_id" in feature_df.columns:
        sort_columns.append("raw_id")

    ordered = feature_df.sort_values(["source_file", "engine_id", *sort_columns])
    latest = ordered.groupby(["source_file", "engine_id"], as_index=False).tail(1)
    return latest.sort_values(["source_file", "engine_id"]).reset_index(drop=True)


def score_risk(latest_features: pd.DataFrame) -> pd.DataFrame:
    """Deterministic placeholder risk scorer.

    The real ML model can replace this function later. For now it gives a stable
    end-to-end prediction output from raw sensor history without doing work inside
    the real-time consumer.
    """
    if latest_features.empty:
        return latest_features.copy()

    df = latest_features.copy()
    delta_cols = [f"delta_{sensor}" for sensor in FEATURE_SENSOR_COLUMNS if f"delta_{sensor}" in df.columns]
    std_cols = [f"rolling_std_{sensor}" for sensor in FEATURE_SENSOR_COLUMNS if f"rolling_std_{sensor}" in df.columns]

    if delta_cols:
        delta_signal = df[delta_cols].abs().mean(axis=1)
    else:
        delta_signal = pd.Series([0.0] * len(df), index=df.index)

    if std_cols:
        std_signal = df[std_cols].abs().mean(axis=1)
    else:
        std_signal = pd.Series([0.0] * len(df), index=df.index)

    # Normalize within this run to produce a 0..100 score. This is not a trained
    # industrial risk model; it is a safe deterministic placeholder for the pipeline.
    raw_signal = (delta_signal * 2.0) + std_signal
    max_signal = float(raw_signal.max()) if len(raw_signal) else 0.0
    if math.isclose(max_signal, 0.0):
        risk_score = pd.Series([0.0] * len(df), index=df.index)
    else:
        risk_score = (raw_signal / max_signal * 100.0).clip(0.0, 100.0)

    df["risk_score"] = risk_score.round(2)
    df["predicted_rul"] = (100.0 - df["risk_score"]).clip(lower=0.0).round(2)
    df["risk_level"] = df["risk_score"].apply(_risk_level)
    df["recommended_action"] = df["risk_level"].apply(_recommendation)
    return df


def _risk_level(score: float) -> str:
    if score >= 90:
        return "CRITICAL"
    if score >= 70:
        return "HIGH"
    if score >= 40:
        return "MEDIUM"
    return "LOW"


def _recommendation(level: str) -> str:
    return {
        "CRITICAL": "Stop machine and inspect before operation.",
        "HIGH": "Schedule urgent inspection during the nearest safe stop.",
        "MEDIUM": "Monitor during the next prediction window.",
        "LOW": "Continue normal operation.",
    }[level]
