"""Quick local smoke demo for the feature/prediction logic without Kafka/PostgreSQL.

If C-MAPSS files are not present, the script creates a tiny synthetic sample so
this repository can stay lightweight with an empty data/raw/CMAPSSData folder.
"""
from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from config import SENSOR_COLUMNS, dataset_path
from prediction.features import build_features, clean_raw_dataframe, latest_engine_features, score_risk


def _synthetic_raw(rows_per_engine: int = 50, engines: int = 3) -> pd.DataFrame:
    rows = []
    for engine_id in range(1, engines + 1):
        for cycle in range(1, rows_per_engine + 1):
            row = {"raw_id": len(rows) + 1, "source_file": "SYNTHETIC", "engine_id": engine_id, "time_in_cycles": cycle}
            for column in SENSOR_COLUMNS:
                if column in ("engine_id", "time_in_cycles"):
                    continue
                row[column] = float(engine_id + cycle / 10)
            rows.append(row)
    return pd.DataFrame(rows)


def main() -> None:
    path = dataset_path("FD001")
    if path.exists():
        raw = pd.read_csv(path, sep=r"\s+", header=None, names=SENSOR_COLUMNS).head(500)
        raw.insert(0, "source_file", "FD001")
        raw.insert(0, "raw_id", range(1, len(raw) + 1))
    else:
        raw = _synthetic_raw()

    clean = clean_raw_dataframe(raw)
    features = build_features(clean)
    predictions = score_risk(latest_engine_features(features))
    print(predictions[["source_file", "engine_id", "time_in_cycles", "risk_score", "risk_level", "recommended_action"]].head(10))
    print(f"raw_rows={len(raw)}, feature_rows={len(features)}, prediction_rows={len(predictions)}")


if __name__ == "__main__":
    main()
