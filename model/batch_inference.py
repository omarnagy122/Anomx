from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = PROJECT_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from prediction.trained_model import is_scheduled_time, run_trained_model_prediction  # noqa: E402


def run_pipeline(respect_schedule: bool = True) -> dict:
    """Compatibility wrapper for the integrated trained-model pipeline.

    The real implementation now lives under src/prediction and writes to the
    existing prediction_results, alerts, and prediction_runs tables.
    """
    if respect_schedule and not is_scheduled_time():
        print("Skipping trained model run; current time is not in schedule_settings.")
        return {"status": "SKIPPED"}

    return run_trained_model_prediction(run_type="batch_inference_trained_model")


if __name__ == "__main__":
    run_pipeline()
