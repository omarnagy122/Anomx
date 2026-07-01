# Trained Model + Dashboard Integration

This handoff connects the final delivery path:

```text
Streamlit Dashboard -> PostgreSQL -> trained XGBoost model -> prediction_results / alerts -> Streamlit Dashboard
```

## What changed

- The dashboard runs from `src/dashboard/app.py` on port `8501`.
- The real model is loaded from `model/xgboost_predictive_model.pkl` through `MODEL_PATH`.
- The integration code lives in `src/prediction/trained_model.py`.
- The model reads the latest row per `source_file` / `engine_id` from `processed_sensor_data`.
- Results are written to the existing tables only:
  - `prediction_runs`
  - `prediction_results`
  - `alerts`
- No new prediction tables are created.
- The old `model/app.py` is now only a compatibility wrapper for the new dashboard.
- The old `model/batch_inference.py` is now only a compatibility wrapper for the integrated trained-model runner.

## Model features

The model exposes 45 named input features via `feature_names_in_`:

```text
time_in_cycles,
op_setting_1, op_setting_2, op_setting_3,
s2, s3, s4, s7, s8, s9, s11, s12, s13, s14, s15, s17, s20, s21,
rolling_avg_s2, rolling_std_s2, delta_s2,
rolling_avg_s3, rolling_std_s3, delta_s3,
rolling_avg_s4, rolling_std_s4, delta_s4,
rolling_avg_s7, rolling_std_s7, delta_s7,
rolling_avg_s11, rolling_std_s11, delta_s11,
rolling_avg_s12, rolling_std_s12, delta_s12,
rolling_avg_s15, rolling_std_s15, delta_s15,
rolling_avg_s20, rolling_std_s20, delta_s20,
rolling_avg_s21, rolling_std_s21, delta_s21
```

All of these columns already exist in `processed_sensor_data`.

## Risk mapping

The trained model returns a failure probability. The integration stores that probability as `risk_score`:

- `risk_score >= 0.80` -> `HIGH`
- `risk_score >= 0.50` -> `MEDIUM`
- otherwise -> `LOW`

Alerts are created for `HIGH` predictions only.

## Docker run

```powershell
docker compose up -d --build
```

Open:

```text
http://localhost:8501
```

## Manual trained-model command

```powershell
docker compose run --rm prediction
```

or directly:

```powershell
docker compose run --rm prediction --run-type manual_trained_model --use-trained-model
```

## Verification SQL

```powershell
docker compose exec postgres psql -U anomx -d anomx_db -c "SELECT COUNT(*) FROM prediction_results;"
docker compose exec postgres psql -U anomx -d anomx_db -c "SELECT COUNT(*) FROM alerts;"
docker compose exec postgres psql -U anomx -d anomx_db -c "SELECT run_id, run_type, status, raw_rows_used, notes FROM prediction_runs ORDER BY run_id DESC LIMIT 5;"
```
