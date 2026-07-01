CREATE TABLE IF NOT EXISTS raw_sensor_data (
    id BIGSERIAL PRIMARY KEY,
    source_file TEXT NOT NULL DEFAULT 'UNKNOWN',
    engine_id INTEGER NOT NULL,
    time_in_cycles INTEGER NOT NULL,
    op_setting_1 DOUBLE PRECISION,
    op_setting_2 DOUBLE PRECISION,
    op_setting_3 DOUBLE PRECISION,
    s1 DOUBLE PRECISION,
    s2 DOUBLE PRECISION,
    s3 DOUBLE PRECISION,
    s4 DOUBLE PRECISION,
    s5 DOUBLE PRECISION,
    s6 DOUBLE PRECISION,
    s7 DOUBLE PRECISION,
    s8 DOUBLE PRECISION,
    s9 DOUBLE PRECISION,
    s10 DOUBLE PRECISION,
    s11 DOUBLE PRECISION,
    s12 DOUBLE PRECISION,
    s13 DOUBLE PRECISION,
    s14 DOUBLE PRECISION,
    s15 DOUBLE PRECISION,
    s16 DOUBLE PRECISION,
    s17 DOUBLE PRECISION,
    s18 DOUBLE PRECISION,
    s19 DOUBLE PRECISION,
    s20 DOUBLE PRECISION,
    s21 DOUBLE PRECISION,
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT raw_sensor_data_unique_row UNIQUE (source_file, engine_id, time_in_cycles)
);

CREATE INDEX IF NOT EXISTS idx_raw_sensor_data_source_engine_cycle
    ON raw_sensor_data (source_file, engine_id, time_in_cycles);

CREATE INDEX IF NOT EXISTS idx_raw_sensor_data_inserted_at
    ON raw_sensor_data (inserted_at);

CREATE INDEX IF NOT EXISTS idx_raw_sensor_data_id
    ON raw_sensor_data (id);

CREATE TABLE IF NOT EXISTS processing_checkpoints (
    pipeline_name TEXT PRIMARY KEY,
    last_processed_raw_id BIGINT NOT NULL DEFAULT 0,
    last_processed_inserted_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO processing_checkpoints (pipeline_name, last_processed_raw_id)
VALUES ('prediction_pipeline', 0)
ON CONFLICT (pipeline_name) DO NOTHING;

CREATE TABLE IF NOT EXISTS processed_sensor_data (
    id BIGSERIAL PRIMARY KEY,
    raw_id BIGINT NOT NULL REFERENCES raw_sensor_data(id) ON DELETE CASCADE,
    source_file TEXT NOT NULL DEFAULT 'UNKNOWN',
    engine_id INTEGER NOT NULL,
    time_in_cycles INTEGER NOT NULL,
    op_setting_1 DOUBLE PRECISION,
    op_setting_2 DOUBLE PRECISION,
    op_setting_3 DOUBLE PRECISION,
    s2 DOUBLE PRECISION,
    s3 DOUBLE PRECISION,
    s4 DOUBLE PRECISION,
    s7 DOUBLE PRECISION,
    s8 DOUBLE PRECISION,
    s9 DOUBLE PRECISION,
    s11 DOUBLE PRECISION,
    s12 DOUBLE PRECISION,
    s13 DOUBLE PRECISION,
    s14 DOUBLE PRECISION,
    s15 DOUBLE PRECISION,
    s17 DOUBLE PRECISION,
    s20 DOUBLE PRECISION,
    s21 DOUBLE PRECISION,
    rolling_avg_s2 DOUBLE PRECISION,
    rolling_std_s2 DOUBLE PRECISION,
    delta_s2 DOUBLE PRECISION,
    rolling_avg_s3 DOUBLE PRECISION,
    rolling_std_s3 DOUBLE PRECISION,
    delta_s3 DOUBLE PRECISION,
    rolling_avg_s4 DOUBLE PRECISION,
    rolling_std_s4 DOUBLE PRECISION,
    delta_s4 DOUBLE PRECISION,
    rolling_avg_s7 DOUBLE PRECISION,
    rolling_std_s7 DOUBLE PRECISION,
    delta_s7 DOUBLE PRECISION,
    rolling_avg_s11 DOUBLE PRECISION,
    rolling_std_s11 DOUBLE PRECISION,
    delta_s11 DOUBLE PRECISION,
    rolling_avg_s12 DOUBLE PRECISION,
    rolling_std_s12 DOUBLE PRECISION,
    delta_s12 DOUBLE PRECISION,
    rolling_avg_s15 DOUBLE PRECISION,
    rolling_std_s15 DOUBLE PRECISION,
    delta_s15 DOUBLE PRECISION,
    rolling_avg_s20 DOUBLE PRECISION,
    rolling_std_s20 DOUBLE PRECISION,
    delta_s20 DOUBLE PRECISION,
    rolling_avg_s21 DOUBLE PRECISION,
    rolling_std_s21 DOUBLE PRECISION,
    delta_s21 DOUBLE PRECISION,
    demo_rul INTEGER,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT processed_sensor_data_unique_raw UNIQUE (raw_id),
    CONSTRAINT processed_sensor_data_unique_row UNIQUE (source_file, engine_id, time_in_cycles)
);

CREATE TABLE IF NOT EXISTS prediction_runs (
    run_id BIGSERIAL PRIMARY KEY,
    run_type TEXT NOT NULL,
    scheduled_time TIMESTAMPTZ,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    status TEXT NOT NULL DEFAULT 'RUNNING',
    raw_rows_used INTEGER NOT NULL DEFAULT 0,
    from_raw_id BIGINT,
    to_raw_id BIGINT,
    checkpoint_before BIGINT,
    checkpoint_after BIGINT,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS prediction_results (
    id BIGSERIAL PRIMARY KEY,
    run_id BIGINT NOT NULL REFERENCES prediction_runs(run_id) ON DELETE CASCADE,
    raw_id BIGINT NOT NULL REFERENCES raw_sensor_data(id) ON DELETE CASCADE,
    model_version TEXT NOT NULL DEFAULT 'demo-v1',
    source_file TEXT NOT NULL DEFAULT 'UNKNOWN',
    engine_id INTEGER NOT NULL,
    prediction_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    latest_cycle INTEGER NOT NULL,
    risk_score DOUBLE PRECISION NOT NULL,
    predicted_rul DOUBLE PRECISION,
    risk_level TEXT NOT NULL,
    recommended_action TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT prediction_results_unique_raw_model UNIQUE (raw_id, model_version)
);

CREATE INDEX IF NOT EXISTS idx_prediction_results_run_id
    ON prediction_results (run_id);

CREATE INDEX IF NOT EXISTS idx_prediction_results_engine
    ON prediction_results (source_file, engine_id, latest_cycle);

CREATE TABLE IF NOT EXISTS alerts (
    id BIGSERIAL PRIMARY KEY,
    run_id BIGINT NOT NULL REFERENCES prediction_runs(run_id) ON DELETE CASCADE,
    raw_id BIGINT NOT NULL REFERENCES raw_sensor_data(id) ON DELETE CASCADE,
    model_version TEXT NOT NULL DEFAULT 'demo-v1',
    source_file TEXT NOT NULL DEFAULT 'UNKNOWN',
    engine_id INTEGER NOT NULL,
    severity TEXT NOT NULL,
    message TEXT NOT NULL,
    is_resolved BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT alerts_unique_raw_model UNIQUE (raw_id, model_version)
);

CREATE INDEX IF NOT EXISTS idx_alerts_run_id
    ON alerts (run_id);

CREATE TABLE IF NOT EXISTS schedule_settings (
    id SERIAL PRIMARY KEY,
    time_1 VARCHAR(5) NOT NULL DEFAULT '07:00',
    time_2 VARCHAR(5) NOT NULL DEFAULT '12:00',
    time_3 VARCHAR(5) NOT NULL DEFAULT '17:00',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT schedule_settings_time_1_format
        CHECK (time_1 ~ '^([01][0-9]|2[0-3]):[0-5][0-9]'),

    CONSTRAINT schedule_settings_time_2_format
        CHECK (time_2 ~ '^([01][0-9]|2[0-3]):[0-5][0-9]'),

    CONSTRAINT schedule_settings_time_3_format
        CHECK (time_3 ~ '^([01][0-9]|2[0-3]):[0-5][0-9]')
);

INSERT INTO schedule_settings (id, time_1, time_2, time_3)
VALUES (1, '07:00', '12:00', '17:00')
ON CONFLICT (id) DO NOTHING;
