CREATE TABLE IF NOT EXISTS schedule_settings (
    id SERIAL PRIMARY KEY,
    time_1 VARCHAR(5) NOT NULL DEFAULT '07:00',
    time_2 VARCHAR(5) NOT NULL DEFAULT '12:00',
    time_3 VARCHAR(5) NOT NULL DEFAULT '17:00',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT schedule_settings_time_1_format CHECK (time_1 ~ '^([01][0-9]|2[0-3]):[0-5][0-9]'),
    CONSTRAINT schedule_settings_time_2_format CHECK (time_2 ~ '^([01][0-9]|2[0-3]):[0-5][0-9]'),
    CONSTRAINT schedule_settings_time_3_format CHECK (time_3 ~ '^([01][0-9]|2[0-3]):[0-5][0-9]')
);

INSERT INTO schedule_settings (id, time_1, time_2, time_3)
VALUES (1, '07:00', '12:00', '17:00')
ON CONFLICT (id) DO NOTHING;
