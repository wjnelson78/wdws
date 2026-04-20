-- Migration 015: Health Tracking — HealthKit, medications, symptoms, daily summaries
-- Athena Cognitive Engine
-- Date: 2026-04-04

-- Health data sources (HealthKit devices, manual, Fitbit, etc.)
CREATE TABLE IF NOT EXISTS core.health_sources (
    id          SERIAL PRIMARY KEY,
    user_id     INT NOT NULL REFERENCES core.users(id),
    provider    TEXT NOT NULL,              -- healthkit, fitbit, google, manual
    device_name TEXT,                       -- "Apple Watch Series 9", "iPhone 15 Pro"
    device_id   TEXT,                       -- unique device identifier
    is_active   BOOLEAN DEFAULT true,
    last_synced_at TIMESTAMPTZ,
    created_at  TIMESTAMPTZ DEFAULT now(),
    UNIQUE (user_id, provider, device_id)
);

-- Timestamped numeric health readings (heart rate, steps, sleep, etc.)
CREATE TABLE IF NOT EXISTS core.health_metrics (
    id          BIGSERIAL PRIMARY KEY,
    user_id     INT NOT NULL REFERENCES core.users(id),
    source_id   INT REFERENCES core.health_sources(id),
    metric_type TEXT NOT NULL,              -- see index below for types
    value       DOUBLE PRECISION NOT NULL,
    unit        TEXT,                       -- bpm, ms, count, kcal, kg, mmHg, %, °C, ml, min, m
    start_time  TIMESTAMPTZ NOT NULL,
    end_time    TIMESTAMPTZ,
    metadata    JSONB DEFAULT '{}',
    created_at  TIMESTAMPTZ DEFAULT now(),
    UNIQUE (user_id, metric_type, start_time, source_id)
);
CREATE INDEX IF NOT EXISTS idx_health_metrics_user_type ON core.health_metrics (user_id, metric_type, start_time DESC);
CREATE INDEX IF NOT EXISTS idx_health_metrics_time ON core.health_metrics (start_time DESC);

-- Workout sessions
CREATE TABLE IF NOT EXISTS core.health_workouts (
    id          BIGSERIAL PRIMARY KEY,
    user_id     INT NOT NULL REFERENCES core.users(id),
    source_id   INT REFERENCES core.health_sources(id),
    workout_type TEXT NOT NULL,             -- running, walking, cycling, swimming, strength, yoga, etc.
    start_time  TIMESTAMPTZ NOT NULL,
    end_time    TIMESTAMPTZ,
    duration_secs INT,
    calories    DOUBLE PRECISION,
    distance    DOUBLE PRECISION,           -- meters
    avg_heart_rate DOUBLE PRECISION,
    max_heart_rate DOUBLE PRECISION,
    metadata    JSONB DEFAULT '{}',         -- route, elevation, laps, etc.
    created_at  TIMESTAMPTZ DEFAULT now(),
    UNIQUE (user_id, workout_type, start_time, source_id)
);
CREATE INDEX IF NOT EXISTS idx_health_workouts_user ON core.health_workouts (user_id, start_time DESC);

-- Medication / supplement tracking
CREATE TABLE IF NOT EXISTS core.health_medications (
    id          SERIAL PRIMARY KEY,
    user_id     INT NOT NULL REFERENCES core.users(id),
    name        TEXT NOT NULL,
    dosage      TEXT,                       -- "50mg", "2 capsules"
    frequency   TEXT,                       -- "daily", "twice daily", "as needed"
    times_of_day TEXT[],                    -- {"08:00", "20:00"}
    category    TEXT,                       -- prescription, supplement, otc
    prescriber  TEXT,
    is_active   BOOLEAN DEFAULT true,
    notes       TEXT,
    started_at  DATE,
    ended_at    DATE,
    created_at  TIMESTAMPTZ DEFAULT now()
);

-- Medication adherence log
CREATE TABLE IF NOT EXISTS core.health_medication_logs (
    id              BIGSERIAL PRIMARY KEY,
    medication_id   INT NOT NULL REFERENCES core.health_medications(id) ON DELETE CASCADE,
    user_id         INT NOT NULL REFERENCES core.users(id),
    taken_at        TIMESTAMPTZ DEFAULT now(),
    scheduled_time  TEXT,                   -- "08:00"
    skipped         BOOLEAN DEFAULT false,
    notes           TEXT,
    created_at      TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_med_logs_user ON core.health_medication_logs (user_id, taken_at DESC);

-- Symptom journal
CREATE TABLE IF NOT EXISTS core.health_symptoms (
    id              BIGSERIAL PRIMARY KEY,
    user_id         INT NOT NULL REFERENCES core.users(id),
    symptom_type    TEXT NOT NULL,           -- fatigue, brain_fog, headache, pain, nausea, dizziness, etc.
    severity        INT NOT NULL CHECK (severity BETWEEN 1 AND 10),
    duration_mins   INT,
    triggers        TEXT[],                  -- {"exertion", "stress", "poor_sleep"}
    notes           TEXT,
    recorded_at     TIMESTAMPTZ DEFAULT now(),
    created_at      TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_symptoms_user ON core.health_symptoms (user_id, recorded_at DESC);

-- Pre-aggregated daily summaries for AI context injection
CREATE TABLE IF NOT EXISTS core.health_daily_summaries (
    id              SERIAL PRIMARY KEY,
    user_id         INT NOT NULL REFERENCES core.users(id),
    date            DATE NOT NULL,
    sleep_hours     DOUBLE PRECISION,
    sleep_quality   TEXT,                   -- good, fair, poor
    sleep_deep_pct  DOUBLE PRECISION,
    sleep_rem_pct   DOUBLE PRECISION,
    avg_heart_rate  DOUBLE PRECISION,
    resting_hr      DOUBLE PRECISION,
    hrv_avg         DOUBLE PRECISION,       -- ms
    steps           INT,
    active_calories INT,
    distance_m      DOUBLE PRECISION,
    flights_climbed INT,
    workouts        INT DEFAULT 0,
    blood_oxygen    DOUBLE PRECISION,
    weight_kg       DOUBLE PRECISION,
    water_ml        DOUBLE PRECISION,
    mindfulness_mins INT,
    symptom_count   INT DEFAULT 0,
    fatigue_level   INT,                    -- 1-10
    meds_taken      INT DEFAULT 0,
    meds_missed     INT DEFAULT 0,
    summary_text    TEXT,                   -- AI-generated natural language summary
    metadata        JSONB DEFAULT '{}',
    generated_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ DEFAULT now(),
    UNIQUE (user_id, date)
);
CREATE INDEX IF NOT EXISTS idx_health_summary_user ON core.health_daily_summaries (user_id, date DESC);
