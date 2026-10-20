CREATE TABLE IF NOT EXISTS extract_checkpoint (
    stream       VARCHAR(128) PRIMARY KEY,
    cursor_value TEXT         NOT NULL,
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    run_id       UUID         NOT NULL
);

CREATE TABLE IF NOT EXISTS consumer_offsets (
    topic       VARCHAR(255) NOT NULL,
    partition   INT          NOT NULL,
    next_offset BIGINT       NOT NULL,
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (topic, partition)
);

CREATE TABLE IF NOT EXISTS stream_schema (
    stream       VARCHAR(128) PRIMARY KEY,
    version      INT          NOT NULL,
    columns_json JSONB        NOT NULL,
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS schema_events (
    id           BIGSERIAL    PRIMARY KEY,
    stream       VARCHAR(128) NOT NULL,
    old_version  INT,
    new_version  INT          NOT NULL,
    event_type   VARCHAR(64)  NOT NULL,
    column_name  VARCHAR(128) NOT NULL,
    details_json JSONB,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dead_letters (
    id              BIGSERIAL    PRIMARY KEY,
    stream          VARCHAR(128) NOT NULL,
    pk              TEXT,
    reason_code     VARCHAR(64)  NOT NULL,
    reason_detail   TEXT,
    raw_payload     TEXT         NOT NULL,
    kafka_topic     VARCHAR(255),
    kafka_partition INT,
    kafka_offset    BIGINT,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS sync_runs (
    id                UUID         PRIMARY KEY,
    connector         VARCHAR(64)  NOT NULL,
    stream            VARCHAR(128) NOT NULL,
    mode              VARCHAR(32)  NOT NULL,
    status            VARCHAR(32)  NOT NULL,
    started_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    finished_at       TIMESTAMPTZ,
    records_extracted BIGINT       NOT NULL DEFAULT 0,
    records_loaded    BIGINT       NOT NULL DEFAULT 0,
    records_dead      BIGINT       NOT NULL DEFAULT 0,
    error_message     TEXT
);
