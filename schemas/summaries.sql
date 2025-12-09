CREATE EXTENSION IF NOT EXISTS citext;

CREATE TABLE trank.channel_summaries (
    id BIGSERIAL PRIMARY KEY,
    channel_id CITEXT NOT NULL UNIQUE,
    summary JSONB NOT NULL,
    topic TEXT,
    few_words TEXT,
    one_sentence TEXT,
    error TEXT,
    model TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_channel_summaries_created_at
    ON trank.channel_summaries (created_at DESC);
