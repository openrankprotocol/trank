CREATE EXTENSION IF NOT EXISTS citext;

CREATE TABLE trank.channel_summaries (
    id BIGSERIAL PRIMARY KEY,
    channel_id CITEXT NOT NULL,
    run_id CITEXT,
    messages_limit INTEGER NOT NULL,
    summary JSONB NOT NULL,
    topic TEXT,
    few_words TEXT,
    one_sentence TEXT,
    error TEXT,
    model TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX channel_summaries_channel_null_run_unique
    ON trank.channel_summaries (channel_id)
    WHERE run_id IS NULL;

CREATE UNIQUE INDEX channel_summaries_channel_run_unique
    ON trank.channel_summaries (channel_id, run_id)
    WHERE run_id IS NOT NULL;

CREATE INDEX idx_channel_summaries_created_at
    ON trank.channel_summaries (created_at DESC);
