CREATE TABLE trank.runs (
    channel_id BIGINT NOT NULL REFERENCES trank.channels(channel_id),
    run_id INT NOT NULL,
    days_back INT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    PRIMARY KEY (channel_id, run_id)
);

-- Index for querying runs by creation time
CREATE INDEX idx_runs_created_at ON trank.runs(created_at);
