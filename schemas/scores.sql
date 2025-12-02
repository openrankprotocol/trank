CREATE TABLE trank.scores (
    channel_id BIGINT NOT NULL,
    run_id INT NOT NULL,
    user_id BIGINT NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (channel_id, run_id, user_id),
    FOREIGN KEY (channel_id, run_id) REFERENCES trank.runs(channel_id, run_id) ON DELETE CASCADE
);

-- Index for querying scores by user
CREATE INDEX idx_scores_user_id ON trank.scores(user_id);

-- Index for querying top scores within a run
CREATE INDEX idx_scores_run_value ON trank.scores(channel_id, run_id, value DESC);
