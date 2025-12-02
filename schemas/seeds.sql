CREATE TABLE trank.seeds (
    channel_id BIGINT NOT NULL,
    run_id INT NOT NULL,
    user_id BIGINT NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (channel_id, run_id, user_id),
    FOREIGN KEY (channel_id, run_id) REFERENCES trank.runs(channel_id, run_id) ON DELETE CASCADE
);

-- Index for querying seeds by user
CREATE INDEX idx_seeds_user_id ON trank.seeds(user_id);
