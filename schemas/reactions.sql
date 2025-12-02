CREATE TABLE trank.message_reactions (
    id SERIAL PRIMARY KEY,
    channel_id BIGINT NOT NULL,
    message_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    emoji VARCHAR(32) NOT NULL,
    date TIMESTAMP WITH TIME ZONE NOT NULL,
    FOREIGN KEY (channel_id, message_id) REFERENCES trank.messages(channel_id, id) ON DELETE CASCADE,
    UNIQUE (channel_id, message_id, user_id, emoji)
);

-- Index for querying reactions by user
CREATE INDEX idx_reactions_user_id ON trank.message_reactions(user_id);

-- Index for querying reactions by message (also supports FK constraint)
CREATE INDEX idx_reactions_message ON trank.message_reactions(channel_id, message_id);
