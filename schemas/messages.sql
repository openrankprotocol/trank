CREATE TABLE trank.messages (
    id BIGINT NOT NULL,
    channel_id BIGINT NOT NULL,
    date TIMESTAMP WITH TIME ZONE NOT NULL,
    from_id BIGINT NOT NULL,
    message TEXT,
    reply_to_msg_id BIGINT,
    PRIMARY KEY (channel_id, id)
);

-- Index for querying messages by user
CREATE INDEX idx_messages_from_id ON trank.messages(from_id);

-- Index for querying messages by date
CREATE INDEX idx_messages_date ON trank.messages(date);

-- Index for querying reply threads
CREATE INDEX idx_messages_reply_to ON trank.messages(reply_to_msg_id) WHERE reply_to_msg_id IS NOT NULL;
