CREATE TABLE trank.channels (
    channel_id BIGINT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    username VARCHAR(255),
    is_group BOOLEAN NOT NULL DEFAULT FALSE,
    description TEXT,
    member_count INT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Index for looking up by username
CREATE INDEX idx_channels_username ON trank.channels(username) WHERE username IS NOT NULL;

-- Index for filtering by type (group vs channel)
CREATE INDEX idx_channels_is_group ON trank.channels(is_group);
