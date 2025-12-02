CREATE TABLE trank.channel_users (
    channel_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    username VARCHAR(255),
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    bio TEXT,
    photo_id BIGINT,
    is_admin BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (channel_id, user_id)
);

-- Index for looking up users across all channels
CREATE INDEX idx_channel_users_user_id ON trank.channel_users(user_id);

-- Index for looking up by username
CREATE INDEX idx_channel_users_username ON trank.channel_users(username) WHERE username IS NOT NULL;

-- Index for filtering admins
CREATE INDEX idx_channel_users_admins ON trank.channel_users(channel_id) WHERE is_admin = TRUE;
