# Telegram Channel Crawler

A simple Python tool to fetch and archive messages from Telegram channels using [Telethon](https://github.com/LonamiWebs/Telethon).

## Features

- **Simple** - Login with phone number, no session strings needed
- **Flexible** - Configure via `config.toml` file
- **Async** - Built with async/await for efficient message fetching
- **Rate limiting** - Respects Telegram API limits
- **Parallel processing** - Crawls multiple channels concurrently
- **Channel exclusion** - Skip unwanted channels (logs, bots, etc.)
- **JSON export** - Saves messages with full metadata

## Quick Start

### 1. Get API Credentials

1. Visit https://my.telegram.org
2. Login with your phone number
3. Go to "API Development Tools"
4. Create a new application (any name/description)
5. Copy your `api_id` and `api_hash`

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Setup Environment

```bash
# Copy sample environment file
cp .sample.env .env

# Edit .env and add your credentials
nano .env
```

Your `.env` should contain:
```env
TELEGRAM_APP_ID=12345678
TELEGRAM_APP_HASH=abcdef1234567890abcdef1234567890
TELEGRAM_PHONE=+1234567890
```

*Note: TELEGRAM_PHONE is optional - you'll be prompted if not set*

### 4. Configure Channels

First, list all your accessible channels:

```bash
python list_channels.py
```

This will show all channels with their IDs. Then edit `config.toml` to set which channels to crawl:

```toml
[channels]
include = [
    -1001234567890,     # Channel ID (from list_channels.py)
    -1003198190559,     # Another channel ID
]

exclude = [
    -1001111111111,     # Channel IDs to skip
]
```

### 5. Run the Crawler

```bash
python read_messages.py
```

On first run, you'll be prompted to:
- Enter your phone number (if not in .env)
- Enter the verification code Telegram sends you
- Enter your 2FA password (if enabled)

A session file will be created so you don't need to login again on subsequent runs.

## Configuration

Edit `config.toml` to customize the crawler:

```toml
[crawler]
time_window_days = 3              # How many days back to fetch
max_messages_per_channel = 2000   # Message limit per channel
parallel_requests = 3             # Concurrent channels to process
batch_size = 500                  # Number of messages to fetch per batch
rate_limiting_delay = 0.5         # Delay between requests (seconds)

[channels]
include = [-1001234567890, -1003198190559]  # Channel IDs to crawl
exclude = [-1001111111111]                   # Channel IDs to skip

[output]
pretty_print = true               # Format JSON nicely
indent_spaces = 2                 # JSON indentation

# Note: Messages are saved to raw/[channel_id]_messages.json
```

## Output

Messages are saved as JSON files in the `raw/` directory, named by channel ID:
- `raw/-1001234567890_messages.json` - Channel messages
- `raw/-1003198190559_messages.json` - Another channel messages

Each JSON file contains an array of simplified message objects with only essential fields:

```json
[
  {
    "id": 9099,
    "date": "2025-11-13T01:49:52+00:00",
    "from_id": 526750941,
    "message": "@lazovicff @dharmikumbhani",
    "reply_to_msg_id": 9098,
    "reactions": [
      {
        "user_id": 526750941,
        "emoji": "👍"
      },
      {
        "user_id": 123456789,
        "emoji": "👍"
      }
    ],
    "replies": 3
  }
]
```

**Fields included:**
- `id` - Message ID
- `date` - Message timestamp (ISO format)
- `from_id` - User ID who sent the message
- `message` - Message text content
- `reply_to_msg_id` - ID of message being replied to (if any)
- `reactions` - Array of reactions with user ID and emoji
- `replies` - Number of replies to this message

## Files

- `read_messages.py` - Main crawler script (run this)
- `list_channels.py` - List all accessible channels/groups
- `list_admins.py` - List all admins/moderators for channels in config
- `get_user_ids.py` - Get user ID to username mapping for all members
- `generate_trust.py` - Calculate trust scores from messages
- `login.py` - Setup guide and instructions
- `config.toml` - Configuration file
- `.env` - Environment variables (credentials)
- `requirements.txt` - Python dependencies

## Common Commands

```bash
# List all your channels
python list_channels.py

# List admins/moderators for channels in config
python list_admins.py

# Get user ID to username mapping
python get_user_ids.py

# Run the crawler
python read_messages.py

# Calculate trust scores
python generate_trust.py

# View setup guide
python login.py
```

## Troubleshooting

**"Missing Telegram credentials"**
→ Make sure `.env` has TELEGRAM_APP_ID and TELEGRAM_APP_HASH

**"Channel is not a valid ID"**
→ Only numeric IDs are accepted, run `python list_channels.py` to get IDs

**"Could not find the input entity"**
→ Make sure the channel ID is correct (from list_channels.py)

**"A wait of X seconds is required"**
→ You're rate limited. Increase `rate_limiting_delay` in config.toml

**Import errors**
→ Install dependencies: `pip install -r requirements.txt`

**Authorization failed**
→ Make sure you enter the correct phone number and verification code

## Output Format

Messages are saved in the `raw/` directory:
- Format: `raw/[channel_id]_messages.json`
- One file per channel
- Contains simplified message data (ID, date, user ID, text, reactions, replies)
- No unnecessary metadata included

## Session Files

The crawler creates a `telegram_session.session` file to remember your login.
- This file is automatically created on first login
- Don't commit this file to git (it's in .gitignore)
- Delete it if you want to login with a different account

## Resources

- [Telethon Documentation](https://docs.telethon.dev/)
- [Telethon GitHub](https://github.com/LonamiWebs/Telethon)
- [Telegram API](https://core.telegram.org/api)
- [Get API Credentials](https://my.telegram.org)

## License

ISC