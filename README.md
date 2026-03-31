# Telegram Channel Crawler

A Python tool to fetch, archive, and analyze messages from Telegram channels and groups using [Telethon](https://github.com/LonamiWebs/Telethon). Includes trust scoring with OpenRank and database integration.

## Features

- **Simple** - Login with phone number, no session strings needed
- **Flexible** - Configure via `config.toml` file
- **Async** - Built with async/await for efficient message fetching
- **Rate limiting** - Respects Telegram API limits
- **Parallel processing** - Crawls multiple channels concurrently
- **Channel exclusion** - Skip unwanted channels (logs, bots, etc.)
- **Checkpoints** - Automatically saves progress and allows resuming if interrupted
- **JSON export** - Saves messages with full metadata
- **Trust scoring** - Calculate trust scores using OpenRank algorithm
- **Database integration** - Import data to PostgreSQL
- **AI summarization** - Generate summaries using OpenAI
- **Photo management** - Download and upload user profile photos to S3

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

Create a `.env` file with your credentials:

```env
# Required for Telegram
TELEGRAM_APP_ID=12345678
TELEGRAM_APP_HASH=abcdef1234567890abcdef1234567890
TELEGRAM_PHONE=+1234567890

# Optional: For database imports
DATABASE_URL=postgresql://user:pass@localhost:5432/dbname

# Optional: For S3 photo uploads
S3USERNAME=your_aws_access_key_id
S3CREDENTIAL=your_aws_secret_access_key

# Optional: For AI summarization
OPENAI_API_KEY=sk-...
```

*Note: TELEGRAM_PHONE is optional - you'll be prompted if not set*

### 4. Configure Channels

First, list all your accessible channels:

```bash
python list_channels.py
```

This will show all channels with their IDs. Then edit `config.toml` to set which channels to crawl:

```toml
[group_chats]
include = [
    1234567890,     # Group chat ID (from list_channels.py)
]

[channels]
include = [
    -1001234567890,     # Channel ID (from list_channels.py)
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
time_window_days = 365            # How many days back to fetch
max_messages_per_channel = 40000  # Message limit per channel
parallel_requests = 1             # Concurrent channels to process
batch_size = 500                  # Number of messages to fetch per batch
rate_limiting_delay = 0.5         # Delay between requests (seconds)
checkpoint_interval = 2000        # Save checkpoint every N messages (0 to disable)
fetch_replies = true              # Fetch replies/comments to channel posts
max_reply_depth = 4               # Maximum depth for nested replies (0-5 recommended)

[group_chats]
include = [1234567890]            # Group chat IDs to crawl

[channels]
include = [-1001234567890]        # Channel IDs to crawl

[output]
pretty_print = true               # Format JSON nicely
indent_spaces = 2                 # JSON indentation

[trust]
mention_points = 50               # Points for direct mentions
reply_points = 40                 # Points for replies
reaction_points = 30              # Points for reactions
```

## Files

### Main Scripts

| Script | Description |
|--------|-------------|
| `read_messages.py` | Main crawler script - fetches messages from Telegram |
| `list_channels.py` | List all accessible channels/groups with their IDs |
| `list_admins.py` | List admins/moderators for configured channels (saves to CSV) |
| `generate_trust.py` | Calculate trust scores from messages |
| `process_scores.py` | Aggregate and normalize trust scores |
| `process_seed.py` | Process seed CSV files with tier-based weighting |
| `generate_json.py` | Generate JSON files for UI from seed/output data |

### Database Scripts

| Script | Description |
|--------|-------------|
| `import_metadata_to_db.py` | Import messages, reactions, users, channels to PostgreSQL |
| `import_scores_to_db.py` | Import seeds and scores to PostgreSQL |

### Photo Management

| Script | Description |
|--------|-------------|
| `download_photos.py` | Download user profile photos to `raw/photos/` |
| `upload_photos.py` | Upload photos to S3 (`s3://openrank-files/telegram`) |

### AI Features

| Script | Description |
|--------|-------------|
| `summarize_posts.py` | Generate AI summaries of posts using OpenAI |

### Channel-Specific Scripts

Located in `channel/` directory for channel-specific processing:

| Script | Description |
|--------|-------------|
| `channel/read_channel_messages.py` | Channel-specific message fetching |
| `channel/generate_channel_trust.py` | Channel-specific trust generation |
| `channel/generate_channel_json.py` | Channel-specific JSON generation |

### Pipeline

| Script | Description |
|--------|-------------|
| `run_pipeline.sh` | Run complete pipeline: trust → OpenRank → scores → JSON |

### Configuration Files

| File | Description |
|------|-------------|
| `config.toml` | Main configuration file |
| `.env` | Environment variables (credentials) |
| `requirements.txt` | Python dependencies |

## Directory Structure

```
trank/
├── channel/          # Channel-specific scripts
├── output/           # Processed output files
├── raw/              # Raw data from Telegram
│   ├── checkpoints/  # Checkpoint files for resuming
│   └── photos/       # Downloaded user profile photos
├── schemas/          # PostgreSQL schema files
├── scores/           # Computed OpenRank scores
├── seed/             # Seed values for trust computation
├── trust/            # Trust edge files
├── ui/               # JSON files for UI consumption
└── tmp/              # Temporary files
```

## Common Commands

```bash
# List all your channels
python list_channels.py

# Run the crawler
python read_messages.py

# List admins/moderators (saves to CSV)
python list_admins.py

# Calculate trust scores
python generate_trust.py

# Process and normalize scores
python process_scores.py

# Generate UI JSON files
python generate_json.py

# Run complete pipeline
./run_pipeline.sh

# Run pipeline for channels (not group chats)
./run_pipeline.sh --channel

# Download user profile photos
python download_photos.py
python download_photos.py --skip-existing
python download_photos.py --verbose

# Upload photos to S3
python upload_photos.py
python upload_photos.py --dry-run
python upload_photos.py --force

# Import to database
python import_metadata_to_db.py
python import_metadata_to_db.py --channel 123456
python import_metadata_to_db.py --dry-run

python import_scores_to_db.py
python import_scores_to_db.py --channel 123456

# Generate AI summaries
python summarize_posts.py
```

## Checkpoints

The crawler automatically saves checkpoints during message fetching to prevent data loss if interrupted. Checkpoints are saved to `raw/checkpoints/` directory.

**How it works:**
- Checkpoint files are created every N messages (configurable via `checkpoint_interval` in config.toml)
- Default is every 2000 messages
- If the script is interrupted, it will detect the checkpoint on next run and ask if you want to resume
- Checkpoints are automatically deleted after successful completion
- Set `checkpoint_interval = 0` to disable checkpoints

**Resuming from checkpoint:**
```bash
python read_messages.py
# If a checkpoint is found, you'll see:
# 📂 Found checkpoint for channel -1001234567890 with 500 messages
#    Last saved: 2025-01-15T10:30:45+00:00
#    Resume from checkpoint? (y/n):
```

## Output Format

### Messages

Messages are saved in the `raw/` directory:
- Format: `raw/[channel_id]_messages.json`
- One file per channel
- Contains simplified message data (ID, date, user ID, text, reactions, replies)

Example message:
```json
{
  "id": 9099,
  "date": "2025-11-13T01:49:52+00:00",
  "from_id": 526750941,
  "message": "@lazovicff @dharmikumbhani",
  "reply_to_msg_id": 9098,
  "reactions": [
    {"user_id": 526750941, "emoji": "👍"},
    {"user_id": 123456789, "emoji": "👍"}
  ],
  "replies_count": 3,
  "replies_data": [...]
}
```

### User Information

- Format: `raw/[channel_id]_user_ids.csv`
- Columns: `user_id,username,first_name,last_name`
- Some users may not have usernames (this is normal on Telegram)

### Admin Lists

- Format: `raw/[channel_id]_admins.csv`
- Columns: `user_id,username,first_name,last_name`
- Generated by running `python list_admins.py`

### Trust Scores

- `trust/[channel_id].csv` - Raw trust edges with user IDs (i,j,v format)
- `scores/[channel_id].csv` - OpenRank computed scores
- `output/[channel_id].csv` - Processed scores with display names or user IDs

## Trust Score Workflow

The complete trust scoring workflow:

1. **Fetch messages**: `python read_messages.py`
   - Saves messages to `raw/[channel_id]_messages.json`
   - Saves user info to `raw/[channel_id]_user_ids.csv`

2. **Generate trust edges**: `python generate_trust.py`
   - Reads messages and calculates trust based on reactions, replies, and mentions
   - Saves trust edges to `trust/[channel_id].csv` (format: `i,j,v`)

3. **Compute OpenRank scores**: Uses external `openrank` CLI tool
   ```bash
   openrank compute-local-et trust/[channel_id].csv seed/[channel_id].csv \
       --out-path=scores/[channel_id].csv --alpha=0.25 --delta=0.000001
   ```

4. **Process scores**: `python process_scores.py`
   - Aggregates incoming trust for each user
   - Converts user IDs to display names
   - Normalizes scores
   - Saves to `output/[channel_id].csv`

5. **Generate JSON**: `python generate_json.py`
   - Creates UI-ready JSON files in `ui/` directory

**Or run everything at once:**
```bash
./run_pipeline.sh           # For group chats
./run_pipeline.sh --channel # For channels
```

## Database Integration

### Schema Files

Located in `schemas/` directory:
- `messages.sql` - Message storage
- `reactions.sql` - Reaction data
- `users.sql` - User information
- `channels.sql` - Channel metadata
- `runs.sql` - Processing run tracking
- `seeds.sql` - Seed values
- `scores.sql` - Computed scores
- `summaries.sql` - AI-generated summaries

### Importing Data

```bash
# Set DATABASE_URL in .env first
export DATABASE_URL=postgresql://user:pass@localhost:5432/dbname

# Import all metadata (messages, reactions, users, channels)
python import_metadata_to_db.py

# Import specific channel
python import_metadata_to_db.py --channel 123456

# Preview without inserting
python import_metadata_to_db.py --dry-run

# Import seeds and scores
python import_scores_to_db.py
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

**Script keeps getting interrupted**
→ Enable checkpoints in config.toml with `checkpoint_interval = 2000` to save progress periodically

**Want to restart from scratch (ignore checkpoint)**
→ When prompted to resume, type 'n' or manually delete checkpoint files in `raw/checkpoints/`

**Import errors**
→ Install dependencies: `pip install -r requirements.txt`

**Authorization failed**
→ Make sure you enter the correct phone number and verification code

**"Collected info for 0 unique users" for channel posts**
→ This is normal for channels (not groups). Set `fetch_replies = true` in config.toml to fetch comments/replies where user interactions happen.

**Database connection failed**
→ Check that DATABASE_URL is set correctly in `.env`

**S3 upload failed**
→ Check that S3USERNAME and S3CREDENTIAL are set in `.env`

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
- [OpenRank](https://github.com/openrank-community/openrank)

## License

ISC