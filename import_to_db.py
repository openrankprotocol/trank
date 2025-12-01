#!/usr/bin/env python3
"""
Import Messages and Reactions to PostgreSQL

Imports messages from raw/[channel_id]_messages.json files into PostgreSQL database.
Also imports reactions from those messages.

Usage:
    python3 import_to_db.py                    # Import all channels from config
    python3 import_to_db.py --channel 123456   # Import specific channel
    python3 import_to_db.py --dry-run          # Show what would be imported without inserting

Requirements:
    - psycopg2 (install with: pip install psycopg2-binary)
    - Environment variable: DATABASE_URL (e.g., postgresql://user:pass@localhost:5432/dbname)

Database schema:
    CREATE TABLE messages (
        id BIGINT NOT NULL,
        channel_id BIGINT NOT NULL,
        date TIMESTAMP WITH TIME ZONE NOT NULL,
        from_id BIGINT NOT NULL,
        message TEXT,
        reply_to_msg_id BIGINT,
        PRIMARY KEY (channel_id, id)
    );

    CREATE TABLE message_reactions (
        id SERIAL PRIMARY KEY,
        channel_id BIGINT NOT NULL,
        message_id BIGINT NOT NULL,
        user_id BIGINT NOT NULL,
        emoji VARCHAR(32) NOT NULL,
        date TIMESTAMP WITH TIME ZONE NOT NULL,
        FOREIGN KEY (channel_id, message_id) REFERENCES messages(channel_id, id) ON DELETE CASCADE,
        UNIQUE (channel_id, message_id, user_id, emoji)
    );
"""

import argparse
import json
import os
import sys
from pathlib import Path

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values

# Use built-in tomllib for Python 3.11+, fallback to tomli for older versions
if sys.version_info >= (3, 11):
    import tomllib
else:
    try:
        import tomli as tomllib
    except ImportError:
        import toml as tomllib

# Load environment variables from .env file
load_dotenv()


def load_config():
    """Load configuration from config.toml"""
    config_path = Path(__file__).parent / "config.toml"

    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, "rb") as f:
        return tomllib.load(f)


def get_db_connection():
    """Get database connection from DATABASE_URL environment variable."""
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL environment variable is required")

    return psycopg2.connect(database_url)


def collect_messages_and_reactions(messages, channel_id):
    """
    Recursively collect all messages and reactions from the JSON structure.

    Args:
        messages: List of message objects from JSON
        channel_id: Channel ID for these messages

    Returns:
        tuple: (list of message tuples, list of reaction tuples)
    """
    all_messages = []
    all_reactions = []

    def process_message(msg):
        """Process a single message and its nested replies."""
        msg_id = msg.get("id")
        from_id = msg.get("from_id")
        date = msg.get("date")
        message_text = msg.get("message")
        reply_to_msg_id = msg.get("reply_to_msg_id")

        # Skip messages without required fields
        if msg_id is None or date is None:
            return

        # Skip messages without from_id (channel announcements)
        if from_id is None:
            return

        # Add message tuple: (id, channel_id, date, from_id, message, reply_to_msg_id)
        all_messages.append(
            (msg_id, channel_id, date, from_id, message_text, reply_to_msg_id)
        )

        # Process reactions
        reactions = msg.get("reactions", [])
        for reaction in reactions:
            user_id = reaction.get("user_id")
            emoji = reaction.get("emoji")

            # Skip aggregated reactions (user_id is null for channels)
            if user_id is None:
                continue

            # Add reaction tuple: (channel_id, message_id, user_id, emoji, date)
            all_reactions.append(
                (
                    channel_id,
                    msg_id,
                    user_id,
                    emoji,
                    date,  # Use message date as reaction date (Telegram doesn't provide reaction timestamp)
                )
            )

        # Process nested replies
        replies_data = msg.get("replies_data", [])
        for reply in replies_data:
            process_message(reply)

    # Process all top-level messages
    for msg in messages:
        process_message(msg)

    return all_messages, all_reactions


def import_channel(conn, channel_id, dry_run=False):
    """
    Import messages and reactions for a single channel.

    Args:
        conn: Database connection
        channel_id: Channel ID to import
        dry_run: If True, don't actually insert data

    Returns:
        tuple: (messages_count, reactions_count)
    """
    messages_file = Path("raw") / f"{channel_id}_messages.json"

    if not messages_file.exists():
        print(f"  ⚠️  Messages file not found: {messages_file}")
        return 0, 0

    print(f"  📂 Loading messages from: {messages_file}")

    with open(messages_file, "r", encoding="utf-8") as f:
        messages = json.load(f)

    # Collect all messages and reactions
    all_messages, all_reactions = collect_messages_and_reactions(
        messages, int(channel_id)
    )

    print(f"  📊 Found {len(all_messages)} messages and {len(all_reactions)} reactions")

    if dry_run:
        print(f"  🔍 Dry run - no data inserted")
        return len(all_messages), len(all_reactions)

    cursor = conn.cursor()

    try:
        # Insert messages using ON CONFLICT to handle duplicates
        if all_messages:
            print(f"  💾 Inserting messages...")
            execute_values(
                cursor,
                """
                INSERT INTO messages (id, channel_id, date, from_id, message, reply_to_msg_id)
                VALUES %s
                ON CONFLICT (channel_id, id) DO UPDATE SET
                    date = EXCLUDED.date,
                    from_id = EXCLUDED.from_id,
                    message = EXCLUDED.message,
                    reply_to_msg_id = EXCLUDED.reply_to_msg_id
                """,
                all_messages,
                page_size=1000,
            )

        # Insert reactions using ON CONFLICT to handle duplicates
        if all_reactions:
            print(f"  💾 Inserting reactions...")
            execute_values(
                cursor,
                """
                INSERT INTO message_reactions (channel_id, message_id, user_id, emoji, date)
                VALUES %s
                ON CONFLICT (channel_id, message_id, user_id, emoji) DO NOTHING
                """,
                all_reactions,
                page_size=1000,
            )

        conn.commit()
        print(
            f"  ✅ Successfully imported {len(all_messages)} messages and {len(all_reactions)} reactions"
        )

    except Exception as e:
        conn.rollback()
        print(f"  ❌ Error importing data: {e}")
        raise

    finally:
        cursor.close()

    return len(all_messages), len(all_reactions)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Import messages and reactions to PostgreSQL database"
    )
    parser.add_argument(
        "--channel",
        type=str,
        help="Specific channel ID to import (otherwise imports all from config)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be imported without actually inserting",
    )

    args = parser.parse_args()

    print("📥 Import Messages and Reactions to PostgreSQL\n")

    # Determine which channels to process
    if args.channel:
        channels = [args.channel]
        print(f"Processing specific channel: {args.channel}")
    else:
        # Load from config
        try:
            config = load_config()
        except FileNotFoundError as e:
            print(f"❌ Error: {e}")
            sys.exit(1)

        group_chats = config.get("group_chats", {}).get("include", [])
        channels_list = config.get("channels", {}).get("include", [])
        channels = [str(ch) for ch in group_chats + channels_list]

        if not channels:
            print("❌ Error: No channels configured in config.toml")
            sys.exit(1)

        print(f"Found {len(channels)} channel(s) in config.toml")

    if args.dry_run:
        print("Mode: Dry run (no data will be inserted)\n")
    else:
        print()

    # Connect to database
    try:
        conn = get_db_connection()
        print("✅ Connected to database\n")
    except Exception as e:
        print(f"❌ Error connecting to database: {e}")
        sys.exit(1)

    total_messages = 0
    total_reactions = 0

    try:
        for channel_id in channels:
            print(f"{'=' * 60}")
            print(f"Channel: {channel_id}")
            print(f"{'=' * 60}")

            messages_count, reactions_count = import_channel(
                conn, channel_id, dry_run=args.dry_run
            )

            total_messages += messages_count
            total_reactions += reactions_count
            print()

    finally:
        conn.close()

    print(f"{'=' * 60}")
    print("📊 Summary")
    print(f"{'=' * 60}")
    print(f"   Total messages: {total_messages}")
    print(f"   Total reactions: {total_reactions}")
    if args.dry_run:
        print("   (Dry run - no data was inserted)")
    print()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n⚠️  Interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
