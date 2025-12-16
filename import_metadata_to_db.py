#!/usr/bin/env python3
"""
Import Metadata to PostgreSQL (Messages, Reactions, Users, Channels)

Imports messages from raw/[channel_id]_messages.json files into PostgreSQL database.
Also imports reactions from those messages, users from CSV files, and channel metadata from raw/channels.json.

Usage:
    python3 import_metadata_to_db.py                    # Import all channels from config
    python3 import_metadata_to_db.py --channel 123456   # Import specific channel
    python3 import_metadata_to_db.py --dry-run          # Show what would be imported without inserting

Requirements:
    - psycopg2 (install with: pip install psycopg2-binary)
    - Environment variable: DATABASE_URL (e.g., postgresql://user:pass@localhost:5432/dbname)

Database schema:
    See schemas/messages.sql, schemas/reactions.sql, schemas/users.sql, and schemas/channels.sql
"""

import argparse
import csv
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

        # Only add messages that have from_id (skip channel announcements)
        if from_id is not None:
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

        # Always process nested replies (even if parent has no from_id, like channel posts)
        replies_data = msg.get("replies_data", [])
        for reply in replies_data:
            process_message(reply)

    # Process all top-level messages
    for msg in messages:
        process_message(msg)

    return all_messages, all_reactions


def collect_users_from_csv(channel_id):
    """
    Collect users and admins from CSV files.

    Args:
        channel_id: Channel ID

    Returns:
        list of user tuples: (channel_id, user_id, username, first_name, last_name, bio, photo_id, is_admin)
    """
    users_file = Path("raw") / f"{channel_id}_user_ids.csv"
    admins_file = Path("raw") / f"{channel_id}_admins.csv"

    users_dict = {}  # user_id -> user data
    admin_ids = set()

    # Load admins first
    if admins_file.exists():
        with open(admins_file, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                user_id = row.get("user_id")
                if user_id:
                    admin_ids.add(int(user_id))

    # Load users
    if users_file.exists():
        with open(users_file, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                user_id = row.get("user_id")
                if not user_id:
                    continue

                user_id = int(user_id)
                username = row.get("username") or None
                first_name = row.get("first_name") or None
                last_name = row.get("last_name") or None
                bio = row.get("bio") or None

                # Parse photo_id from photo_url (format: "photo:123456")
                photo_url = row.get("photo_url") or ""
                photo_id = None
                if photo_url.startswith("photo:"):
                    try:
                        photo_id = int(photo_url.split(":")[1])
                    except (ValueError, IndexError):
                        pass

                is_admin = user_id in admin_ids

                users_dict[user_id] = (
                    int(channel_id),
                    user_id,
                    username,
                    first_name,
                    last_name,
                    bio,
                    photo_id,
                    is_admin,
                )

    # Add any admins that weren't in the users file
    for admin_id in admin_ids:
        if admin_id not in users_dict:
            users_dict[admin_id] = (
                int(channel_id),
                admin_id,
                None,  # username
                None,  # first_name
                None,  # last_name
                None,  # bio
                None,  # photo_id
                True,  # is_admin
            )

    return list(users_dict.values())


def load_channels_metadata():
    """
    Load channel metadata from raw/channels.json.

    Returns:
        dict: channel_id -> channel data
    """
    channels_file = Path("raw") / "channels.json"

    if not channels_file.exists():
        return {}

    with open(channels_file, "r", encoding="utf-8") as f:
        channels_list = json.load(f)

    return {ch["channel_id"]: ch for ch in channels_list}


def import_channels(conn, channel_ids, channels_metadata, dry_run=False):
    """
    Import channel metadata to database.

    Args:
        conn: Database connection
        channel_ids: List of channel IDs to import
        channels_metadata: Dict of channel_id -> channel data
        dry_run: If True, don't actually insert data

    Returns:
        int: Number of channels imported
    """
    channels_to_import = []

    for channel_id in channel_ids:
        channel_id_int = int(channel_id)
        if channel_id_int in channels_metadata:
            ch = channels_metadata[channel_id_int]
            channels_to_import.append(
                (
                    channel_id_int,
                    ch.get("name"),
                    ch.get("username"),
                    ch.get("is_group", False),
                )
            )

    if not channels_to_import:
        return 0

    if dry_run:
        return len(channels_to_import)

    cursor = conn.cursor()

    try:
        execute_values(
            cursor,
            """
            INSERT INTO trank.channels (channel_id, name, username, is_group)
            VALUES %s
            ON CONFLICT (channel_id) DO UPDATE SET
                name = EXCLUDED.name,
                username = EXCLUDED.username,
                is_group = EXCLUDED.is_group,
                updated_at = NOW()
            """,
            channels_to_import,
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()

    return len(channels_to_import)


def import_channel(conn, channel_id, dry_run=False):
    """
    Import messages, reactions, and users for a single channel.

    Args:
        conn: Database connection
        channel_id: Channel ID to import
        dry_run: If True, don't actually insert data

    Returns:
        tuple: (messages_count, reactions_count, users_count)
    """
    messages_file = Path("raw") / f"{channel_id}_messages.json"

    if not messages_file.exists():
        print(f"  ⚠️  Messages file not found: {messages_file}")
        return 0, 0, 0

    print(f"  📂 Loading messages from: {messages_file}")

    with open(messages_file, "r", encoding="utf-8") as f:
        messages = json.load(f)

    # Collect all messages and reactions
    all_messages, all_reactions = collect_messages_and_reactions(
        messages, int(channel_id)
    )

    # Collect users from CSV files
    all_users = collect_users_from_csv(channel_id)

    print(
        f"  📊 Found {len(all_messages)} messages, {len(all_reactions)} reactions, and {len(all_users)} users"
    )

    if dry_run:
        print(f"  🔍 Dry run - no data inserted")
        return len(all_messages), len(all_reactions), len(all_users)

    cursor = conn.cursor()

    try:
        # Insert messages using ON CONFLICT to handle duplicates
        if all_messages:
            print(f"  💾 Inserting messages...")
            execute_values(
                cursor,
                """
                INSERT INTO trank.messages (id, channel_id, date, from_id, message, reply_to_msg_id)
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
                INSERT INTO trank.message_reactions (channel_id, message_id, user_id, emoji, date)
                VALUES %s
                ON CONFLICT (channel_id, message_id, user_id, emoji) DO NOTHING
                """,
                all_reactions,
                page_size=1000,
            )

        # Insert users using ON CONFLICT to handle duplicates
        if all_users:
            print(f"  💾 Inserting users...")
            execute_values(
                cursor,
                """
                INSERT INTO trank.channel_users (channel_id, user_id, username, first_name, last_name, bio, photo_id, is_admin)
                VALUES %s
                ON CONFLICT (channel_id, user_id) DO UPDATE SET
                    username = EXCLUDED.username,
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name,
                    bio = EXCLUDED.bio,
                    photo_id = EXCLUDED.photo_id,
                    is_admin = EXCLUDED.is_admin
                """,
                all_users,
                page_size=1000,
            )

        conn.commit()
        print(
            f"  ✅ Successfully imported {len(all_messages)} messages, {len(all_reactions)} reactions, and {len(all_users)} users"
        )

    except Exception as e:
        conn.rollback()
        print(f"  ❌ Error importing data: {e}")
        raise

    finally:
        cursor.close()

    return len(all_messages), len(all_reactions), len(all_users)


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

    # Load channel metadata
    channels_metadata = load_channels_metadata()
    if channels_metadata:
        print(
            f"📋 Loaded metadata for {len(channels_metadata)} channels from raw/channels.json\n"
        )
    else:
        print(
            "⚠️  No raw/channels.json found - run 'python list_channels.py' to generate it\n"
        )

    total_messages = 0
    total_reactions = 0
    total_users = 0
    total_channels = 0

    try:
        for channel_id in channels:
            print(f"{'=' * 60}")
            print(f"Channel: {channel_id}")
            print(f"{'=' * 60}")

            messages_count, reactions_count, users_count = import_channel(
                conn, channel_id, dry_run=args.dry_run
            )

            total_messages += messages_count
            total_reactions += reactions_count
            total_users += users_count
            print()

        # Import channel metadata
        if channels_metadata:
            print(f"{'=' * 60}")
            print("Importing channel metadata")
            print(f"{'=' * 60}")
            total_channels = import_channels(
                conn, channels, channels_metadata, dry_run=args.dry_run
            )
            print(f"  ✅ Imported {total_channels} channel(s)\n")

    finally:
        conn.close()

    print(f"{'=' * 60}")
    print("📊 Summary")
    print(f"{'=' * 60}")
    print(f"   Total channels: {total_channels}")
    print(f"   Total messages: {total_messages}")
    print(f"   Total reactions: {total_reactions}")
    print(f"   Total users: {total_users}")
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
