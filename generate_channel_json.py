#!/usr/bin/env python3
"""
Generate JSON files for UI from seed and output data for Telegram Channels.

This script is specifically designed for channel data where:
- Main posts have from_id: null (channel announcements)
- Replies are nested within replies_data
- Reactions have user_id: null with only count (aggregated) - so num_given_reactions is not tracked

This script:
1. Loads channel list from config.toml
2. Loads corresponding entries from seed/ directory
3. Loads corresponding score files from output/ directory
4. Loads user mappings from raw/[channel_id]_user_ids.csv
5. Calculates user stats from raw/[channel_id]_messages.json
6. Creates JSON files for each channel with enriched user data

Usage:
    python3 generate_channel_json.py

Requirements:
    - pandas (install with: pip install pandas)
    - toml (install with: pip install toml)
    - config.toml with channel configuration
    - CSV files in seed/ directory
    - CSV files in output/ directory (matching seed filenames)
    - CSV files in raw/ directory: [channel_id]_user_ids.csv
    - JSON files in raw/ directory: [channel_id]_messages.json

Output:
    - Creates ui/ directory if it doesn't exist
    - For each configured channel (e.g., 1533865579), creates:
      - ui/1533865579.json with seed and score data
"""

import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import toml


def load_user_info(channel_id):
    """
    Load user info from CSV file.

    Args:
        channel_id: Channel ID to load user info for

    Returns:
        dict: Mapping of user_id (str) -> {username, display_name, bio}
    """
    user_info = {}

    try:
        mapping_file = Path(f"raw/{channel_id}_user_ids.csv")

        if mapping_file.exists():
            print(f"  📋 Loading user info from: {mapping_file}")
            mapping_df = pd.read_csv(mapping_file)

            for _, row in mapping_df.iterrows():
                user_id = str(row["user_id"])

                # Extract username
                username = ""
                if pd.notna(row.get("username")):
                    username = str(row["username"]).strip()

                # Extract first name
                first_name = ""
                if pd.notna(row.get("first_name")):
                    first_name = str(row["first_name"]).strip()

                # Extract last name
                last_name = ""
                if pd.notna(row.get("last_name")):
                    last_name = str(row["last_name"]).strip()

                # Extract bio
                bio = ""
                if pd.notna(row.get("bio")):
                    bio = str(row["bio"]).strip()

                # Build display name: "first last" or username or user_id
                if first_name or last_name:
                    display_name = f"{first_name} {last_name}".strip()
                elif username:
                    display_name = username
                else:
                    display_name = user_id

                user_info[user_id] = {
                    "username": username,
                    "display_name": display_name,
                    "bio": bio,
                }

            print(f"  ✅ Loaded {len(user_info)} user info entries")
        else:
            print(f"  ⚠️  Warning: {mapping_file} not found")

    except Exception as e:
        print(f"  ⚠️  Warning: Could not load user info: {str(e)}")

    return user_info


def calculate_channel_user_stats(channel_id):
    """
    Calculate user stats from channel messages JSON file.

    For channels, reactions are aggregated (user_id is null, only count is available),
    so we cannot track num_given_reactions.

    Stats calculated per user:
    - num_posts: Number of messages/replies posted
    - num_received_reactions: Total reactions received on their messages
    - num_received_replies: Total replies under all messages authored by this user
    - num_given_replies: Number of replies they made

    Args:
        channel_id: Channel ID to calculate stats for

    Returns:
        dict: Mapping of user_id (str) -> stats dict
    """
    stats = defaultdict(
        lambda: {
            "num_posts": 0,
            "num_received_reactions": 0,
            "num_received_replies": 0,
            "num_given_replies": 0,
            "first_post_at": None,
            "last_post_at": None,
        }
    )

    messages_file = Path(f"raw/{channel_id}_messages.json")

    if not messages_file.exists():
        print(f"  ⚠️  Warning: {messages_file} not found, stats will be empty")
        return dict(stats)

    try:
        print(f"  📊 Calculating stats from: {messages_file}")

        with open(messages_file, "r", encoding="utf-8") as f:
            messages = json.load(f)

        # First pass: build message_id -> author_id map and collect all messages
        message_authors = {}
        all_messages = []

        def collect_messages(msg, is_reply=False):
            """Collect all messages and build author map."""
            msg_id = msg.get("id")
            author_id = msg.get("from_id")
            if msg_id and author_id:
                message_authors[msg_id] = str(author_id)
            all_messages.append((msg, is_reply))
            for reply in msg.get("replies_data", []):
                collect_messages(reply, is_reply=True)

        for msg in messages:
            collect_messages(msg, is_reply=False)

        # Second pass: calculate stats
        for msg, is_reply in all_messages:
            author_id = msg.get("from_id")

            if author_id:
                author_id = str(author_id)
                # Count post
                stats[author_id]["num_posts"] += 1

                # If this message is a reply (has reply_to_msg_id or is in replies_data), increment given_replies
                if msg.get("reply_to_msg_id") is not None or is_reply:
                    stats[author_id]["num_given_replies"] += 1

                # Track first and last post timestamps
                msg_date = msg.get("date")
                if msg_date:
                    current_first = stats[author_id]["first_post_at"]
                    current_last = stats[author_id]["last_post_at"]
                    if current_first is None or msg_date < current_first:
                        stats[author_id]["first_post_at"] = msg_date
                    if current_last is None or msg_date > current_last:
                        stats[author_id]["last_post_at"] = msg_date

            # Process reactions on this message
            # For channels, reactions are aggregated (user_id is null)
            # We can only count received reactions, not given reactions
            reactions = msg.get("reactions", [])
            for reaction in reactions:
                # Count received reactions for message author
                if author_id:
                    # Handle reactions with count field (aggregated)
                    count = reaction.get("count", 1)
                    stats[author_id]["num_received_reactions"] += count

            # Track received replies via reply_to_msg_id
            reply_to_id = msg.get("reply_to_msg_id")
            if reply_to_id and reply_to_id in message_authors:
                target_author = message_authors[reply_to_id]
                # Don't count self-replies
                if author_id and author_id != target_author:
                    stats[target_author]["num_received_replies"] += 1

        # Also count replies_data as received_replies for the parent message author
        def count_direct_replies(msg):
            author_id = msg.get("from_id")
            if author_id:
                author_id = str(author_id)
                for reply in msg.get("replies_data", []):
                    reply_author = reply.get("from_id")
                    if reply_author and str(reply_author) != author_id:
                        stats[author_id]["num_received_replies"] += 1
                    count_direct_replies(reply)

        for msg in messages:
            count_direct_replies(msg)

        print(f"  ✅ Calculated stats for {len(stats)} users")

    except Exception as e:
        print(f"  ⚠️  Warning: Could not calculate stats: {str(e)}")
        import traceback

        traceback.print_exc()

    return dict(stats)


def load_seed_data(seed_file):
    """Load all entries from seed file."""
    df = pd.read_csv(seed_file)
    return df.to_dict("records")


def load_scores(scores_file):
    """Load scores from output file."""
    if not scores_file.exists():
        print(f"  ⚠️  Warning: {scores_file} not found, using empty scores")
        return []

    df = pd.read_csv(scores_file)
    return df.to_dict("records")


def enrich_data(data, user_info, user_stats):
    """
    Enrich data entries with user info and stats.

    Args:
        data: List of dicts with 'i' (user_id) and 'v' (value)
        user_info: Dict mapping user_id -> {username, display_name, bio}
        user_stats: Dict mapping user_id -> stats

    Returns:
        List of enriched entries
    """
    enriched = []

    for entry in data:
        user_id = str(entry["i"])
        info = user_info.get(user_id, {})
        stats = user_stats.get(user_id, {})

        enriched_entry = {
            "i": user_id,
            "v": entry["v"],
            "username": info.get("username", ""),
            "display_name": info.get("display_name", user_id),
            "bio": info.get("bio", ""),
            "num_posts": stats.get("num_posts", 0),
            "num_received_reactions": stats.get("num_received_reactions", 0),
            "num_received_replies": stats.get("num_received_replies", 0),
            "num_given_replies": stats.get("num_given_replies", 0),
            "first_post_at": stats.get("first_post_at"),
            "last_post_at": stats.get("last_post_at"),
        }

        enriched.append(enriched_entry)

    return enriched


def generate_json_file(channel_id, seed_data, scores_data, output_file):
    """Generate JSON file with seed and scores data."""
    created_at = datetime.now(timezone.utc).isoformat()
    json_data = {
        "category": "socialrank",
        "channel": channel_id,
        "created_at": created_at,
        "seed": seed_data,
        "scores": scores_data,
    }

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(json_data, f, indent=2, ensure_ascii=False)

    print(f"  ✅ Created {output_file}")
    print(f"     Seed entries: {len(seed_data)}")
    print(f"     Score entries: {len(scores_data)}")


def load_config():
    """Load configuration from config.toml."""
    config_file = Path("config.toml")
    if not config_file.exists():
        print("❌ config.toml not found")
        return None

    with open(config_file, "r", encoding="utf-8") as f:
        return toml.load(f)


def get_channels_from_config(config):
    """Get list of channel IDs to process from config."""
    include = config.get("channels", {}).get("include", [])
    exclude = config.get("channels", {}).get("exclude", [])

    # Filter out excluded channels
    channels = [str(ch) for ch in include if ch not in exclude]
    return channels


def main():
    """Main execution function."""
    print("=" * 60)
    print("Generating JSON files for UI (Channel Mode)")
    print("=" * 60)
    print()
    print("Note: num_given_reactions is not available for channels")
    print("      (Telegram only provides aggregated reaction counts)")
    print()

    # Load config
    config = load_config()
    if config is None:
        return

    # Get channels to process
    channels = get_channels_from_config(config)
    if not channels:
        print("❌ No channels configured in config.toml")
        return

    # Define directories
    seed_dir = Path("seed")
    output_dir = Path("output")
    ui_dir = Path("ui")

    # Create ui directory if it doesn't exist
    ui_dir.mkdir(exist_ok=True)
    print(f"✓ Output directory: {ui_dir}/")
    print()

    print(f"Found {len(channels)} channel(s) in config.toml to process...")
    print()

    # Process each channel from config
    for channel_id in channels:
        seed_file = seed_dir / f"{channel_id}.csv"

        if not seed_file.exists():
            print(f"⚠️  Skipping channel {channel_id}: {seed_file} not found")
            print()
            continue

        print(f"Processing: {channel_id}")

        # Load user info
        user_info = load_user_info(channel_id)

        # Calculate user stats from messages (channel-specific)
        user_stats = calculate_channel_user_stats(channel_id)

        # Load seed data
        seed_data = load_seed_data(seed_file)
        print(f"  ✅ Loaded {len(seed_data)} seed entries")

        # Load scores
        scores_file = output_dir / f"{channel_id}.csv"
        scores_data = load_scores(scores_file)
        print(f"  ✅ Loaded {len(scores_data)} score entries")

        # Enrich data with user info and stats
        seed_data = enrich_data(seed_data, user_info, user_stats)
        scores_data = enrich_data(scores_data, user_info, user_stats)

        # Generate JSON file
        output_file = ui_dir / f"{channel_id}.json"
        generate_json_file(channel_id, seed_data, scores_data, output_file)
        print()

    print("=" * 60)
    print("✓ JSON generation complete!")
    print("=" * 60)
    print(f"\nJSON files saved to {ui_dir}/")


if __name__ == "__main__":
    main()
