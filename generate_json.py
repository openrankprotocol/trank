#!/usr/bin/env python3
"""
Generate JSON files for UI from seed and output data.

This script:
1. Loads channel list from config.toml
2. Loads corresponding entries from seed/ directory
3. Loads corresponding score files from output/ directory
4. Loads user mappings from raw/[channel_id]_user_ids.csv
5. Calculates user stats from raw/[channel_id]_messages.json
6. Creates JSON files for each channel with enriched user data

Usage:
    python3 generate_json.py

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

        # Also load from admins file (for users who are admins but haven't posted)
        admins_file = Path(f"raw/{channel_id}_admins.csv")
        if admins_file.exists():
            admins_df = pd.read_csv(admins_file)
            added_from_admins = 0

            for _, row in admins_df.iterrows():
                user_id = str(row["user_id"])

                # Skip if we already have info for this user
                if user_id in user_info:
                    continue

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
                    "bio": "",  # Admins file doesn't have bio
                }
                added_from_admins += 1

            if added_from_admins > 0:
                print(f"  ✅ Added {added_from_admins} users from admins file")

    except Exception as e:
        print(f"  ⚠️  Warning: Could not load user info: {str(e)}")

    return user_info


def load_admins(channel_id):
    """
    Load admin user IDs from CSV file.

    Args:
        channel_id: Channel ID to load admins for

    Returns:
        set: Set of admin user IDs (as strings)
    """
    admins = set()

    try:
        admins_file = Path(f"raw/{channel_id}_admins.csv")

        if admins_file.exists():
            print(f"  👑 Loading admins from: {admins_file}")
            admins_df = pd.read_csv(admins_file)

            for _, row in admins_df.iterrows():
                user_id = str(row["user_id"])
                admins.add(user_id)

            print(f"  ✅ Loaded {len(admins)} admins")
        else:
            print(f"  ⚠️  Warning: {admins_file} not found, no admin info")

    except Exception as e:
        print(f"  ⚠️  Warning: Could not load admins: {str(e)}")

    return admins


def calculate_user_stats(channel_id):
    """
    Calculate user stats from messages JSON file.

    Stats calculated per user:
    - num_posts: Number of messages/replies posted
    - num_received_reactions: Total reactions received on their messages
    - num_received_replies: Total replies under all messages authored by this user
    - num_given_reactions: Number of reactions they gave
    - num_given_replies: Number of replies they made
    - first_post_at: Date-time of first post
    - last_post_at: Date-time of last post

    Args:
        channel_id: Channel ID to calculate stats for

    Returns:
        tuple: (stats dict, total_num_posts, first_message_at, last_message_at)
            - stats: Mapping of user_id (str) -> stats dict
            - total_num_posts: Total number of posts/messages
            - first_message_at: Timestamp of earliest message
            - last_message_at: Timestamp of most recent message
    """
    stats = defaultdict(
        lambda: {
            "num_posts": 0,
            "num_received_reactions": 0,
            "num_received_replies": 0,
            "num_given_reactions": 0,
            "num_given_replies": 0,
            "first_post_at": None,
            "last_post_at": None,
        }
    )

    messages_file = Path(f"raw/{channel_id}_messages.json")

    if not messages_file.exists():
        print(f"  ⚠️  Warning: {messages_file} not found, stats will be empty")
        return dict(stats), 0, None, None

    try:
        print(f"  📊 Calculating stats from: {messages_file}")

        with open(messages_file, "r", encoding="utf-8") as f:
            messages = json.load(f)

        # First pass: build message_id -> author_id map and collect all messages
        message_authors = {}
        all_messages = []

        def collect_messages(msg):
            """Collect all messages and build author map."""
            msg_id = msg.get("id")
            author_id = msg.get("from_id")
            if msg_id and author_id:
                message_authors[msg_id] = str(author_id)
            all_messages.append(msg)
            for reply in msg.get("replies_data", []):
                collect_messages(reply)

        for msg in messages:
            collect_messages(msg)

        # Track channel-wide first/last message timestamps
        channel_first_message_at = None
        channel_last_message_at = None

        # Second pass: calculate stats
        for msg in all_messages:
            author_id = msg.get("from_id")
            is_reply = msg.get("reply_to_msg_id") is not None or any(
                msg.get("id") in [r.get("id") for r in m.get("replies_data", [])]
                for m in messages
            )

            # Track channel-wide first/last message (regardless of author)
            msg_date = msg.get("date")
            if msg_date:
                if (
                    channel_first_message_at is None
                    or msg_date < channel_first_message_at
                ):
                    channel_first_message_at = msg_date
                if (
                    channel_last_message_at is None
                    or msg_date > channel_last_message_at
                ):
                    channel_last_message_at = msg_date

            if author_id:
                author_id = str(author_id)
                # Count post
                stats[author_id]["num_posts"] += 1

                # Track first and last post dates
                if msg_date:
                    current_first = stats[author_id]["first_post_at"]
                    current_last = stats[author_id]["last_post_at"]
                    if current_first is None or msg_date < current_first:
                        stats[author_id]["first_post_at"] = msg_date
                    if current_last is None or msg_date > current_last:
                        stats[author_id]["last_post_at"] = msg_date

                # If this message is a reply (has reply_to_msg_id), increment given_replies
                if msg.get("reply_to_msg_id") is not None:
                    stats[author_id]["num_given_replies"] += 1

            # Process reactions on this message
            reactions = msg.get("reactions", [])
            for reaction in reactions:
                reactor_id = reaction.get("user_id")
                if reactor_id:
                    reactor_id = str(reactor_id)
                    stats[reactor_id]["num_given_reactions"] += 1

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

        total_num_posts = len(all_messages)
        print(f"  ✅ Calculated stats for {len(stats)} users")
        print(f"  ✅ Total posts: {total_num_posts}")

    except Exception as e:
        print(f"  ⚠️  Warning: Could not calculate stats: {str(e)}")
        import traceback

        traceback.print_exc()
        return dict(stats), 0, None, None

    return (
        dict(stats),
        total_num_posts,
        channel_first_message_at,
        channel_last_message_at,
    )


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


def calculate_channel_engagement_score(total_num_posts, user_stats):
    """
    Calculate channel-wide engagement score between 0.0 and 1.0.

    Engagement score is based on the ratio of interactions (reactions + replies)
    to total posts. Higher ratio means more engaged community.

    Formula: min(1.0, (total_reactions + total_replies) / (total_posts * 2))

    Args:
        total_num_posts: Total number of posts in the channel
        user_stats: Dict mapping user_id -> stats

    Returns:
        float: Engagement score between 0.0 and 1.0
    """
    if total_num_posts == 0:
        return 0.0

    total_reactions = sum(
        s.get("num_received_reactions", 0) for s in user_stats.values()
    )
    total_replies = sum(s.get("num_received_replies", 0) for s in user_stats.values())

    # Normalize: if every post gets 2 interactions on average, score is 1.0
    raw_score = (total_reactions + total_replies) / (total_num_posts * 2)
    return min(1.0, round(raw_score, 4))


def enrich_data(data, user_info, user_stats, admins):
    """
    Enrich data entries with user info and stats.

    Args:
        data: List of dicts with 'i' (user_id) and 'v' (value)
        user_info: Dict mapping user_id -> {username, display_name, bio}
        user_stats: Dict mapping user_id -> stats
        admins: Set of admin user IDs

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
            "is_admin": user_id in admins,
            "num_posts": stats.get("num_posts", 0),
            "num_received_reactions": stats.get("num_received_reactions", 0),
            "num_received_replies": stats.get("num_received_replies", 0),
            "num_given_reactions": stats.get("num_given_reactions", 0),
            "num_given_replies": stats.get("num_given_replies", 0),
            "first_post_at": stats.get("first_post_at"),
            "last_post_at": stats.get("last_post_at"),
        }

        enriched.append(enriched_entry)

    return enriched


def generate_json_file(
    channel_id,
    seed_data,
    scores_data,
    output_file,
    days_back,
    total_num_posts,
    total_users,
    first_message_at,
    last_message_at,
    engagement_score,
):
    """Generate JSON file with seed and scores data."""
    created_at = datetime.now(timezone.utc).isoformat()
    json_data = {
        "category": "socialrank",
        "channel": channel_id,
        "created_at": created_at,
        "days_back": days_back,
        "total_num_posts": total_num_posts,
        "total_users": total_users,
        "engagement_score": engagement_score,
        "first_message_at": first_message_at,
        "last_message_at": last_message_at,
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
    """Get list of group chat IDs to process from config."""
    include = config.get("group_chats", {}).get("include", [])
    exclude = config.get("group_chats", {}).get("exclude", [])

    # Filter out excluded channels
    channels = [str(ch) for ch in include if ch not in exclude]
    return channels


def main():
    """Main execution function."""
    print("=" * 60)
    print("Generating JSON files for UI")
    print("=" * 60)
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

        # Load admins
        admins = load_admins(channel_id)

        # Calculate user stats from messages
        user_stats, total_num_posts, first_message_at, last_message_at = (
            calculate_user_stats(channel_id)
        )

        # Load seed data
        seed_data = load_seed_data(seed_file)
        print(f"  ✅ Loaded {len(seed_data)} seed entries")

        # Load scores
        scores_file = output_dir / f"{channel_id}.csv"
        scores_data = load_scores(scores_file)
        print(f"  ✅ Loaded {len(scores_data)} score entries")

        # Enrich data with user info and stats
        seed_data = enrich_data(seed_data, user_info, user_stats, admins)
        scores_data = enrich_data(scores_data, user_info, user_stats, admins)

        # Get days_back from config
        days_back = config.get("crawler", {}).get("time_window_days", 0)

        # Calculate total users (unique users in scores)
        total_users = len(scores_data)

        # Calculate channel-wide engagement score
        engagement_score = calculate_channel_engagement_score(
            total_num_posts, user_stats
        )

        # Generate JSON file
        output_file = ui_dir / f"{channel_id}.json"
        generate_json_file(
            channel_id,
            seed_data,
            scores_data,
            output_file,
            days_back,
            total_num_posts,
            total_users,
            first_message_at,
            last_message_at,
            engagement_score,
        )
        print()

    print("=" * 60)
    print("✓ JSON generation complete!")
    print("=" * 60)
    print(f"\nJSON files saved to {ui_dir}/")


if __name__ == "__main__":
    main()
