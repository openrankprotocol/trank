#!/usr/bin/env python3
"""
Generate JSON files for UI from seed and output data.

This script:
1. Loads all entries from seed/ directory
2. Loads corresponding score files from output/ directory
3. Loads user mappings from raw/[channel_id]_user_ids.csv
4. Calculates user stats from raw/[channel_id]_messages.json
5. Creates JSON files for each channel with enriched user data

Usage:
    python3 generate_json.py

Requirements:
    - pandas (install with: pip install pandas)
    - CSV files in seed/ directory
    - CSV files in output/ directory (matching seed filenames)
    - CSV files in raw/ directory: [channel_id]_user_ids.csv
    - JSON files in raw/ directory: [channel_id]_messages.json

Output:
    - Creates ui/ directory if it doesn't exist
    - For each seed file (e.g., 1533865579.csv), creates:
      - ui/1533865579.json with seed and score data
"""

import json
from collections import defaultdict
from pathlib import Path

import pandas as pd


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


def calculate_user_stats(channel_id):
    """
    Calculate user stats from messages JSON file.

    Stats calculated per user:
    - num_posts: Number of messages/replies posted
    - num_received_reactions: Total reactions received on their messages
    - num_received_replies: Total replies under all messages authored by this user
    - num_given_reactions: Number of reactions they gave
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
            "num_given_reactions": 0,
            "num_given_replies": 0,
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

        # Second pass: calculate stats
        for msg in all_messages:
            author_id = msg.get("from_id")
            is_reply = msg.get("reply_to_msg_id") is not None or any(
                msg.get("id") in [r.get("id") for r in m.get("replies_data", [])]
                for m in messages
            )

            if author_id:
                author_id = str(author_id)
                # Count post
                stats[author_id]["num_posts"] += 1

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
            "num_given_reactions": stats.get("num_given_reactions", 0),
            "num_given_replies": stats.get("num_given_replies", 0),
        }

        enriched.append(enriched_entry)

    return enriched


def generate_json_file(channel_id, seed_data, scores_data, output_file):
    """Generate JSON file with seed and scores data."""
    json_data = {
        "category": "socialrank",
        "channel": channel_id,
        "seed": seed_data,
        "scores": scores_data,
    }

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(json_data, f, indent=2, ensure_ascii=False)

    print(f"  ✅ Created {output_file}")
    print(f"     Seed entries: {len(seed_data)}")
    print(f"     Score entries: {len(scores_data)}")


def main():
    """Main execution function."""
    print("=" * 60)
    print("Generating JSON files for UI")
    print("=" * 60)
    print()

    # Define directories
    seed_dir = Path("seed")
    output_dir = Path("output")
    ui_dir = Path("ui")

    # Create ui directory if it doesn't exist
    ui_dir.mkdir(exist_ok=True)
    print(f"✓ Output directory: {ui_dir}/")
    print()

    # Find all seed CSV files
    seed_files = sorted(list(seed_dir.glob("*.csv")))

    if not seed_files:
        print(f"❌ No CSV files found in {seed_dir}")
        return

    print(f"Found {len(seed_files)} seed file(s) to process...")
    print()

    # Process each seed file
    for seed_file in seed_files:
        base_name = seed_file.stem
        channel_id = base_name

        print(f"Processing: {base_name}")

        # Load user info
        user_info = load_user_info(channel_id)

        # Calculate user stats from messages
        user_stats = calculate_user_stats(channel_id)

        # Load seed data
        seed_data = load_seed_data(seed_file)
        print(f"  ✅ Loaded {len(seed_data)} seed entries")

        # Load scores
        scores_file = output_dir / f"{base_name}.csv"
        scores_data = load_scores(scores_file)
        print(f"  ✅ Loaded {len(scores_data)} score entries")

        # Enrich data with user info and stats
        seed_data = enrich_data(seed_data, user_info, user_stats)
        scores_data = enrich_data(scores_data, user_info, user_stats)

        # Generate JSON file
        output_file = ui_dir / f"{base_name}.json"
        generate_json_file(channel_id, seed_data, scores_data, output_file)
        print()

    print("=" * 60)
    print("✓ JSON generation complete!")
    print("=" * 60)
    print(f"\nJSON files saved to {ui_dir}/")


if __name__ == "__main__":
    main()
