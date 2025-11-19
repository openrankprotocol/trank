#!/usr/bin/env python3
"""
Calculate Trust Scores
Generates local trust lists from message data based on weights in config.toml
"""

import json
import sys
from collections import defaultdict
from pathlib import Path

# Use built-in tomllib for Python 3.11+, fallback to tomli for older versions
if sys.version_info >= (3, 11):
    import tomllib
else:
    try:
        import tomli as tomllib
    except ImportError:
        import toml as tomllib


def load_config():
    """Load configuration from config.toml"""
    config_path = Path(__file__).parent / "config.toml"

    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, "rb") as f:
        return tomllib.load(f)


def load_messages(channel_id):
    """Load messages from raw JSON file"""
    messages_file = Path(__file__).parent / "raw" / f"{channel_id}_messages.json"

    if not messages_file.exists():
        raise FileNotFoundError(f"Messages file not found: {messages_file}")

    with open(messages_file, "r", encoding="utf-8") as f:
        return json.load(f)


def load_user_ids_mapping(channel_id):
    """Load user ID to username mapping from CSV"""
    user_ids_file = Path(__file__).parent / "raw" / f"{channel_id}_user_ids.csv"

    if not user_ids_file.exists():
        print(f"⚠️  Warning: User IDs mapping not found: {user_ids_file}")
        print(f"   Run 'python get_user_ids.py' to create it")
        return {}

    mapping = {}
    with open(user_ids_file, "r", encoding="utf-8") as f:
        # Skip header
        next(f)
        for line in f:
            parts = line.strip().split(",")
            if len(parts) >= 2:
                user_id = int(parts[0])
                username = parts[1]
                if username:  # Only map if username exists
                    mapping[user_id] = username

    return mapping


def extract_mentions(text):
    """Extract @mentions from message text"""
    if not text:
        return []

    mentions = []
    words = text.split()
    for word in words:
        if word.startswith("@") and len(word) > 1:
            username = word[1:].strip(",.!?;:")
            mentions.append(username)

    return mentions


def calculate_trust_scores(messages, weights):
    """Calculate trust scores between users based on interactions"""
    # Store trust scores: (user_i, user_j) -> total_score
    trust_scores = defaultdict(float)

    reaction_weight = weights.get(
        "reaction_points", weights.get("reaction_weight", 1.0)
    )
    reply_weight = weights.get("reply_points", weights.get("reply_weight", 2.0))
    mention_weight = weights.get("mention_points", weights.get("mention_weight", 1.5))

    # Build message lookup for replies
    message_lookup = {msg["id"]: msg for msg in messages}

    for msg in messages:
        msg_author = msg.get("from_id")
        if not msg_author:
            continue

        # Process reactions: reactor -> message author
        reactions = msg.get("reactions", [])
        for reaction in reactions:
            reactor_id = reaction.get("user_id")
            if reactor_id and reactor_id != msg_author:
                # Trust edge: reactor -> msg_author
                trust_scores[(reactor_id, msg_author)] += reaction_weight

        # Process replies: replier -> original message author
        reply_to_id = msg.get("reply_to_msg_id")
        if reply_to_id and reply_to_id in message_lookup:
            original_msg = message_lookup[reply_to_id]
            original_author = original_msg.get("from_id")
            if original_author and original_author != msg_author:
                # Trust edge: msg_author -> original_author
                trust_scores[(msg_author, original_author)] += reply_weight

        # Process mentions: mentioner -> mentioned user
        message_text = msg.get("message", "")
        mentions = extract_mentions(message_text)
        for mentioned_username in mentions:
            # Store as username since we don't have user_id mapping here
            # Trust edge: msg_author -> mentioned_username
            trust_scores[(msg_author, mentioned_username)] += mention_weight

    return trust_scores


def save_trust_csv(channel_id, trust_scores, user_id_to_username):
    """Save trust scores to CSV file in trust/ directory"""
    # Create trust directory if it doesn't exist
    trust_dir = Path(__file__).parent / "trust"
    trust_dir.mkdir(parents=True, exist_ok=True)

    output_file = trust_dir / f"{channel_id}.csv"

    # Aggregate scores by (i, j) pairs
    aggregated = defaultdict(float)
    skipped_no_username = 0

    for (user_i, user_j), score in trust_scores.items():
        # Convert user IDs to usernames if available
        # Only include if BOTH users have usernames
        username_i = user_id_to_username.get(user_i)
        username_j = user_id_to_username.get(user_j)

        # Skip if either user doesn't have a username
        if not username_i or not username_j:
            skipped_no_username += 1
            continue

        # Skip if same user
        if username_i == username_j:
            continue

        # Aggregate scores for unique (i, j) pairs
        aggregated[(username_i, username_j)] += score

    # Write to CSV
    with open(output_file, "w", encoding="utf-8") as f:
        f.write("i,j,v\n")
        for (user_i, user_j), score in sorted(aggregated.items()):
            f.write(f"{user_i},{user_j},{score}\n")

    if skipped_no_username > 0:
        print(f"⚠️  Skipped {skipped_no_username} edges with missing usernames")

    return output_file, len(aggregated)


def main():
    """Main entry point"""
    print("📊 Calculating Trust Scores\n")

    # Load configuration
    config = load_config()
    channels_config = config.get("channels", {})
    trust_config = config.get("trust", {})

    if not trust_config:
        print("⚠️  Warning: No [trust] section found in config.toml")
        print("Using default weights:")
        trust_config = {
            "reaction_points": 30,
            "reply_points": 40,
            "mention_points": 50,
        }
        print(f"  reaction_points = {trust_config['reaction_points']}")
        print(f"  reply_points = {trust_config['reply_points']}")
        print(f"  mention_points = {trust_config['mention_points']}")
        print()
    else:
        print("Using weights from config.toml:")
        print(f"  reaction_points = {trust_config.get('reaction_points', 30)}")
        print(f"  reply_points = {trust_config.get('reply_points', 40)}")
        print(f"  mention_points = {trust_config.get('mention_points', 50)}")
        print()

    # Get channels to process
    channels = channels_config.get("include", [])
    if not channels:
        print("❌ Error: No channels configured in config.toml")
        sys.exit(1)

    print(f"Processing {len(channels)} channel(s)...\n")

    for channel_id in channels:
        print(f"{'=' * 80}")
        print(f"Channel ID: {channel_id}")
        print(f"{'=' * 80}")

        try:
            # Load messages
            messages = load_messages(channel_id)
            print(f"✅ Loaded {len(messages)} messages")

            # Load user ID to username mapping from CSV
            user_id_to_username = load_user_ids_mapping(channel_id)
            if user_id_to_username:
                print(f"✅ Loaded {len(user_id_to_username)} user ID mappings")
            else:
                print(f"⚠️  No user ID mappings found - output will be empty")

            # Calculate trust scores
            trust_scores = calculate_trust_scores(messages, trust_config)
            print(f"✅ Calculated {len(trust_scores)} trust edges")

            # Save to CSV
            output_file, edge_count = save_trust_csv(
                channel_id, trust_scores, user_id_to_username
            )
            print(f"💾 Saved {edge_count} aggregated edges to {output_file}")
            print()

        except FileNotFoundError as e:
            print(f"❌ Error: {e}")
            print(f"   Run 'python read_messages.py' first to crawl messages\n")
            continue
        except Exception as e:
            print(f"❌ Error processing channel {channel_id}: {e}\n")
            continue

    print("✅ Done!\n")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n⚠️  Interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)
