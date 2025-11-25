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
    """
    Load user ID to display name mapping from CSV.

    Creates a mapping where each user_id maps to the best available display name.
    Display name fallback priority:
      1. username (e.g., "john_doe") - if user has set a Telegram username
      2. "first_name last_name" (e.g., "John Doe") - constructed from profile names
      3. user_id as string - fallback if no other info available

    Args:
        channel_id: Channel ID to load user mapping for

    Returns:
        dict: Mapping of user_id (int) -> display_name (str)
              Empty dict if mapping file not found
    """
    user_ids_file = Path(__file__).parent / "raw" / f"{channel_id}_user_ids.csv"

    if not user_ids_file.exists():
        print(f"⚠️  Warning: User IDs mapping not found: {user_ids_file}")
        print(f"   This is optional but recommended for better user identification")
        return {}

    mapping = {}
    with open(user_ids_file, "r", encoding="utf-8") as f:
        # Skip header: user_id,username,first_name,last_name
        next(f)
        for line in f:
            parts = line.strip().split(",")
            if len(parts) >= 4:
                user_id = int(parts[0])
                username = parts[1].strip()
                first_name = parts[2].strip()
                last_name = parts[3].strip()

                # Display name priority: username > full name > user_id
                # Note: Not all Telegram users have usernames (it's optional)
                if username:
                    # Best case: user has a username like @john_doe
                    display_name = username
                elif first_name or last_name:
                    # Fallback: construct name from first/last name
                    display_name = f"{first_name} {last_name}".strip()
                else:
                    # Last resort: use user_id as identifier
                    display_name = str(user_id)

                mapping[user_id] = display_name

    return mapping


def extract_mentioned_user_ids(text, user_id_to_display):
    """
    Extract mentioned user IDs from @mentions in message text.

    Args:
        text: Message text to search for mentions
        user_id_to_display: Mapping of user_id -> display_name

    Returns:
        list: List of user IDs that were mentioned
    """
    if not text:
        return []

    # Create reverse mapping: display_name -> user_id
    display_to_user_id = {display: uid for uid, display in user_id_to_display.items()}

    mentioned_ids = []
    words = text.split()
    for word in words:
        if word.startswith("@") and len(word) > 1:
            username = word[1:].strip(",.!?;:")
            # Try to find user ID for this username
            if username in display_to_user_id:
                mentioned_ids.append(display_to_user_id[username])

    return mentioned_ids


def calculate_trust_scores(messages, weights, user_id_to_display):
    """
    Calculate trust scores between users based on interactions.

    Args:
        messages: List of message objects
        weights: Dictionary of weight values from config
        user_id_to_display: Mapping of user_id -> display_name (for mentions)

    Returns:
        defaultdict: Trust scores mapping (user_i_id, user_j_id) -> total_score
    """
    # Store trust scores: (user_i_id, user_j_id) -> total_score
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
                # Trust edge: reactor -> msg_author (using user IDs)
                trust_scores[(reactor_id, msg_author)] += reaction_weight

        # Process replies: replier -> original message author
        reply_to_id = msg.get("reply_to_msg_id")
        if reply_to_id and reply_to_id in message_lookup:
            original_msg = message_lookup[reply_to_id]
            original_author = original_msg.get("from_id")
            if original_author and original_author != msg_author:
                # Trust edge: msg_author -> original_author (using user IDs)
                trust_scores[(msg_author, original_author)] += reply_weight

        # Process mentions: mentioner -> mentioned user
        message_text = msg.get("message", "")
        mentioned_user_ids = extract_mentioned_user_ids(
            message_text, user_id_to_display
        )
        for mentioned_user_id in mentioned_user_ids:
            if mentioned_user_id != msg_author:
                # Trust edge: msg_author -> mentioned_user_id (using user IDs)
                trust_scores[(msg_author, mentioned_user_id)] += mention_weight

    return trust_scores


def save_trust_csv(channel_id, trust_scores):
    """
    Save trust scores to CSV file in trust/ directory with user IDs.

    Args:
        channel_id: Channel ID
        trust_scores: Dictionary mapping (user_i_id, user_j_id) -> score

    Returns:
        tuple: (output_file_path, edge_count)
    """
    # Create trust directory if it doesn't exist
    trust_dir = Path(__file__).parent / "trust"
    trust_dir.mkdir(parents=True, exist_ok=True)

    output_file = trust_dir / f"{channel_id}.csv"

    # Aggregate scores by (i, j) pairs
    aggregated = defaultdict(float)

    for (user_i_id, user_j_id), score in trust_scores.items():
        # Skip if same user
        if user_i_id == user_j_id:
            continue

        # Aggregate scores for unique (i, j) pairs using user IDs
        aggregated[(user_i_id, user_j_id)] += score

    # Write to CSV with user IDs
    with open(output_file, "w", encoding="utf-8") as f:
        f.write("i,j,v\n")
        for (user_i_id, user_j_id), score in sorted(aggregated.items()):
            f.write(f"{user_i_id},{user_j_id},{score}\n")

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

            # Load user ID to display name mapping from CSV (optional, for mentions)
            user_id_to_display = load_user_ids_mapping(channel_id)
            if user_id_to_display:
                print(f"✅ Loaded {len(user_id_to_display)} user ID mappings")

            # Calculate trust scores (now using user IDs)
            trust_scores = calculate_trust_scores(
                messages, trust_config, user_id_to_display
            )
            print(f"✅ Calculated {len(trust_scores)} trust edges")

            # Save to CSV with user IDs
            output_file, edge_count = save_trust_csv(channel_id, trust_scores)
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
