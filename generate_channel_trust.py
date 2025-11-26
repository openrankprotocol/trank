#!/usr/bin/env python3
"""
Calculate Trust Scores for Telegram Channels
Generates local trust lists from channel message data based on weights in config.toml

This script is specifically designed for channel data where:
- Main posts have from_id: null (channel announcements)
- Replies are nested within replies_data
- Reactions may have a count field for aggregated reactions
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
                if username:
                    display_name = username
                elif first_name or last_name:
                    display_name = f"{first_name} {last_name}".strip()
                else:
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
            if username in display_to_user_id:
                mentioned_ids.append(display_to_user_id[username])

    return mentioned_ids


def flatten_replies(messages):
    """
    Flatten nested replies from channel messages into a single list.

    Channel messages have replies nested in replies_data, which may also have
    nested replies. This function extracts all replies recursively.

    Args:
        messages: List of channel message objects

    Returns:
        tuple: (flattened list of all reply messages with parent_msg_id added,
                dict mapping post_id -> list of reply authors in that thread)
    """
    all_replies = []
    # Track all users who participated in each thread (by main post id)
    thread_participants = defaultdict(list)

    def extract_replies(msg, root_post_id, parent_id=None):
        """Recursively extract replies from a message"""
        replies_data = msg.get("replies_data", [])
        for reply in replies_data:
            # Add reference to the message being replied to
            reply_copy = reply.copy()
            reply_copy["parent_msg_id"] = msg.get("id")
            reply_copy["parent_author_id"] = msg.get("from_id")
            reply_copy["root_post_id"] = root_post_id
            all_replies.append(reply_copy)

            # Track thread participants
            reply_author = reply.get("from_id")
            if reply_author:
                thread_participants[root_post_id].append(reply_author)

            # Recursively process nested replies
            extract_replies(reply, root_post_id, reply.get("id"))

    for msg in messages:
        extract_replies(msg, msg.get("id"))

    return all_replies, dict(thread_participants)


def build_message_lookup(messages, replies):
    """
    Build a lookup dictionary for all messages and replies.

    Args:
        messages: List of main channel messages
        replies: List of flattened replies

    Returns:
        dict: Mapping of message_id -> message object
    """
    lookup = {}

    # Add main messages
    for msg in messages:
        lookup[msg["id"]] = msg

    # Add replies
    for reply in replies:
        lookup[reply["id"]] = reply

    return lookup


def calculate_channel_trust_scores(messages, weights, user_id_to_display):
    """
    Calculate trust scores between users based on channel interactions.

    For channels:
    - Main posts often have from_id: null (channel announcements)
    - User interactions are primarily in replies_data
    - Reactions may have count field (aggregated) or user_id (individual)
    - Users participating in the same thread implicitly trust each other

    Args:
        messages: List of message objects
        weights: Dictionary of weight values from config
        user_id_to_display: Mapping of user_id -> display_name (for mentions)

    Returns:
        defaultdict: Trust scores mapping (user_i_id, user_j_id) -> total_score
    """
    trust_scores = defaultdict(float)

    reaction_weight = weights.get(
        "reaction_points", weights.get("reaction_weight", 1.0)
    )
    reply_weight = weights.get("reply_points", weights.get("reply_weight", 2.0))
    mention_weight = weights.get("mention_points", weights.get("mention_weight", 1.5))
    # Thread participation weight - lower than direct reply since it's implicit
    thread_participation_weight = weights.get(
        "thread_participation_points", reply_weight * 0.25
    )

    # Flatten all replies from channel messages and get thread participants
    all_replies, thread_participants = flatten_replies(messages)

    # Build message lookup for finding reply targets
    message_lookup = build_message_lookup(messages, all_replies)

    # Build a lookup for reply authors by message id
    reply_author_lookup = {}
    for reply in all_replies:
        reply_author_lookup[reply["id"]] = reply.get("from_id")

    # Process main channel messages
    for msg in messages:
        msg_author = msg.get("from_id")
        # Skip channel posts with no author (announcements)

        # Process reactions on main messages
        reactions = msg.get("reactions", [])
        for reaction in reactions:
            reactor_id = reaction.get("user_id")
            # For channel reactions with count but no user_id, we can't assign trust
            if reactor_id and msg_author and reactor_id != msg_author:
                trust_scores[(reactor_id, msg_author)] += reaction_weight

    # Process all replies
    for reply in all_replies:
        reply_author = reply.get("from_id")
        if not reply_author:
            continue

        # Process reactions on replies: reactor -> reply author
        reactions = reply.get("reactions", [])
        for reaction in reactions:
            reactor_id = reaction.get("user_id")
            if reactor_id and reactor_id != reply_author:
                trust_scores[(reactor_id, reply_author)] += reaction_weight

        # Process reply relationships
        # First check reply_to_msg_id (explicit reply to another message)
        reply_to_id = reply.get("reply_to_msg_id")
        if reply_to_id and reply_to_id in message_lookup:
            original_msg = message_lookup[reply_to_id]
            original_author = original_msg.get("from_id")
            if original_author and original_author != reply_author:
                trust_scores[(reply_author, original_author)] += reply_weight
        # If no explicit reply_to_msg_id, check parent_author_id (reply to thread)
        elif reply.get("parent_author_id"):
            parent_author = reply.get("parent_author_id")
            if parent_author and parent_author != reply_author:
                trust_scores[(reply_author, parent_author)] += reply_weight

        # Process mentions in reply text
        message_text = reply.get("message", "")
        mentioned_user_ids = extract_mentioned_user_ids(
            message_text, user_id_to_display
        )
        for mentioned_user_id in mentioned_user_ids:
            if mentioned_user_id != reply_author:
                trust_scores[(reply_author, mentioned_user_id)] += mention_weight

    # Process thread participation - users in the same thread trust each other
    # This captures implicit trust from engaging in the same conversation
    for post_id, participants in thread_participants.items():
        # Get unique participants
        unique_participants = list(set(participants))
        # Count how many times each participant engaged in this thread
        participant_counts = defaultdict(int)
        for p in participants:
            participant_counts[p] += 1

        # Create trust edges between participants
        # Weight is proportional to engagement (number of replies)
        for i, user_i in enumerate(unique_participants):
            for user_j in unique_participants[i + 1 :]:
                if user_i != user_j:
                    # Bidirectional trust based on co-participation
                    # Scale by minimum engagement of the two users
                    min_engagement = min(
                        participant_counts[user_i], participant_counts[user_j]
                    )
                    weight = thread_participation_weight * min_engagement
                    trust_scores[(user_i, user_j)] += weight
                    trust_scores[(user_j, user_i)] += weight

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

        # Aggregate scores for unique (i, j) pairs
        aggregated[(user_i_id, user_j_id)] += score

    # Write to CSV with user IDs
    with open(output_file, "w", encoding="utf-8") as f:
        f.write("i,j,v\n")
        for (user_i_id, user_j_id), score in sorted(aggregated.items()):
            f.write(f"{user_i_id},{user_j_id},{score}\n")

    return output_file, len(aggregated)


def count_channel_stats(messages):
    """
    Count statistics for channel messages.

    Args:
        messages: List of channel message objects

    Returns:
        dict: Statistics about the channel messages
    """
    stats = {
        "total_posts": len(messages),
        "posts_with_author": 0,
        "posts_without_author": 0,
        "total_replies": 0,
        "replies_with_author": 0,
        "total_reactions": 0,
        "reactions_with_user_id": 0,
    }

    for msg in messages:
        if msg.get("from_id"):
            stats["posts_with_author"] += 1
        else:
            stats["posts_without_author"] += 1

        # Count reactions on main post
        for reaction in msg.get("reactions", []):
            stats["total_reactions"] += reaction.get("count", 1)
            if reaction.get("user_id"):
                stats["reactions_with_user_id"] += 1

        # Count replies recursively
        def count_replies(replies_data):
            for reply in replies_data:
                stats["total_replies"] += 1
                if reply.get("from_id"):
                    stats["replies_with_author"] += 1
                for reaction in reply.get("reactions", []):
                    stats["total_reactions"] += reaction.get("count", 1)
                    if reaction.get("user_id"):
                        stats["reactions_with_user_id"] += 1
                count_replies(reply.get("replies_data", []))

        count_replies(msg.get("replies_data", []))

    return stats


def main():
    """Main entry point"""
    print("📊 Calculating Channel Trust Scores\n")

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
            print(f"✅ Loaded {len(messages)} channel posts")

            # Print channel statistics
            stats = count_channel_stats(messages)
            print(f"   📝 Posts with author: {stats['posts_with_author']}")
            print(
                f"   📢 Posts without author (announcements): {stats['posts_without_author']}"
            )
            print(f"   💬 Total replies: {stats['total_replies']}")
            print(f"   👤 Replies with author: {stats['replies_with_author']}")
            print(f"   ❤️  Total reactions: {stats['total_reactions']}")
            print(f"   🔗 Reactions with user_id: {stats['reactions_with_user_id']}")

            # Load user ID to display name mapping from CSV (optional, for mentions)
            user_id_to_display = load_user_ids_mapping(channel_id)
            if user_id_to_display:
                print(f"✅ Loaded {len(user_id_to_display)} user ID mappings")

            # Calculate trust scores
            trust_scores = calculate_channel_trust_scores(
                messages, trust_config, user_id_to_display
            )
            print(f"✅ Calculated {len(trust_scores)} trust edges")

            # Save to CSV
            output_file, edge_count = save_trust_csv(channel_id, trust_scores)
            print(f"💾 Saved {edge_count} aggregated edges to {output_file}")
            print()

        except FileNotFoundError as e:
            print(f"❌ Error: {e}")
            print(f"   Run 'python read_channel_messages.py' first to crawl messages\n")
            continue
        except Exception as e:
            print(f"❌ Error processing channel {channel_id}: {e}\n")
            import traceback

            traceback.print_exc()
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
        import traceback

        traceback.print_exc()
        sys.exit(1)
