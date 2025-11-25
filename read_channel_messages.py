#!/usr/bin/env python3
"""
Telegram Channel Crawler
Simple script to fetch and archive messages from Telegram channels.
Uses phone number login (no session strings needed).
"""

import asyncio
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Generator, List, TypeVar, Union


class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder that handles datetime objects."""

    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, bytes):
            return obj.decode("utf-8", errors="ignore")
        return super().default(obj)


from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.tl.types import Message

# Load environment variables
load_dotenv()

# Use built-in tomllib for Python 3.11+, fallback to tomli for older versions
if sys.version_info >= (3, 11):
    import tomllib
else:
    try:
        import tomli as tomllib
    except ImportError:
        import toml as tomllib

T = TypeVar("T")


def load_config():
    """Load configuration from config.toml"""
    config_path = Path(__file__).parent / "config.toml"

    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    # Load TOML file (always use binary mode for tomllib/tomli)
    with open(config_path, "rb") as f:
        return tomllib.load(f)


def chunk_array(array: List[T], chunk_size: int) -> Generator[List[T], None, None]:
    """Split an array into chunks of the specified size."""
    for i in range(0, len(array), chunk_size):
        yield array[i : i + chunk_size]


def save_checkpoint(channel: int, messages: List[dict], user_info: dict, config: dict):
    """Save checkpoint data to resume processing if interrupted."""
    checkpoint_dir = Path("raw/checkpoints")
    checkpoint_dir.mkdir(parents=True, exist_ok=True)

    checkpoint_file = checkpoint_dir / f"{channel}_checkpoint.json"

    output_config = config.get("output", {})
    indent_spaces = output_config.get("indent_spaces", 2)

    checkpoint_data = {
        "channel": channel,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "message_count": len(messages),
        "last_message_id": messages[-1]["id"] if messages else None,
        "messages": messages,
        "user_info": user_info,
    }

    with open(checkpoint_file, "w", encoding="utf-8") as f:
        json.dump(checkpoint_data, f, indent=indent_spaces, ensure_ascii=False)

    print(f"💾 Checkpoint saved: {len(messages)} messages to {checkpoint_file}")


def load_checkpoint(channel: int) -> dict:
    """Load checkpoint data if it exists."""
    checkpoint_file = Path("raw/checkpoints") / f"{channel}_checkpoint.json"

    if checkpoint_file.exists():
        with open(checkpoint_file, "r", encoding="utf-8") as f:
            return json.load(f)
    return None


def clear_checkpoint(channel: int):
    """Remove checkpoint file after successful completion."""
    checkpoint_file = Path("raw/checkpoints") / f"{channel}_checkpoint.json"
    if checkpoint_file.exists():
        checkpoint_file.unlink()
        print(f"🗑️  Checkpoint cleared for channel {channel}")


async def fetch_replies_recursive(
    client: TelegramClient,
    channel: int,
    message_id: int,
    user_info: dict,
    rate_delay: float,
    current_depth: int,
    max_depth: int,
    discussion_group=None,
) -> list:
    """
    Recursively fetch replies to a message, up to max_depth levels.

    Args:
        client: Telegram client
        channel: Channel ID
        message_id: ID of message to fetch replies for
        user_info: Dictionary to collect user info into
        rate_delay: Delay between requests
        current_depth: Current recursion depth
        max_depth: Maximum recursion depth
        discussion_group: Discussion group ID (if channel has linked discussion)

    Returns:
        list: List of reply messages with nested replies
    """
    if current_depth >= max_depth:
        return []

    replies_list = []

    try:
        # Use discussion group if available, otherwise use channel
        peer = discussion_group if discussion_group else channel

        # Get replies to this message
        try:
            replies = await client.get_messages(peer, reply_to=message_id, limit=100)

        except Exception as e:
            # If reply_to fails, silently skip (deleted messages, gaps in IDs, etc.)
            if "invalid" in str(e).lower():
                # For depth 0 (direct replies to channel posts), try alternative method
                # Fetch recent messages and filter by those replying to this channel
                if current_depth == 0 and discussion_group:
                    return await fetch_channel_post_replies(
                        client,
                        channel,
                        message_id,
                        user_info,
                        rate_delay,
                        max_depth,
                        discussion_group,
                    )
                return []
            raise

        if replies:
            print(
                f"     {'  ' * current_depth}└─ Found {len(replies)} replies at depth {current_depth}"
            )

        for reply in replies:
            if not reply:
                continue

            # Collect user info from reply sender
            if reply.from_id and hasattr(reply.from_id, "user_id"):
                reply_user_id = reply.from_id.user_id
                if reply_user_id not in user_info:
                    try:
                        user = await client.get_entity(reply_user_id)
                        user_info[reply_user_id] = {
                            "username": user.username or "",
                            "first_name": user.first_name or "",
                            "last_name": user.last_name or "",
                        }
                    except Exception:
                        pass

            # Get reactions for this reply
            reply_reactions = []
            if reply.reactions and reply.reactions.results:
                try:
                    from telethon.tl.functions.messages import (
                        GetMessageReactionsListRequest,
                    )

                    for reaction_result in reply.reactions.results:
                        emoji = (
                            reaction_result.reaction.emoticon
                            if hasattr(reaction_result.reaction, "emoticon")
                            else None
                        )
                        if emoji:
                            result = await client(
                                GetMessageReactionsListRequest(
                                    peer=channel,
                                    id=reply.id,
                                    reaction=reaction_result.reaction,
                                    limit=100,
                                )
                            )
                            for reaction_peer in result.reactions:
                                reactor_id = (
                                    reaction_peer.peer_id.user_id
                                    if hasattr(reaction_peer.peer_id, "user_id")
                                    else None
                                )
                                if reactor_id:
                                    reply_reactions.append(
                                        {"user_id": reactor_id, "emoji": emoji}
                                    )
                                    # Collect user info
                                    if reactor_id not in user_info:
                                        try:
                                            user = await client.get_entity(reactor_id)
                                            user_info[reactor_id] = {
                                                "username": user.username or "",
                                                "first_name": user.first_name or "",
                                                "last_name": user.last_name or "",
                                            }
                                        except Exception:
                                            pass
                except Exception:
                    # Fallback to basic reaction info
                    for reaction_result in reply.reactions.results:
                        emoji = (
                            reaction_result.reaction.emoticon
                            if hasattr(reaction_result.reaction, "emoticon")
                            else None
                        )
                        if emoji:
                            reply_reactions.append(
                                {
                                    "user_id": None,
                                    "emoji": emoji,
                                    "count": reaction_result.count,
                                }
                            )

            # Recursively fetch nested replies to this reply
            # Always try to fetch nested replies (up to max_depth)
            nested_replies = []
            if current_depth + 1 < max_depth:
                print(
                    f"     {'  ' * current_depth}├─ Checking for nested replies to message {reply.id}..."
                )
                nested_replies = await fetch_replies_recursive(
                    client,
                    channel,
                    reply.id,
                    user_info,
                    rate_delay,
                    current_depth + 1,
                    max_depth,
                    discussion_group,
                )
                if nested_replies:
                    print(
                        f"     {'  ' * current_depth}├─ ✓ Found {len(nested_replies)} nested replies"
                    )
                else:
                    print(f"     {'  ' * current_depth}├─ No nested replies found")

            # Add simplified reply to list
            replies_list.append(
                {
                    "id": reply.id,
                    "date": reply.date.isoformat() if reply.date else None,
                    "from_id": reply.from_id.user_id
                    if hasattr(reply.from_id, "user_id")
                    else None,
                    "message": reply.message,
                    "reply_to_msg_id": reply.reply_to.reply_to_msg_id
                    if reply.reply_to
                    else None,
                    "reactions": reply_reactions,
                    "replies_count": len(nested_replies),
                    "replies_data": nested_replies,
                }
            )

        await asyncio.sleep(rate_delay)  # Rate limiting

    except Exception as e:
        error_msg = str(e)
        if "invalid" in error_msg.lower() or "message id" in error_msg.lower():
            # This is expected for some messages (e.g., forwarded, deleted, or from main channel)
            print(
                f"     {'  ' * current_depth}⚠️  Skipping message {message_id} (invalid ID or not in discussion group)"
            )
        else:
            print(
                f"     {'  ' * current_depth}⚠️  Could not fetch replies for message {message_id}: {e}"
            )

    return replies_list


async def fetch_channel_post_replies(
    client: TelegramClient,
    channel: int,
    post_id: int,
    user_info: dict,
    rate_delay: float,
    max_depth: int,
    discussion_group: int,
) -> list:
    """
    Fetch ALL messages from discussion group - they are all replies to channel posts.

    Discussion groups only exist for channel comments, so all messages there
    are replies to channel posts.
    """
    replies_list = []

    try:
        # Get ALL messages from discussion group (up to 1000)
        # Since discussion groups only contain replies to channel posts,
        # we treat all messages as replies
        messages = await client.get_messages(discussion_group, limit=1000)
        print(f"     └─ Fetched {len(messages)} total messages from discussion group")

        # Process ALL messages as replies
        for msg in messages:
            # Collect user info from message author
            if msg.from_id and hasattr(msg.from_id, "user_id"):
                reply_user_id = msg.from_id.user_id
                if reply_user_id not in user_info:
                    try:
                        user = await client.get_entity(reply_user_id)
                        user_info[reply_user_id] = {
                            "username": user.username or "",
                            "first_name": user.first_name or "",
                            "last_name": user.last_name or "",
                        }
                    except Exception:
                        pass

            # Get reactions for this message
            reply_reactions = []
            if msg.reactions and msg.reactions.results:
                try:
                    from telethon.tl.functions.messages import (
                        GetMessageReactionsListRequest,
                    )

                    for reaction_result in msg.reactions.results:
                        emoji = (
                            reaction_result.reaction.emoticon
                            if hasattr(reaction_result.reaction, "emoticon")
                            else None
                        )
                        if emoji:
                            result = await client(
                                GetMessageReactionsListRequest(
                                    peer=discussion_group,
                                    id=msg.id,
                                    reaction=reaction_result.reaction,
                                    limit=100,
                                )
                            )
                            for reaction_peer in result.reactions:
                                reactor_id = (
                                    reaction_peer.peer_id.user_id
                                    if hasattr(reaction_peer.peer_id, "user_id")
                                    else None
                                )
                                if reactor_id:
                                    reply_reactions.append(
                                        {
                                            "user_id": reactor_id,
                                            "emoji": emoji,
                                        }
                                    )
                                    # Collect user info
                                    if reactor_id not in user_info:
                                        try:
                                            user = await client.get_entity(reactor_id)
                                            user_info[reactor_id] = {
                                                "username": user.username or "",
                                                "first_name": user.first_name or "",
                                                "last_name": user.last_name or "",
                                            }
                                        except Exception:
                                            pass
                except Exception:
                    # Fallback to basic reaction info
                    for reaction_result in msg.reactions.results:
                        emoji = (
                            reaction_result.reaction.emoticon
                            if hasattr(reaction_result.reaction, "emoticon")
                            else None
                        )
                        if emoji:
                            reply_reactions.append(
                                {
                                    "user_id": None,
                                    "emoji": emoji,
                                    "count": reaction_result.count,
                                }
                            )

            # Add all messages to replies_list
            replies_list.append(
                {
                    "id": msg.id,
                    "date": msg.date.isoformat() if msg.date else None,
                    "from_id": msg.from_id.user_id
                    if hasattr(msg.from_id, "user_id")
                    else None,
                    "message": msg.message,
                    "reply_to_msg_id": msg.reply_to.reply_to_msg_id
                    if msg.reply_to
                    else None,
                    "reactions": reply_reactions,
                    "replies_count": 0,  # We're getting all messages flat
                    "replies_data": [],  # No nested structure for now
                }
            )

        print(
            f"     └─ Collected {len(replies_list)} messages as replies (all messages from discussion group)"
        )

        await asyncio.sleep(rate_delay)

    except Exception as e:
        print(f"     ⚠️  Could not fetch discussion group messages: {e}")

    return replies_list


async def fetch_channel_messages(client: TelegramClient, channel: int, config: dict):
    """Fetch messages from a Telegram channel within the configured time window."""
    crawler_config = config.get("crawler", {})
    channels_config = config.get("channels", {})

    # Check if channel is excluded
    excluded_channels = channels_config.get("exclude", [])
    if channel in excluded_channels:
        print(f"⏭️  Skipping excluded channel: {channel}")
        return {"channel": channel, "messages": [], "skipped": True}

    time_window_days = crawler_config.get("time_window_days", 3)
    max_messages = crawler_config.get("max_messages_per_channel", 2000)
    rate_delay = crawler_config.get("rate_limiting_delay", 0.5)
    batch_size = crawler_config.get(
        "batch_size", 500
    )  # Fetch messages in batches (default 500)
    checkpoint_interval = crawler_config.get(
        "checkpoint_interval", 100
    )  # Save every N messages
    fetch_replies = crawler_config.get("fetch_replies", True)  # Fetch replies to posts
    max_reply_depth = crawler_config.get(
        "max_reply_depth", 2
    )  # Maximum depth for nested replies

    offset_date = datetime.now(timezone.utc) - timedelta(days=time_window_days)

    # Check for existing checkpoint
    checkpoint = load_checkpoint(channel)
    if checkpoint:
        print(
            f"📂 Found checkpoint for channel {channel} with {checkpoint['message_count']} messages"
        )
        print(f"   Last saved: {checkpoint['timestamp']}")
        resume = input("   Resume from checkpoint? (y/n): ").strip().lower()
        if resume == "y":
            messages_with_reactions = checkpoint["messages"]
            user_info = checkpoint["user_info"]
            offset_id = checkpoint["last_message_id"]
            total_fetched = len(messages_with_reactions)
            print(f"✅ Resuming from message ID {offset_id}")
        else:
            messages_with_reactions = []
            user_info = {}
            offset_id = 0
            total_fetched = 0
            clear_checkpoint(channel)
    else:
        messages_with_reactions = []
        user_info = {}  # Track user_id -> {username, first_name, last_name}
        offset_id = 0
        total_fetched = 0

    try:
        print(f"📥 Fetching messages from {channel} in batches of {batch_size}...")
        print(f"  ⏰ Time window: fetching messages since {offset_date.isoformat()}")

        if offset_id > 0:
            batch_number = (total_fetched // batch_size) + 1
        else:
            batch_number = 1

        while total_fetched < max_messages:
            # Calculate how many messages to fetch in this batch
            messages_to_fetch = min(batch_size, max_messages - total_fetched)

            # Fetch a batch of messages (starting from most recent, going backwards)
            batch_messages = await client.get_messages(
                channel,
                limit=messages_to_fetch,
                offset_id=offset_id,
            )

            if not batch_messages:
                # No more messages to fetch
                break

            print(f"  📦 Batch {batch_number}: Fetched {len(batch_messages)} messages")
            if batch_messages:
                first_msg_date = (
                    batch_messages[0].date.isoformat()
                    if batch_messages[0].date
                    else "None"
                )
                last_msg_date = (
                    batch_messages[-1].date.isoformat()
                    if batch_messages[-1].date
                    else "None"
                )
                print(f"     First message date: {first_msg_date}")
                print(f"     Last message date: {last_msg_date}")

            batch_processed_count = 0
            reached_time_limit = False

            # Get discussion group from first message with replies
            # (replies.channel_id contains the discussion group ID)
            discussion_group = None
            if batch_number == 1:  # Only check once
                for msg in batch_messages:
                    if msg.replies and hasattr(msg.replies, "channel_id"):
                        discussion_group = msg.replies.channel_id
                        print(
                            f"  💬 Found discussion group from replies: {discussion_group}"
                        )
                        break

            for idx, message in enumerate(batch_messages, 1):
                # Check if message is within time window
                if message.date and message.date < offset_date:
                    # We've gone past our time window, stop fetching
                    print(
                        f"  ⏹️  Reached end of time window at message {message.id} (date: {message.date.isoformat()})"
                    )
                    print(
                        f"     Processed {batch_processed_count} messages from this batch before time limit"
                    )
                    reached_time_limit = True
                    break

                batch_processed_count += 1

                # Collect user info from message sender
                if message.from_id and hasattr(message.from_id, "user_id"):
                    user_id = message.from_id.user_id
                    if user_id not in user_info:
                        try:
                            user = await client.get_entity(user_id)
                            user_info[user_id] = {
                                "username": user.username or "",
                                "first_name": user.first_name or "",
                                "last_name": user.last_name or "",
                            }
                        except Exception:
                            # If we can't get user info, just skip it
                            pass

                # Fetch replies to this post if enabled and replies exist
                post_replies = []
                if fetch_replies and message.replies and message.replies.replies > 0:
                    try:
                        post_replies = await fetch_replies_recursive(
                            client,
                            channel,
                            message.id,
                            user_info,
                            rate_delay,
                            current_depth=0,
                            max_depth=max_reply_depth,
                            discussion_group=discussion_group,
                        )
                    except Exception as e:
                        print(
                            f"     ⚠️  Error fetching replies for post {message.id}: {e}"
                        )
                        post_replies = []

                # Show progress for reactions and replies fetching
                if idx % 50 == 0 or idx == len(batch_messages):
                    print(
                        f"     Processing message {idx}/{len(batch_messages)} (reactions & replies)..."
                    )

                # Fetch detailed reactions with user IDs for this message
                reaction_details = []
                if message.reactions and message.reactions.results:
                    try:
                        # Get message reactions with user details using GetMessageReactionsList
                        from telethon.tl.functions.messages import (
                            GetMessageReactionsListRequest,
                        )

                        for reaction_result in message.reactions.results:
                            emoji = (
                                reaction_result.reaction.emoticon
                                if hasattr(reaction_result.reaction, "emoticon")
                                else None
                            )

                            if emoji:
                                # Get users who reacted with this specific emoji
                                result = await client(
                                    GetMessageReactionsListRequest(
                                        peer=channel,
                                        id=message.id,
                                        reaction=reaction_result.reaction,
                                        limit=100,
                                    )
                                )

                                # Extract user IDs from the reaction list
                                for reaction_peer in result.reactions:
                                    user_id = (
                                        reaction_peer.peer_id.user_id
                                        if hasattr(reaction_peer.peer_id, "user_id")
                                        else None
                                    )
                                    if user_id:
                                        reaction_details.append(
                                            {"user_id": user_id, "emoji": emoji}
                                        )
                                        # Collect user info from reactions
                                        if user_id not in user_info:
                                            try:
                                                user = await client.get_entity(user_id)
                                                user_info[user_id] = {
                                                    "username": user.username or "",
                                                    "first_name": user.first_name or "",
                                                    "last_name": user.last_name or "",
                                                }
                                            except Exception:
                                                pass
                    except Exception as e:
                        # Fallback to basic reaction without user IDs if detailed fetch fails
                        for reaction_result in message.reactions.results:
                            emoji = (
                                reaction_result.reaction.emoticon
                                if hasattr(reaction_result.reaction, "emoticon")
                                else None
                            )
                            if emoji:
                                reaction_details.append(
                                    {
                                        "user_id": None,
                                        "emoji": emoji,
                                        "count": reaction_result.count,
                                    }
                                )

                # Convert message to simplified format immediately for checkpoint
                simplified_msg = {
                    "id": message.id,
                    "date": message.date.isoformat() if message.date else None,
                    "from_id": message.from_id.user_id
                    if hasattr(message.from_id, "user_id")
                    else None,
                    "message": message.message,
                    "reply_to_msg_id": message.reply_to.reply_to_msg_id
                    if message.reply_to
                    else None,
                    "reactions": reaction_details if reaction_details else [],
                    "replies_count": message.replies.replies
                    if message.replies
                    else None,
                    "replies_data": post_replies if post_replies else [],
                }

                messages_with_reactions.append(simplified_msg)

            total_fetched += batch_processed_count

            # Save checkpoint periodically
            if checkpoint_interval > 0 and total_fetched % checkpoint_interval == 0:
                save_checkpoint(channel, messages_with_reactions, user_info, config)

            # If we reached the time limit, stop fetching more batches
            if reached_time_limit:
                break

            # Rate limiting delay between batches
            await asyncio.sleep(rate_delay)

            # Update offset_id to the ID of the last (oldest) message in this batch
            offset_id = batch_messages[-1].id
            batch_number += 1

            # If we got fewer messages than requested, we've reached the end
            if len(batch_messages) < messages_to_fetch:
                break

        print(
            f"✅ Fetched {len(messages_with_reactions)} messages from {channel} in {batch_number} batches"
        )
        print(f"👥 Collected info for {len(user_info)} unique users")

        # Clear checkpoint after successful completion
        clear_checkpoint(channel)

        return {
            "channel": channel,
            "messages": messages_with_reactions,
            "user_info": user_info,
            "skipped": False,
        }

    except Exception as e:
        print(f"❌ Error fetching messages from {channel}: {e}")
        return {"channel": channel, "messages": [], "error": str(e)}


async def save_messages(results: list, config: dict):
    """Save fetched messages to JSON files."""
    output_config = config.get("output", {})
    pretty_print = output_config.get("pretty_print", True)
    indent_spaces = output_config.get("indent_spaces", 2)

    # Create raw directory if it doesn't exist
    output_dir = Path("raw")
    output_dir.mkdir(parents=True, exist_ok=True)

    for result in results:
        # Skip excluded or errored channels
        if result.get("skipped", False) or result.get("error"):
            continue

        # Save user info to CSV if available
        user_info = result.get("user_info", {})
        if user_info:
            csv_filename = output_dir / f"{result['channel']}_user_ids.csv"
            with open(csv_filename, "w", encoding="utf-8") as f:
                f.write("user_id,username,first_name,last_name\n")
                for user_id, info in sorted(user_info.items()):
                    username = info.get("username", "")
                    first_name = info.get("first_name", "").replace(",", " ")
                    last_name = info.get("last_name", "").replace(",", " ")
                    f.write(f"{user_id},{username},{first_name},{last_name}\n")
            print(f"👥 Saved {len(user_info)} users to {csv_filename}")

        # Messages are already in simplified format from fetch_channel_messages
        serializable_messages = result["messages"]

        # Save to raw directory with _messages.json suffix
        filename = output_dir / f"{result['channel']}_messages.json"
        indent = indent_spaces if pretty_print else None

        with open(filename, "w", encoding="utf-8") as f:
            json.dump(
                serializable_messages,
                f,
                indent=indent,
                ensure_ascii=False,
            )

        print(f"💾 Saved {len(serializable_messages)} messages to {filename}")


async def main():
    """Main entry point for the Telegram crawler."""
    print("🚀 Starting Telegram Channel Crawler\n")

    # Load configuration
    config = load_config()
    crawler_config = config.get("crawler", {})
    channels_config = config.get("channels", {})

    # Get environment variables
    api_id = int(os.getenv("TELEGRAM_APP_ID", "0"))
    api_hash = os.getenv("TELEGRAM_APP_HASH", "")
    phone = os.getenv("TELEGRAM_PHONE", "")

    if not api_id or not api_hash:
        print("❌ Error: Missing Telegram credentials in .env file")
        print("Please set TELEGRAM_APP_ID and TELEGRAM_APP_HASH")
        print("\nGet your credentials from: https://my.telegram.org")
        sys.exit(1)

    # Get channels to crawl (must be integers)
    channels = channels_config.get("include", [])
    if not channels:
        print("❌ Error: No channels configured in config.toml")
        sys.exit(1)

    # Validate that all channels are integers
    for ch in channels:
        if not isinstance(ch, int):
            print(f"❌ Error: Channel '{ch}' is not a valid ID (must be an integer)")
            print("Use 'python list_channels.py' to see your channel IDs")
            sys.exit(1)

    parallel_requests = crawler_config.get("parallel_requests", 3)

    # Initialize Telegram client with session file
    session_file = "telegram_session"
    client = TelegramClient(session_file, api_id, api_hash)

    try:
        print("🔌 Connecting to Telegram...")
        await client.start(
            phone=lambda: phone
            or input(
                "📱 Enter your phone number (with country code, e.g., +1234567890): "
            ),
            password=lambda: input("🔒 Enter your 2FA password (if enabled): "),
            code_callback=lambda: input("💬 Enter the code Telegram sent you: "),
        )

        if not await client.is_user_authorized():
            print("❌ Authorization failed. Please try again.")
            sys.exit(1)

        me = await client.get_me()
        print(f"✅ Connected as: {me.first_name} (@{me.username or 'no username'})\n")

        # Process channels in parallel batches
        all_results = []

        for chunk in chunk_array(channels, parallel_requests):
            tasks = [
                fetch_channel_messages(client, channel, config) for channel in chunk
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Separate successful results from errors
            for result in results:
                if isinstance(result, Exception):
                    print(f"❌ Error: {result}")
                else:
                    all_results.append(result)

        # Save all messages
        print()
        await save_messages(all_results, config)

        print("\n✅ Crawling complete!")

    except Exception as e:
        print(f"❌ Fatal error: {e}")
        sys.exit(1)

    finally:
        if client.is_connected():
            await client.disconnect()
            print("🔌 Disconnected from Telegram")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⚠️  Interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)
