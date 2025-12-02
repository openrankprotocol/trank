#!/usr/bin/env python3
"""
List Telegram Channels
Shows all channels/chats the logged-in user has access to.
Saves channel metadata to raw/channels.json (only for channels in config.toml)
"""

import asyncio
import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.tl.types import Channel, Chat, User

# Use built-in tomllib for Python 3.11+, fallback to tomli for older versions
if sys.version_info >= (3, 11):
    import tomllib
else:
    try:
        import tomli as tomllib
    except ImportError:
        import toml as tomllib

# Load environment variables
load_dotenv()


def load_config():
    """Load configuration from config.toml"""
    config_path = Path(__file__).parent / "config.toml"

    if not config_path.exists():
        return None

    with open(config_path, "rb") as f:
        return tomllib.load(f)


async def main():
    """List all channels/chats the user has access to."""
    print("📋 Listing Your Telegram Channels\n")

    # Load config to filter which channels to save
    config = load_config()
    config_channel_ids = set()
    config_group_ids = set()
    if config:
        config_group_ids = set(config.get("group_chats", {}).get("include", []))
        config_channel_ids = set(config.get("channels", {}).get("include", []))

    # Get environment variables
    api_id = int(os.getenv("TELEGRAM_APP_ID", "0"))
    api_hash = os.getenv("TELEGRAM_APP_HASH", "")
    phone = os.getenv("TELEGRAM_PHONE", "")

    if not api_id or not api_hash:
        print("❌ Error: Missing Telegram credentials in .env file")
        print("Please set TELEGRAM_APP_ID and TELEGRAM_APP_HASH")
        sys.exit(1)

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

        print("=" * 80)
        print("Fetching your channels and chats...")
        print("=" * 80)

        # Get all dialogs (chats/channels)
        dialogs = await client.get_dialogs()

        channels = []
        groups = []
        users = []

        for dialog in dialogs:
            entity = dialog.entity

            if isinstance(entity, Channel):
                if entity.broadcast:
                    # It's a channel
                    channels.append(
                        {
                            "title": entity.title,
                            "username": entity.username,
                            "id": entity.id,
                            "access_hash": entity.access_hash,
                        }
                    )
                else:
                    # It's a supergroup
                    groups.append(
                        {
                            "title": entity.title,
                            "username": entity.username,
                            "id": entity.id,
                            "access_hash": entity.access_hash,
                        }
                    )
            elif isinstance(entity, Chat):
                # Regular group
                groups.append(
                    {
                        "title": entity.title,
                        "username": None,
                        "id": entity.id,
                        "access_hash": None,
                    }
                )
            elif isinstance(entity, User):
                # Private chat
                users.append(
                    {
                        "name": f"{entity.first_name or ''} {entity.last_name or ''}".strip(),
                        "username": entity.username,
                        "id": entity.id,
                    }
                )

        # Print channels
        print(f"\n📢 CHANNELS ({len(channels)})")
        print("=" * 80)
        if channels:
            for ch in channels:
                username_str = (
                    f"@{ch['username']}" if ch["username"] else "(no username)"
                )
                print(f"  • {ch['title']}")
                print(f"    Username: {username_str}")
                print(f"    ID: {ch['id']}")
                print(f"    Config: {ch['id']}")
                print()
        else:
            print("  (No channels found)\n")

        # Print groups
        print(f"👥 GROUPS & SUPERGROUPS ({len(groups)})")
        print("=" * 80)
        if groups:
            for grp in groups:
                username_str = (
                    f"@{grp['username']}" if grp["username"] else "(no username)"
                )
                print(f"  • {grp['title']}")
                print(f"    Username: {username_str}")
                print(f"    ID: {grp['id']}")
                print(f"    Config: {grp['id']}")
                print()
        else:
            print("  (No groups found)\n")

        # Print summary
        print("=" * 80)
        print("📊 SUMMARY")
        print("=" * 80)
        print(f"  Channels: {len(channels)}")
        print(f"  Groups: {len(groups)}")
        print(f"  Private chats: {len(users)}")
        print(f"  Total: {len(dialogs)}")
        print()

        # Print config help
        print("=" * 80)
        print("💡 HOW TO USE IN config.toml")
        print("=" * 80)
        print("Add to your config.toml under [channels] include:")
        print()
        print("[channels]")
        print("include = [")

        # Show first 5 channels as examples
        count = 0
        for ch in channels[:5]:
            print(f"    {ch['id']},  # {ch['title']}")
            count += 1

        if len(channels) > 5:
            print(f"    # ... and {len(channels) - 5} more channels")

        print("]")
        print()

        # Save channels and groups to raw/channels.json (only those in config.toml)
        raw_dir = Path("raw")
        raw_dir.mkdir(exist_ok=True)

        channels_data = []
        for ch in channels:
            if ch["id"] in config_channel_ids:
                channels_data.append(
                    {
                        "channel_id": ch["id"],
                        "name": ch["title"],
                        "username": ch["username"],
                        "is_group": False,
                    }
                )
        for grp in groups:
            if grp["id"] in config_group_ids:
                channels_data.append(
                    {
                        "channel_id": grp["id"],
                        "name": grp["title"],
                        "username": grp["username"],
                        "is_group": True,
                    }
                )

        channels_file = raw_dir / "channels.json"
        with open(channels_file, "w", encoding="utf-8") as f:
            json.dump(channels_data, f, indent=2, ensure_ascii=False)

        print(
            f"💾 Saved {len(channels_data)} channels/groups from config.toml to {channels_file}"
        )
        print()

        print("✅ Done!\n")

    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)

    finally:
        if client.is_connected():
            await client.disconnect()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⚠️  Interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)
