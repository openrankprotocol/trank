#!/usr/bin/env python3
"""
List Channel/Group Admins and Roles
Shows all owners, admins, and moderators for channels/groups in config.toml
Saves admin lists to raw/[channel_id]_admins.csv
"""

import asyncio
import csv
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.tl.functions.channels import GetParticipantsRequest
from telethon.tl.functions.messages import GetFullChatRequest
from telethon.tl.types import (
    Channel,
    ChannelParticipantAdmin,
    ChannelParticipantCreator,
    ChannelParticipantsAdmins,
    Chat,
    ChatParticipantAdmin,
    ChatParticipantCreator,
)

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


def load_config():
    """Load configuration from config.toml"""
    config_path = Path(__file__).parent / "config.toml"

    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    # Load TOML file (always use binary mode for tomllib/tomli)
    with open(config_path, "rb") as f:
        return tomllib.load(f)


def get_role_name(participant):
    """Get human-readable role name for a participant"""
    if isinstance(participant, ChannelParticipantCreator):
        return "OWNER/CREATOR"
    elif isinstance(participant, ChannelParticipantAdmin):
        # Get custom title if available
        custom_title = getattr(participant, "rank", None)
        if custom_title:
            return f"ADMIN ({custom_title})"
        return "ADMIN"
    else:
        return "MEMBER"


def get_admin_rights(participant):
    """Extract admin rights/permissions"""
    if not hasattr(participant, "admin_rights"):
        return []

    rights = participant.admin_rights
    permissions = []

    if getattr(rights, "change_info", False):
        permissions.append("Change Info")
    if getattr(rights, "post_messages", False):
        permissions.append("Post Messages")
    if getattr(rights, "edit_messages", False):
        permissions.append("Edit Messages")
    if getattr(rights, "delete_messages", False):
        permissions.append("Delete Messages")
    if getattr(rights, "ban_users", False):
        permissions.append("Ban Users")
    if getattr(rights, "invite_users", False):
        permissions.append("Invite Users")
    if getattr(rights, "pin_messages", False):
        permissions.append("Pin Messages")
    if getattr(rights, "add_admins", False):
        permissions.append("Add Admins")
    if getattr(rights, "manage_call", False):
        permissions.append("Manage Calls")
    if getattr(rights, "manage_topics", False):
        permissions.append("Manage Topics")

    return permissions


async def get_channel_admins(client: TelegramClient, channel_id: int):
    """Get all admins for a channel/group"""
    try:
        # Get channel entity
        entity = await client.get_entity(channel_id)

        print(f"\n{'=' * 80}")
        print(f"Channel/Group: {entity.title}")
        print(f"ID: {channel_id}")

        # Handle username attribute safely
        username = getattr(entity, "username", None)
        if username:
            print(f"Username: @{username}")
        else:
            print(f"Username: (none)")

        # Determine entity type
        is_basic_chat = isinstance(entity, Chat)
        is_channel = isinstance(entity, Channel) and getattr(entity, "broadcast", False)

        if is_basic_chat:
            entity_type = "Basic Group"
        elif is_channel:
            entity_type = "Channel"
        else:
            entity_type = "Supergroup"

        print(f"Type: {entity_type}")
        print(f"{'=' * 80}\n")

        admin_list = []

        # Handle basic groups differently (they use Chat, not Channel API)
        if is_basic_chat:
            # For basic groups, use GetFullChatRequest
            full_chat = await client(GetFullChatRequest(chat_id=entity.id))
            participants = full_chat.full_chat.participants.participants
            users = full_chat.users

            for participant in participants:
                # Check if admin or creator
                if not isinstance(
                    participant, (ChatParticipantCreator, ChatParticipantAdmin)
                ):
                    continue

                user = next((u for u in users if u.id == participant.user_id), None)
                if not user:
                    continue

                # Display user info
                full_name = f"{user.first_name or ''} {user.last_name or ''}".strip()
                username_str = f"@{user.username}" if user.username else "(no username)"

                if isinstance(participant, ChatParticipantCreator):
                    role = "OWNER/CREATOR"
                    print(f"👑 {role}")
                    is_owner = True
                else:
                    role = "ADMIN"
                    print(f"🛡️  {role}")
                    is_owner = False

                print(f"   Name: {full_name}")
                print(f"   Username: {username_str}")
                print(f"   User ID: {user.id}")
                print()

                admin_list.append(
                    {
                        "user_id": user.id,
                        "name": full_name,
                        "username": user.username,
                        "role": role,
                        "permissions": [],
                        "is_owner": is_owner,
                    }
                )
        else:
            # For channels/supergroups, use GetParticipantsRequest
            admins = await client(
                GetParticipantsRequest(
                    channel=entity,
                    filter=ChannelParticipantsAdmins(),
                    offset=0,
                    limit=200,
                    hash=0,
                )
            )

            if not admins.participants:
                print("  ℹ️  No admins found (or insufficient permissions)\n")
                return {"channel_id": channel_id, "admins": []}

            for participant in admins.participants:
                user = next(
                    (u for u in admins.users if u.id == participant.user_id), None
                )

                if not user:
                    continue

                role = get_role_name(participant)
                permissions = get_admin_rights(participant)

                # Display user info
                full_name = f"{user.first_name or ''} {user.last_name or ''}".strip()
                username_str = f"@{user.username}" if user.username else "(no username)"

                if isinstance(participant, ChannelParticipantCreator):
                    print(f"👑 {role}")
                else:
                    print(f"🛡️  {role}")

                print(f"   Name: {full_name}")
                print(f"   Username: {username_str}")
                print(f"   User ID: {user.id}")

                if permissions:
                    print(f"   Permissions: {', '.join(permissions)}")

                print()

                admin_list.append(
                    {
                        "user_id": user.id,
                        "name": full_name,
                        "username": user.username,
                        "role": role,
                        "permissions": permissions,
                        "is_owner": isinstance(participant, ChannelParticipantCreator),
                    }
                )

        # Summary
        owner_count = sum(1 for a in admin_list if a["is_owner"])
        admin_count = len(admin_list) - owner_count

        print(f"📊 Summary: {owner_count} owner(s), {admin_count} admin(s)")
        print(f"Total: {len(admin_list)} staff member(s)\n")

        return {
            "channel_id": channel_id,
            "title": entity.title,
            "admins": admin_list,
            "username": username if username else "",
        }

    except Exception as e:
        print(f"❌ Error getting admins for {channel_id}: {e}\n")
        return {"channel_id": channel_id, "error": str(e)}


def save_admins_to_csv(results):
    """Save admin lists to CSV files in raw/ directory"""
    # Create raw directory if it doesn't exist
    output_dir = Path("raw")
    output_dir.mkdir(parents=True, exist_ok=True)

    for result in results:
        # Skip if there was an error
        if "error" in result or not result.get("admins"):
            continue

        channel_id = result["channel_id"]
        admins = result["admins"]

        # Save to CSV
        csv_filename = output_dir / f"{channel_id}_admins.csv"
        with open(csv_filename, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "user_id",
                    "username",
                    "first_name",
                    "last_name",
                ]
            )

            for admin in admins:
                # Split full name into first and last name
                full_name = admin["name"]
                name_parts = full_name.split(None, 1) if full_name else []
                first_name = name_parts[0] if len(name_parts) > 0 else ""
                last_name = name_parts[1] if len(name_parts) > 1 else ""

                username = admin["username"] if admin["username"] else ""

                writer.writerow(
                    [
                        admin["user_id"],
                        username,
                        first_name,
                        last_name,
                    ]
                )

        print(f"💾 Saved {len(admins)} admin(s) to {csv_filename}")

    print()


async def main():
    """Main entry point"""
    print("🔍 Listing Channel/Group Admins and Roles\n")

    # Load configuration
    config = load_config()
    channels_config = config.get("channels", {})

    # Get environment variables
    api_id = int(os.getenv("TELEGRAM_APP_ID", "0"))
    api_hash = os.getenv("TELEGRAM_APP_HASH", "")
    phone = os.getenv("TELEGRAM_PHONE", "")

    if not api_id or not api_hash:
        print("❌ Error: Missing Telegram credentials in .env file")
        print("Please set TELEGRAM_APP_ID and TELEGRAM_APP_HASH")
        sys.exit(1)

    # Get channels to check (must be integers)
    channels = channels_config.get("include", [])
    if not channels:
        print("❌ Error: No channels configured in config.toml")
        print("Add channel IDs to [channels] include = [...]")
        sys.exit(1)

    # Validate that all channels are integers
    for ch in channels:
        if not isinstance(ch, int):
            print(f"❌ Error: Channel '{ch}' is not a valid ID (must be an integer)")
            print("Use 'python list_channels.py' to see your channel IDs")
            sys.exit(1)

    # Initialize Telegram client
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

        # Process each channel
        all_results = []
        for channel_id in channels:
            result = await get_channel_admins(client, channel_id)
            all_results.append(result)

        # Final summary
        print("\n" + "=" * 80)
        print("📊 OVERALL SUMMARY")
        print("=" * 80)

        total_channels = len(all_results)
        successful = sum(1 for r in all_results if "error" not in r)
        failed = total_channels - successful

        print(f"Channels processed: {total_channels}")
        print(f"Successful: {successful}")
        print(f"Failed: {failed}")

        total_admins = sum(len(r.get("admins", [])) for r in all_results)
        print(f"\nTotal admins across all channels: {total_admins}")

        # Save all admin lists to CSV files
        print("\n" + "=" * 80)
        print("💾 SAVING TO FILES")
        print("=" * 80 + "\n")
        save_admins_to_csv(all_results)

        print("✅ Done!\n")

    except Exception as e:
        print(f"❌ Fatal error: {e}")
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
