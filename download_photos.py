#!/usr/bin/env python3
"""
Download Profile Photos

Downloads profile photos for users listed in raw/[channel_id]_user_ids.csv files.
Photos are saved to raw/photos/[user_id].jpg

Channels are loaded from config.toml.

Usage:
    python3 download_photos.py                    # Download photos for channels in config.toml
    python3 download_photos.py --skip-existing    # Skip users who already have photos
    python3 download_photos.py --verbose          # Show reasons for skipped photos

Requirements:
    - telethon (install with: pip install telethon)
    - Valid Telegram session (run read_messages.py first to authenticate)
    - Environment variables: TELEGRAM_APP_ID, TELEGRAM_APP_HASH
"""

import asyncio
import csv
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from telethon import TelegramClient

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


async def download_photos_for_channel(
    client: TelegramClient,
    channel_id: str,
    skip_existing: bool = False,
    verbose: bool = False,
):
    """
    Download profile photos for all users in a channel's user_ids.csv file.

    Args:
        client: Authenticated Telegram client
        channel_id: Channel ID to process
        skip_existing: If True, skip users who already have a photo downloaded

    Returns:
        tuple: (downloaded_count, skipped_count, failed_count, skip_reasons)
    """
    csv_file = Path("raw") / f"{channel_id}_user_ids.csv"
    photos_dir = Path("raw") / "photos"

    if not csv_file.exists():
        print(f"❌ User IDs file not found: {csv_file}")
        return 0, 0, 0, {}

    # Create photos directory if it doesn't exist
    photos_dir.mkdir(parents=True, exist_ok=True)

    # Read user IDs from CSV
    user_ids = []
    with open(csv_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            user_id = row.get("user_id")
            if user_id:
                user_ids.append(int(user_id))

    print(f"📋 Found {len(user_ids)} users in {csv_file}")

    downloaded = 0
    skipped = 0
    failed = 0
    skip_reasons = {
        "already_exists": [],
        "no_photo": [],
        "user_invalid": [],
        "other": [],
    }

    for i, user_id in enumerate(user_ids, 1):
        photo_path = photos_dir / f"{user_id}.jpg"

        # Skip if photo already exists and skip_existing is True
        if skip_existing and photo_path.exists():
            skipped += 1
            skip_reasons["already_exists"].append(user_id)
            continue

        try:
            # Download profile photo (small size) with timeout
            result = await asyncio.wait_for(
                client.download_profile_photo(
                    user_id,
                    file=str(photo_path),
                    download_big=False,  # Download the small-size photo
                ),
                timeout=10.0,  # 10 second timeout per photo
            )

            if result:
                downloaded += 1
            else:
                # User has no profile photo
                skipped += 1
                skip_reasons["no_photo"].append(user_id)
                print(f"   ⏭️  {user_id}: No profile photo (download returned None)")

        except asyncio.TimeoutError as e:
            failed += 1
            print(f"   ⚠️  {user_id}: {e}")

        except Exception as e:
            error_str = str(e).lower()
            if "flood" in error_str or "wait" in error_str:
                # Rate limited - wait longer
                print(f"   ⚠️  {user_id}: {e} - waiting 30 seconds...")
                await asyncio.sleep(30)
                failed += 1
            elif "user" in error_str and "invalid" in error_str:
                skipped += 1
                skip_reasons["user_invalid"].append(user_id)
                print(f"   ⏭️  {user_id}: {e}")
            elif "no user" in error_str:
                skipped += 1
                skip_reasons["user_invalid"].append(user_id)
                print(f"   ⏭️  {user_id}: {e}")
            else:
                failed += 1
                skip_reasons["other"].append((user_id, str(e)))
                print(f"   ⚠️  {user_id}: {e}")

        # Print progress every 10 users
        if i % 10 == 0 or i == len(user_ids):
            print(
                f"   📸 Progress: {i}/{len(user_ids)} - Downloaded {downloaded}, Skipped {skipped}, Failed {failed}"
            )

        # Delay to avoid rate limiting (increased from 0.1 to 0.5)
        await asyncio.sleep(0.5)

    return downloaded, skipped, failed, skip_reasons


async def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="Download profile photos for users")
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip users who already have photos downloaded",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Show detailed reasons for skipped photos",
    )

    args = parser.parse_args()

    print("📸 Profile Photo Downloader\n")

    # Load config
    try:
        config = load_config()
    except FileNotFoundError as e:
        print(f"❌ Error: {e}")
        sys.exit(1)

    group_chats = config.get("group_chats", {}).get("include", [])
    channels = config.get("channels", {}).get("include", [])
    all_channels = group_chats + channels

    if not all_channels:
        print("❌ Error: No group_chats or channels configured in config.toml")
        sys.exit(1)

    # Get environment variables
    api_id = int(os.getenv("TELEGRAM_APP_ID", "0"))
    api_hash = os.getenv("TELEGRAM_APP_HASH", "")

    if not api_id or not api_hash:
        print(
            "❌ Error: TELEGRAM_APP_ID and TELEGRAM_APP_HASH environment variables required"
        )
        print("   Set them in your environment or .env file")
        sys.exit(1)

    # Find user_ids.csv files for configured channels
    raw_dir = Path("raw")
    if not raw_dir.exists():
        print("❌ Error: raw/ directory not found")
        print("   Run read_messages.py or read_channel_messages.py first")
        sys.exit(1)

    print(f"Found {len(channels)} channel(s) in config.toml")
    if args.skip_existing:
        print("Mode: Skip existing photos")
    if args.verbose:
        print("Mode: Verbose logging enabled")
    print()

    # Connect to Telegram
    session_file = Path(__file__).parent / "telegram_session"
    client = TelegramClient(str(session_file), api_id, api_hash)

    await client.start()
    print("✅ Connected to Telegram\n")

    total_downloaded = 0
    total_skipped = 0
    total_failed = 0
    total_skip_reasons = {
        "already_exists": [],
        "no_photo": [],
        "user_invalid": [],
        "other": [],
    }

    for channel_id in all_channels:
        print(f"{'=' * 60}")
        print(f"Channel: {channel_id}")
        print(f"{'=' * 60}")

        downloaded, skipped, failed, skip_reasons = await download_photos_for_channel(
            client, str(channel_id), args.skip_existing, args.verbose
        )

        total_downloaded += downloaded
        total_skipped += skipped
        total_failed += failed
        for key in total_skip_reasons:
            total_skip_reasons[key].extend(skip_reasons.get(key, []))

        print(
            f"✅ Channel {channel_id}: Downloaded {downloaded}, Skipped {skipped}, Failed {failed}"
        )
        print()

    await client.disconnect()

    print(f"{'=' * 60}")
    print("📊 Summary")
    print(f"{'=' * 60}")
    print(f"   Total downloaded: {total_downloaded}")
    print(f"   Total skipped: {total_skipped}")
    print(f"   Total failed: {total_failed}")
    print(f"   Photos saved to: raw/photos/")
    print()
    print("📋 Skip reasons breakdown:")
    print(f"   Already exists locally: {len(total_skip_reasons['already_exists'])}")
    print(f"   No profile photo set:   {len(total_skip_reasons['no_photo'])}")
    print(f"   Invalid/deleted user:   {len(total_skip_reasons['user_invalid'])}")
    print(f"   Other errors:           {len(total_skip_reasons['other'])}")
    print()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⚠️  Interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
