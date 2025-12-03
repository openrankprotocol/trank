#!/usr/bin/env python3
"""
Import Seeds and Scores to PostgreSQL

Imports seed values from seed/[channel_id].csv and computed scores from scores/[channel_id].csv
into PostgreSQL database.

Usage:
    python3 import_scores_to_db.py                    # Import all channels from config
    python3 import_scores_to_db.py --channel 123456   # Import specific channel
    python3 import_scores_to_db.py --dry-run          # Show what would be imported without inserting

Requirements:
    - psycopg2 (install with: pip install psycopg2-binary)
    - Environment variable: DATABASE_URL (e.g., postgresql://user:pass@localhost:5432/dbname)

Database schema:
    See schemas/runs.sql, schemas/seeds.sql, and schemas/scores.sql
"""

import argparse
import csv
import os
import sys
from pathlib import Path

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values

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


def get_db_connection():
    """Get database connection from DATABASE_URL environment variable."""
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL environment variable is required")

    return psycopg2.connect(database_url)


def load_csv_values(file_path):
    """
    Load values from a CSV file with columns 'i' (user_id) and 'v' (value).

    Args:
        file_path: Path to the CSV file

    Returns:
        list of tuples: [(user_id, value), ...]
    """
    values = []

    if not file_path.exists():
        return values

    with open(file_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            user_id = row.get("i")
            value = row.get("v")

            if user_id is None or value is None:
                continue

            try:
                values.append((int(user_id), float(value)))
            except (ValueError, TypeError):
                continue

    return values


def import_channel_scores(conn, channel_id, dry_run=False):
    """
    Import seeds and scores for a single channel.

    Args:
        conn: Database connection
        channel_id: Channel ID to import
        dry_run: If True, don't actually insert data

    Returns:
        tuple: (seeds_count, scores_count)
    """
    seed_file = Path("seed") / f"{channel_id}.csv"
    output_file = Path("scores") / f"{channel_id}.csv"

    seeds = load_csv_values(seed_file)
    scores = load_csv_values(output_file)

    if seed_file.exists():
        print(f"  📂 Loading seeds from: {seed_file}")
    else:
        print(f"  ⚠️  Seeds file not found: {seed_file}")

    if output_file.exists():
        print(f"  📂 Loading scores from: {output_file}")
    else:
        print(f"  ⚠️  Scores file not found: {output_file}")

    print(f"  📊 Found {len(seeds)} seeds and {len(scores)} scores")

    if not seeds and not scores:
        return 0, 0

    if dry_run:
        print(f"  🔍 Dry run - no data inserted")
        return len(seeds), len(scores)

    cursor = conn.cursor()

    try:
        # Get next run_id for this channel
        cursor.execute(
            """
            SELECT COALESCE(MAX(run_id), 0) + 1
            FROM trank.runs
            WHERE channel_id = %s
            """,
            (int(channel_id),),
        )
        run_id = cursor.fetchone()[0]

        # Create a new run
        cursor.execute(
            """
            INSERT INTO trank.runs (channel_id, run_id)
            VALUES (%s, %s)
            """,
            (int(channel_id), run_id),
        )
        print(f"  🆕 Created run ID: {run_id}")

        # Insert seeds
        if seeds:
            print(f"  💾 Inserting seeds...")
            seed_tuples = [
                (int(channel_id), run_id, user_id, value) for user_id, value in seeds
            ]
            execute_values(
                cursor,
                """
                INSERT INTO trank.seeds (channel_id, run_id, user_id, value)
                VALUES %s
                ON CONFLICT (channel_id, run_id, user_id) DO UPDATE SET
                    value = EXCLUDED.value
                """,
                seed_tuples,
                page_size=1000,
            )

        # Insert scores
        if scores:
            print(f"  💾 Inserting scores...")
            score_tuples = [
                (int(channel_id), run_id, user_id, value) for user_id, value in scores
            ]
            execute_values(
                cursor,
                """
                INSERT INTO trank.scores (channel_id, run_id, user_id, value)
                VALUES %s
                ON CONFLICT (channel_id, run_id, user_id) DO UPDATE SET
                    value = EXCLUDED.value
                """,
                score_tuples,
                page_size=1000,
            )

        conn.commit()
        print(f"  ✅ Successfully imported {len(seeds)} seeds and {len(scores)} scores")

    except Exception as e:
        conn.rollback()
        print(f"  ❌ Error importing data: {e}")
        raise

    finally:
        cursor.close()

    return len(seeds), len(scores)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Import seeds and scores to PostgreSQL database"
    )
    parser.add_argument(
        "--channel",
        type=str,
        help="Specific channel ID to import (otherwise imports all from config)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be imported without actually inserting",
    )

    args = parser.parse_args()

    print("📥 Import Seeds and Scores to PostgreSQL\n")

    # Determine which channels to process
    if args.channel:
        channels = [args.channel]
        print(f"Processing specific channel: {args.channel}")
    else:
        # Load from config
        try:
            config = load_config()
        except FileNotFoundError as e:
            print(f"❌ Error: {e}")
            sys.exit(1)

        group_chats = config.get("group_chats", {}).get("include", [])
        channels_list = config.get("channels", {}).get("include", [])
        channels = [str(ch) for ch in group_chats + channels_list]

        if not channels:
            print("❌ Error: No channels configured in config.toml")
            sys.exit(1)

        print(f"Found {len(channels)} channel(s) in config.toml")

    if args.dry_run:
        print("Mode: Dry run (no data will be inserted)\n")
    else:
        print()

    # Connect to database
    try:
        conn = get_db_connection()
        print("✅ Connected to database\n")
    except Exception as e:
        print(f"❌ Error connecting to database: {e}")
        sys.exit(1)

    total_seeds = 0
    total_scores = 0

    try:
        for channel_id in channels:
            print(f"{'=' * 60}")
            print(f"Channel: {channel_id}")
            print(f"{'=' * 60}")

            seeds_count, scores_count = import_channel_scores(
                conn, channel_id, dry_run=args.dry_run
            )

            total_seeds += seeds_count
            total_scores += scores_count
            print()

    finally:
        conn.close()

    print(f"{'=' * 60}")
    print("📊 Summary")
    print(f"{'=' * 60}")
    print(f"   Total seeds: {total_seeds}")
    print(f"   Total scores: {total_scores}")
    if args.dry_run:
        print("   (Dry run - no data was inserted)")
    print()


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
