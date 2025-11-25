#!/usr/bin/env python3
"""
Generate JSON files for UI from seed and output data.

This script:
1. Loads all entries from seed/ directory
2. Loads corresponding score files from output/ directory
3. Loads user mappings from raw/[channel_id]_user_ids.csv and raw/[channel_id]_admins.csv
4. Optionally converts user IDs to display names (username > "first last" > user_id)
5. Creates JSON files for each channel with format:
   {
     "category": "socialrank",
     "channel": "<channel_id>",
     "seed": [{"i": "peer_id", "v": value}, ...],
     "scores": [{"i": "peer_id", "v": value}, ...]
   }
6. Saves JSON files to ui/ directory

Usage:
    python3 generate_json.py                 # Convert to display names (default)
    python3 generate_json.py --with-user-ids # Keep user IDs

Requirements:
    - pandas (install with: pip install pandas)
    - CSV files in seed/ directory
    - CSV files in output/ directory (matching seed filenames)
    - CSV files in raw/ directory:
      - [channel_id]_user_ids.csv (regular users)
      - [channel_id]_admins.csv (admins - optional)

Output:
    - Creates ui/ directory if it doesn't exist
    - For each seed file (e.g., 1533865579.csv), creates:
      - ui/1533865579.json with seed and score data
"""

import argparse
import json
from pathlib import Path

import pandas as pd


def get_channel_id(filename):
    """
    Get channel ID from filename.

    Args:
        filename (str): Base filename without extension

    Returns:
        str: Channel ID
    """
    # Return the filename as-is (it's already the channel ID)
    return filename


def load_user_ids_mapping(channel_id):
    """
    Load user ID to display name mapping from CSV files.

    Loads both user_ids.csv and admins.csv (if available) to create complete mapping.
    Admin mappings will override regular user mappings if both exist.
    Display name fallback priority:
      1. username (e.g., "john_doe") - if user has set a Telegram username
      2. "first_name last_name" (e.g., "John Doe") - constructed from profile names
      3. user_id as string - fallback if no other info available

    Args:
        channel_id: Channel ID to load user mapping for

    Returns:
        dict: Mapping of user_id (str) -> display_name (str)
              Empty dict if mapping file not found
    """
    user_id_to_display = {}

    # Load regular users from user_ids.csv
    try:
        mapping_file = Path(f"raw/{channel_id}_user_ids.csv")

        if mapping_file.exists():
            print(f"  📋 Loading user mapping from: {mapping_file}")

            # Load as dataframe
            mapping_df = pd.read_csv(mapping_file)

            # Create dictionary mapping user_id -> display_name
            # Display name priority: username > full name > user_id
            # Note: Not all Telegram users have usernames (it's optional)
            for _, row in mapping_df.iterrows():
                user_id = str(row["user_id"])

                # Extract username (may be empty/NaN)
                username = ""
                if pd.notna(row["username"]):
                    username_val = row["username"]
                    if username_val:
                        username = str(username_val).strip()

                # Extract first name (may be empty/NaN)
                first_name = ""
                if pd.notna(row["first_name"]):
                    first_name_val = row["first_name"]
                    if first_name_val:
                        first_name = str(first_name_val).strip()

                # Extract last name (may be empty/NaN)
                last_name = ""
                if pd.notna(row["last_name"]):
                    last_name_val = row["last_name"]
                    if last_name_val:
                        last_name = str(last_name_val).strip()

                # Determine best display name using priority fallback
                if username:
                    # Best case: user has a username like @john_doe
                    display_name = username
                elif first_name or last_name:
                    # Fallback: construct name from first/last name
                    display_name = f"{first_name} {last_name}".strip()
                else:
                    # Last resort: use user_id as identifier
                    display_name = user_id

                user_id_to_display[user_id] = display_name

            print(f"  ✅ Loaded {len(user_id_to_display)} user ID mappings")
        else:
            print(f"  ⚠️  Warning: {mapping_file} not found")

    except Exception as e:
        print(f"  ⚠️  Warning: Could not load user mapping: {str(e)}")

    # Load admins from admins.csv and add/override mappings
    try:
        admins_file = Path(f"raw/{channel_id}_admins.csv")

        if admins_file.exists():
            print(f"  📋 Loading admin mapping from: {admins_file}")

            # Load as dataframe
            admins_df = pd.read_csv(admins_file)

            admin_count = 0
            for _, row in admins_df.iterrows():
                user_id = str(row["user_id"])

                # Extract username (may be empty/NaN)
                username = ""
                if pd.notna(row["username"]):
                    username_val = row["username"]
                    if username_val:
                        username = str(username_val).strip()

                # Extract first name (may be empty/NaN)
                first_name = ""
                if pd.notna(row["first_name"]):
                    first_name_val = row["first_name"]
                    if first_name_val:
                        first_name = str(first_name_val).strip()

                # Extract last name (may be empty/NaN)
                last_name = ""
                if pd.notna(row["last_name"]):
                    last_name_val = row["last_name"]
                    if last_name_val:
                        last_name = str(last_name_val).strip()

                # Determine best display name using priority fallback
                if username:
                    display_name = username
                elif first_name or last_name:
                    display_name = f"{first_name} {last_name}".strip()
                else:
                    display_name = user_id

                user_id_to_display[user_id] = display_name
                admin_count += 1

            print(f"  ✅ Loaded {admin_count} admin mappings")
        else:
            print(f"  ℹ️  No admin file found: {admins_file}")

    except Exception as e:
        print(f"  ⚠️  Warning: Could not load admin mapping: {str(e)}")

    if not user_id_to_display:
        print(f"  ⚠️  No user mappings found, keeping user IDs")

    return user_id_to_display


def convert_ids_to_display_names(data, user_id_mapping):
    """
    Convert user IDs to display names in data.

    Args:
        data (list): List of dictionaries with 'i' (user_id) and 'v' (value) keys
        user_id_mapping (dict): Mapping of user_id -> display_name

    Returns:
        list: Data with user IDs converted to display names
    """
    if not user_id_mapping:
        return data

    converted_data = []
    for entry in data:
        user_id = str(entry["i"])
        display_name = user_id_mapping.get(user_id, user_id)
        converted_data.append({"i": display_name, "v": entry["v"]})

    return converted_data


def load_seed_data(seed_file):
    """
    Load all entries from seed file.

    Args:
        seed_file (Path): Path to seed CSV file

    Returns:
        list: List of dictionaries with 'i' and 'v' keys
    """
    df = pd.read_csv(seed_file)

    # Convert to list of dictionaries
    seed_data = df.to_dict("records")

    return seed_data


def load_scores(scores_file):
    """
    Load scores from output file.

    Args:
        scores_file (Path): Path to scores CSV file

    Returns:
        list: List of dictionaries with 'i' and 'v' keys
    """
    if not scores_file.exists():
        print(f"Warning: {scores_file} not found, using empty scores")
        return []

    df = pd.read_csv(scores_file)

    # Convert to list of dictionaries
    scores_data = df.to_dict("records")

    return scores_data


def generate_json_file(ecosystem_name, seed_data, scores_data, output_file):
    """
    Generate JSON file with seed and scores data.

    Args:
        ecosystem_name (str): Name of the ecosystem
        seed_data (list): List of seed entries
        scores_data (list): List of score entries
        output_file (Path): Path to output JSON file
    """
    json_data = {
        "category": "socialrank",
        "channel": ecosystem_name,
        "seed": seed_data,
        "scores": scores_data,
    }

    # Write JSON file with pretty formatting
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(json_data, f, indent=2, ensure_ascii=False)

    print(f"✓ Created {output_file}")
    print(f"  Seed entries: {len(seed_data)}")
    print(f"  Score entries: {len(scores_data)}")


def main():
    """
    Main execution function.
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Generate JSON files for UI from seed and output data"
    )
    parser.add_argument(
        "--with-user-ids",
        action="store_true",
        help="Keep user IDs instead of converting to display names",
    )
    args = parser.parse_args()

    print("=" * 60)
    print("Generating JSON files for UI")
    print("=" * 60)
    if args.with_user_ids:
        print("Mode: Keep user IDs (no display name conversion)")
    else:
        print('Mode: Convert to display names (username > "first last" > user_id)')
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
        channel_id = get_channel_id(base_name)

        print(f"Processing: {base_name}")

        # Load seed data (all entries)
        seed_data = load_seed_data(seed_file)
        print(f"  ✅ Loaded {len(seed_data)} seed entries")

        # Find corresponding scores file (same name, in output/ directory)
        scores_file = output_dir / f"{base_name}.csv"
        scores_data = load_scores(scores_file)

        # Convert user IDs to display names unless --with-user-ids flag is set
        if not args.with_user_ids:
            # Load user ID to display name mapping
            user_id_mapping = load_user_ids_mapping(channel_id)

            # Convert seed user IDs to display names
            seed_data = convert_ids_to_display_names(seed_data, user_id_mapping)
            print(f"  ✅ Converted seed IDs to display names")

            # Convert scores user IDs to display names
            scores_data = convert_ids_to_display_names(scores_data, user_id_mapping)
            print(f"  ✅ Converted scores IDs to display names")
        else:
            print(f"  📋 Keeping user IDs (no display name conversion)")

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
