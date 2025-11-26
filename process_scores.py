#!/usr/bin/env python3
"""
Score Processing Script

This script processes pre-aggregated score files from the scores/ directory by:
1. Loading all CSV score files (i,v format - username/user_id, score)
2. Optionally excluding admins (with --members-only flag)
3. Normalizing scores to 0-1000 range while preserving relative differences
4. Sorting by score (descending)
5. Saving results to output/ directory

Usage:
    python3 process_scores.py
    python3 process_scores.py --members-only  # Exclude admins from scores

Note: The --log, --sqrt, and --quantile flags are kept for backwards compatibility
but all now apply the same linear normalization to preserve relative score differences.

Requirements:
    - pandas (install with: pip install pandas)
    - CSV files in scores/ directory with columns 'i', 'v'

Output:
    - Creates output/ directory if it doesn't exist
    - For each input file (e.g., 4886853134.csv), creates:
      - output/4886853134.csv with normalized scores
    - Scores are normalized to 0-1000 range, preserving relative differences
    - Sorted by score (descending)
"""

import argparse
import sys
from pathlib import Path

import pandas as pd


def apply_log_transformation(df):
    """Apply simple normalization to 0-1000 range, preserving relative differences"""
    if len(df) == 0:
        return df

    df_transformed = df.copy()

    # Simple linear normalization to 0-1000 range
    min_val = df["v"].min()
    max_val = df["v"].max()
    if max_val != min_val:
        df_transformed["v"] = (df["v"] - min_val) / (max_val - min_val) * 1000
    else:
        df_transformed["v"] = 1000.0

    # Round to 2 decimal places
    df_transformed["v"] = df_transformed["v"].round(2)

    return df_transformed


def apply_sqrt_transformation(df):
    """Apply simple normalization to 0-1000 range, preserving relative differences"""
    if len(df) == 0:
        return df

    df_transformed = df.copy()

    # Simple linear normalization to 0-1000 range
    min_val = df["v"].min()
    max_val = df["v"].max()
    if max_val != min_val:
        df_transformed["v"] = (df["v"] - min_val) / (max_val - min_val) * 1000
    else:
        df_transformed["v"] = 1000.0

    # Round to 2 decimal places
    df_transformed["v"] = df_transformed["v"].round(2)

    return df_transformed


def apply_quantile_transformation(df):
    """Apply simple normalization to 0-1000 range, preserving relative differences"""
    if len(df) == 0:
        return df

    df_transformed = df.copy()

    # Simple linear normalization to 0-1000 range
    min_val = df["v"].min()
    max_val = df["v"].max()
    if max_val != min_val:
        df_transformed["v"] = (df["v"] - min_val) / (max_val - min_val) * 1000
    else:
        df_transformed["v"] = 1000.0

    # Round to 2 decimal places
    df_transformed["v"] = df_transformed["v"].round(2)

    return df_transformed


def load_admin_ids(channel_id):
    """
    Load admin user IDs from raw/[channel_id]_admins.csv

    Args:
        channel_id: Channel ID to load admins for

    Returns:
        set: Set of admin user IDs (as strings for comparison)
    """
    admins_file = Path("raw") / f"{channel_id}_admins.csv"

    if not admins_file.exists():
        print(f"⚠️  Warning: Admins file not found: {admins_file}")
        return set()

    admin_ids = set()
    with open(admins_file, "r", encoding="utf-8") as f:
        # Skip header: user_id,username,first_name,last_name
        next(f)
        for line in f:
            parts = line.strip().split(",")
            if len(parts) >= 1:
                user_id = parts[0].strip()
                if user_id:
                    admin_ids.add(user_id)

    return admin_ids


def process_score_file(
    input_file, output_dir, transform_func, transform_name, members_only=False
):
    """
    Process a single score file by normalizing and saving

    Args:
        input_file: Path to input CSV file
        output_dir: Directory to save processed files
        transform_func: Transformation function to apply
        transform_name: Name of the transformation
        members_only: If True, exclude admins from scores
    """
    # Extract channel ID from filename
    channel_id = Path(input_file).stem

    print(f"\n{'=' * 80}")
    print(f"Processing: {input_file}")
    print(f"Channel ID: {channel_id}")
    print(f"Transformation: {transform_name}")
    if members_only:
        print(f"Mode: Members only (excluding admins)")
    print(f"{'=' * 80}")

    # Load the score CSV file (i,v format - pre-aggregated scores)
    df = pd.read_csv(input_file)
    print(f"✅ Loaded {len(df)} user scores")

    # Exclude admins if --members-only flag is set
    if members_only:
        admin_ids = load_admin_ids(channel_id)
        if admin_ids:
            # Convert 'i' column to string for comparison
            original_count = len(df)
            df = df[~df["i"].astype(str).isin(admin_ids)]
            excluded_count = original_count - len(df)
            print(f"✅ Excluded {excluded_count} admins, {len(df)} members remaining")
        else:
            print(f"⚠️  No admins to exclude")

    # Apply normalization
    transformed = transform_func(df.copy())
    print(f"✅ Applied linear normalization (preserving relative differences)")

    # Sort by score (descending) - highest scores first
    transformed = transformed.sort_values("v", ascending=False)
    print(f"✅ Sorted by score (highest to lowest)")

    # Generate output file name
    output_file = output_dir / f"{channel_id}.csv"

    # Save the processed file
    transformed.to_csv(output_file, index=False)

    # Show statistics
    score_min = transformed["v"].min() if len(transformed) > 0 else 0
    score_max = transformed["v"].max() if len(transformed) > 0 else 0
    score_mean = transformed["v"].mean() if len(transformed) > 0 else 0

    print("📊 Statistics:")
    print(f"   Users: {len(transformed)}")
    print(f"   Score range: {score_min:.2f} - {score_max:.2f}")
    print(f"   Mean score: {score_mean:.2f}")
    print(f"💾 Saved to: {output_file}")


def main():
    """
    Main function to process all trust files
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Process trust scores with transformations"
    )
    parser.add_argument(
        "--sqrt",
        action="store_true",
        help="(Deprecated) Same as default - preserves relative differences",
    )
    parser.add_argument(
        "--quantile",
        action="store_true",
        help="(Deprecated) Same as default - preserves relative differences",
    )
    parser.add_argument(
        "--members-only",
        action="store_true",
        help="Exclude admins from scores (loads from raw/[channel_id]_admins.csv)",
    )

    args = parser.parse_args()

    # All methods now use the same linear normalization
    # Kept for backwards compatibility
    transform_func = apply_log_transformation
    transform_name = "linear"

    print("📊 Trust Score Processing")
    print("Normalization: Linear (0-1000 range, preserving relative differences)")
    print("Output format: user IDs (username mapping done in generate_json.py)")
    print()

    # Define directories
    scores_dir = Path("scores")
    output_dir = Path("output")

    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)

    # Find all CSV files in the trust directory
    csv_files = list(scores_dir.glob("*.csv"))

    if not csv_files:
        print("❌ No CSV files found in {} directory".format(scores_dir))
        print(f"   Make sure score files exist in the scores/ directory")
        sys.exit(1)

    print(f"Found {len(csv_files)} score file(s) to process...")

    # Process each CSV file
    for csv_file in csv_files:
        try:
            process_score_file(
                csv_file, output_dir, transform_func, transform_name, args.members_only
            )
        except Exception as e:
            print(f"❌ Error processing {csv_file}: {str(e)}")
            import traceback

            traceback.print_exc()

    print("\n" + "=" * 80)
    print("✅ Processing complete!")
    print(f"📁 Output saved to: {output_dir}/")
    print("=" * 80)


if __name__ == "__main__":
    main()
