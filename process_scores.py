#!/usr/bin/env python3
"""
Score Processing Script

This script processes trust score files from the scores/ directory by:
1. Loading all CSV trust files (i,j,v format)
2. Aggregating scores by user (summing incoming trust)
3. Normalizing scores to 0-1000 range while preserving relative differences
4. Sorting by score (descending)
5. Saving results to output/ directory

Usage:
    python3 process_scores.py              # Process with usernames in output
    python3 process_scores.py --with-user-ids  # Output user IDs instead of usernames

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


def load_user_ids_mapping(channel_id):
    """
    Load user ID to username mapping from raw/[channel_id]_user_ids.csv

    Args:
        channel_id: Channel ID

    Returns:
        dict: Mapping of username to user_id, or empty dict if file not found
    """
    try:
        mapping_file = Path(f"raw/{channel_id}_user_ids.csv")

        if not mapping_file.exists():
            print(f"    ⚠️  Warning: {mapping_file} not found")
            return {}

        print(f"    📋 Loading user mapping from: {mapping_file}")

        # Load as dataframe
        mapping_df = pd.read_csv(mapping_file)

        # Create dictionary mapping username -> user_id
        username_to_id = dict(
            zip(mapping_df["username"], mapping_df["user_id"].astype(str))
        )

        print(f"    ✅ Loaded {len(username_to_id)} username mappings")
        return username_to_id

    except Exception as e:
        print(f"    ⚠️  Warning: Could not load user mapping: {str(e)}")
        return {}


def aggregate_scores(df):
    """
    Scores are already aggregated in the scores/ directory - just return as is

    Args:
        df: DataFrame with columns i, v (user -> value)

    Returns:
        DataFrame with columns i (user) and v (total trust score)
    """
    # Scores are already aggregated, no need to group
    return df


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


def process_trust_file(
    input_file, output_dir, transform_func, transform_name, with_user_ids=False
):
    """
    Process a single trust file by aggregating, transforming, and saving

    Args:
        input_file: Path to input CSV file
        output_dir: Directory to save processed files
        transform_func: Transformation function to apply
        transform_name: Name of the transformation
        with_user_ids: Whether to output user IDs instead of usernames
    """
    # Extract channel ID from filename
    channel_id = Path(input_file).stem

    print(f"\n{'=' * 80}")
    print(f"Processing: {input_file}")
    print(f"Channel ID: {channel_id}")
    print(f"Transformation: {transform_name}")
    print(f"{'=' * 80}")

    # Load the scores CSV file (i,v format - already aggregated)
    df = pd.read_csv(input_file)
    print(f"✅ Loaded {len(df)} users with scores")

    # Scores are already aggregated, just pass through
    aggregated = aggregate_scores(df)

    # If --with-user-ids flag is passed, convert usernames to user IDs
    if with_user_ids:
        username_to_id = load_user_ids_mapping(channel_id)
        if username_to_id:
            # Map usernames to user IDs
            aggregated["i"] = (
                aggregated["i"].map(username_to_id).fillna(aggregated["i"])  # type: ignore[arg-type]
            )
            converted_count = sum(
                1 for user in df["j"].unique() if user in username_to_id
            )
            print(f"✅ Converted {converted_count} usernames to user IDs")
        else:
            print(f"⚠️  No user ID mapping found, keeping usernames")

    # Apply normalization
    transformed = transform_func(aggregated.copy())
    print(f"✅ Applied linear normalization (preserving relative differences)")

    # Don't sort - preserve the order from the trust file
    # The trust file is already sorted by the trust score generation algorithm

    # Generate output file name
    output_file = output_dir / f"{channel_id}.csv"

    # Save the processed file
    transformed.to_csv(output_file, index=False)

    # Show statistics
    score_min = transformed["v"].min() if len(transformed) > 0 else 0
    score_max = transformed["v"].max() if len(transformed) > 0 else 0
    score_mean = transformed["v"].mean() if len(transformed) > 0 else 0

    print(f"📊 Statistics:")
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
        "--with-user-ids",
        action="store_true",
        help="Output user IDs instead of usernames (requires user_ids.csv)",
    )
    args = parser.parse_args()

    # All methods now use the same linear normalization
    # Kept for backwards compatibility
    transform_func = apply_log_transformation
    transform_name = "linear"

    print("📊 Trust Score Processing")
    print("Normalization: Linear (0-1000 range, preserving relative differences)")
    print(f"Output format: {'user IDs' if args.with_user_ids else 'usernames'}")
    print()

    # Define directories
    scores_dir = Path("scores")
    output_dir = Path("output")

    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)

    # Find all CSV files in the scores directory
    csv_files = list(scores_dir.glob("*.csv"))

    if not csv_files:
        print(f"❌ No CSV files found in {scores_dir} directory")
        print(f"   Run 'python generate_trust.py' first to create scores")
        sys.exit(1)

    print(f"Found {len(csv_files)} score file(s) to process...")

    # Process each CSV file
    for csv_file in csv_files:
        try:
            process_trust_file(
                csv_file, output_dir, transform_func, transform_name, args.with_user_ids
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
