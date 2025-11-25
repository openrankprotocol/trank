#!/usr/bin/env python3
"""
Score Processing Script

This script processes trust score files from the trust/ directory by:
1. Loading all CSV trust files (i,j,v format with user IDs)
2. Aggregating scores by user (summing incoming trust)
3. Normalizing scores to 0-1000 range while preserving relative differences
4. Sorting by score (descending)
5. Saving results to output/ directory

Usage:
    python3 process_scores.py

Note: The --log, --sqrt, and --quantile flags are kept for backwards compatibility
but all now apply the same linear normalization to preserve relative score differences.

Requirements:
    - pandas (install with: pip install pandas)
    - CSV files in trust/ directory with columns 'i', 'j', 'v'

Output:
    - Creates output/ directory if it doesn't exist
    - For each input file (e.g., 4886853134.csv), creates:
      - output/4886853134.csv with normalized scores (user IDs only)
    - Scores are normalized to 0-1000 range, preserving relative differences
    - Sorted by score (descending)
    - User ID to username mapping is done in generate_json.py
"""

import argparse
import sys
from pathlib import Path

import pandas as pd


def aggregate_scores(df):
    """
    Aggregate trust scores by summing incoming trust.

    Processes raw trust edges (i -> j with value v) and calculates
    total incoming trust for each user by summing all edges where
    they are the recipient (j column).

    Args:
        df: DataFrame with columns i, j, v (from_user_id -> to_user_id -> score)

    Returns:
        DataFrame with columns i (user_id) and v (total incoming trust score)
    """
    # Sum all incoming trust for each user (group by j column)
    # j is the recipient of trust, so we aggregate by j
    aggregated = df.groupby("j")["v"].sum().reset_index()
    aggregated.columns = ["i", "v"]  # Rename j to i for consistency
    return aggregated


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


def process_trust_file(input_file, output_dir, transform_func, transform_name):
    """
    Process a single trust file by aggregating, transforming, and saving

    Args:
        input_file: Path to input CSV file
        output_dir: Directory to save processed files
        transform_func: Transformation function to apply
        transform_name: Name of the transformation
    """
    # Extract channel ID from filename
    channel_id = Path(input_file).stem

    print(f"\n{'=' * 80}")
    print(f"Processing: {input_file}")
    print(f"Channel ID: {channel_id}")
    print(f"Transformation: {transform_name}")
    print(f"{'=' * 80}")

    # Load the trust CSV file (i,j,v format - raw edges)
    df = pd.read_csv(input_file)
    print(f"✅ Loaded {len(df)} trust edges")

    # Aggregate scores by summing incoming trust
    aggregated = aggregate_scores(df)
    print(f"✅ Aggregated to {len(aggregated)} users with incoming trust scores")

    # Keep user IDs as-is (no conversion to usernames)
    # Username mapping will be done in generate_json.py
    print("📋 Keeping user IDs in output")

    # Apply normalization
    transformed = transform_func(aggregated.copy())
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
    scores_dir = Path("trust")
    output_dir = Path("output")

    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)

    # Find all CSV files in the trust directory
    csv_files = list(scores_dir.glob("*.csv"))

    if not csv_files:
        print("❌ No CSV files found in {} directory".format(scores_dir))
        print(f"   Run 'python generate_trust.py' first to create trust files")
        sys.exit(1)

    print(f"Found {len(csv_files)} trust file(s) to process...")

    # Process each CSV file
    for csv_file in csv_files:
        try:
            process_trust_file(csv_file, output_dir, transform_func, transform_name)
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
