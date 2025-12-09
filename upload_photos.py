#!/usr/bin/env python3
"""
Upload Photos to S3

Uploads profile photos from raw/photos to S3 bucket.
Target: s3://openrank-files/telegram

Uses S3USERNAME and S3CREDENTIAL from .env file for AWS S3 authentication.
- S3USERNAME = AWS Access Key ID
- S3CREDENTIAL = AWS Secret Access Key

Usage:
    python3 upload_photos.py                    # Upload all photos
    python3 upload_photos.py --dry-run          # Show what would be uploaded without uploading
    python3 upload_photos.py --skip-existing    # Skip photos that already exist in S3

Requirements:
    - boto3 (install with: pip install boto3)
    - python-dotenv (install with: pip install python-dotenv)
"""

import argparse
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# S3 Configuration
S3_BUCKET = "openrank-files"
S3_PREFIX = "telegram"


def get_credentials():
    """Get S3 credentials from environment variables."""
    access_key = os.getenv("S3USERNAME")
    secret_key = os.getenv("S3CREDENTIAL")

    if not access_key or not secret_key:
        print("❌ Error: S3USERNAME and S3CREDENTIAL environment variables required")
        print("   Set them in your .env file")
        print("   S3USERNAME = AWS Access Key ID")
        print("   S3CREDENTIAL = AWS Secret Access Key")
        sys.exit(1)

    return access_key, secret_key


def create_s3_client(access_key: str, secret_key: str):
    """Create an S3 client with the provided credentials."""
    return boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )


def check_file_exists(s3_client, bucket: str, key: str) -> bool:
    """Check if a file already exists in S3."""
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        # Re-raise other errors
        raise


def upload_file(
    s3_client,
    local_path: Path,
    bucket: str,
    s3_key: str,
    dry_run: bool = False,
) -> tuple[str, bool, str]:
    """
    Upload a single file to S3.

    Returns:
        tuple: (filename, success, message)
    """
    if dry_run:
        return (local_path.name, True, f"Would upload to s3://{bucket}/{s3_key}")

    try:
        s3_client.upload_file(
            str(local_path),
            bucket,
            s3_key,
            ExtraArgs={"ContentType": "image/jpeg"},
        )
        return (local_path.name, True, "Uploaded successfully")

    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        error_message = e.response["Error"]["Message"]
        return (local_path.name, False, f"AWS Error {error_code}: {error_message}")
    except IOError as e:
        return (local_path.name, False, f"IO error: {e}")


def upload_photos(
    photos_dir: Path,
    skip_existing: bool = False,
    dry_run: bool = False,
    max_workers: int = 5,
):
    """
    Upload all photos from the photos directory to S3.

    Args:
        photos_dir: Path to the directory containing photos
        skip_existing: If True, skip files that already exist in S3
        dry_run: If True, don't actually upload, just show what would be done
        max_workers: Number of concurrent upload threads
    """
    # Get credentials and create S3 client
    access_key, secret_key = get_credentials()
    s3_client = create_s3_client(access_key, secret_key)

    # Get list of photos to upload
    photos = list(photos_dir.glob("*.jpg"))
    if not photos:
        print("❌ No photos found in", photos_dir)
        return

    print(f"📷 Found {len(photos)} photos to process")

    if dry_run:
        print("🔍 Dry run mode - no files will be uploaded\n")

    uploaded = 0
    skipped = 0
    failed = 0

    # Filter out existing files if skip_existing is True
    photos_to_upload = []
    if skip_existing and not dry_run:
        print("🔍 Checking for existing files...")
        for i, photo in enumerate(photos):
            s3_key = f"{S3_PREFIX}/{photo.name}"
            if check_file_exists(s3_client, S3_BUCKET, s3_key):
                skipped += 1
            else:
                photos_to_upload.append(photo)

            # Progress update every 100 files
            if (i + 1) % 100 == 0:
                print(f"   Checked {i + 1}/{len(photos)} files...")

        print(f"   Skipping {skipped} existing files")
        print(f"   {len(photos_to_upload)} files to upload\n")
    else:
        photos_to_upload = photos

    if not photos_to_upload:
        print("✅ All files already exist in S3")
        return

    # Upload files with thread pool
    # Note: boto3 client is thread-safe for upload operations
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        for photo in photos_to_upload:
            s3_key = f"{S3_PREFIX}/{photo.name}"
            future = executor.submit(
                upload_file, s3_client, photo, S3_BUCKET, s3_key, dry_run
            )
            futures[future] = photo

        total = len(futures)
        completed = 0

        for future in as_completed(futures):
            completed += 1
            filename, success, message = future.result()

            if success:
                uploaded += 1
            else:
                failed += 1
                print(f"   ❌ {filename}: {message}")

            # Print progress every 50 files or at the end
            if completed % 50 == 0 or completed == total:
                print(
                    f"   📤 Progress: {completed}/{total} - "
                    f"Uploaded: {uploaded}, Skipped: {skipped}, Failed: {failed}"
                )

    # Print summary
    print(f"\n{'=' * 60}")
    print("📊 Summary")
    print(f"{'=' * 60}")
    print(f"   Total processed: {len(photos)}")
    print(f"   Uploaded: {uploaded}")
    print(f"   Skipped: {skipped}")
    print(f"   Failed: {failed}")
    print(f"   Destination: s3://{S3_BUCKET}/{S3_PREFIX}/")
    print()


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Upload profile photos to S3",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python3 upload_photos.py                    # Upload all photos
    python3 upload_photos.py --skip-existing    # Skip existing files
    python3 upload_photos.py --dry-run          # Preview without uploading
    python3 upload_photos.py --workers 10       # Use 10 concurrent uploads
        """,
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip photos that already exist in S3",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be uploaded without actually uploading",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=5,
        help="Number of concurrent upload threads (default: 5)",
    )

    args = parser.parse_args()

    print("📤 S3 Photo Uploader")
    print(f"   Target: s3://{S3_BUCKET}/{S3_PREFIX}/\n")

    # Find photos directory
    photos_dir = Path(__file__).parent / "raw" / "photos"
    if not photos_dir.exists():
        print(f"❌ Error: Photos directory not found: {photos_dir}")
        print("   Run download_photos.py first to download profile photos")
        sys.exit(1)

    upload_photos(
        photos_dir=photos_dir,
        skip_existing=args.skip_existing,
        dry_run=args.dry_run,
        max_workers=args.workers,
    )


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
