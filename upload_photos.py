#!/usr/bin/env python3
"""
Upload Photos to S3

Uploads profile photos from raw/photos to S3 bucket.
Target: s3://openrank-files/telegram

Uses S3USERNAME and S3CREDENTIAL from .env file for AWS S3 authentication.
- S3USERNAME = AWS Access Key ID
- S3CREDENTIAL = AWS Secret Access Key

Usage:
    python3 upload_photos.py                    # Upload new photos (skips existing)
    python3 upload_photos.py --file 123.jpg     # Upload a specific file
    python3 upload_photos.py --force            # Re-upload all photos (overwrite existing)
    python3 upload_photos.py --dry-run          # Show what would be uploaded without uploading
    python3 upload_photos.py --check 123.jpg    # Check why an image might be missing
    python3 upload_photos.py --check-all        # Check all user photos from database

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
import psycopg2
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


def get_db_connection():
    """Get database connection from DATABASE_URL environment variable."""
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL environment variable is required")

    return psycopg2.connect(database_url)


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


def check_file_status(
    filename: str,
    photos_dir: Path,
):
    """
    Check the status of a file both locally and in S3.

    Args:
        filename: Filename to check (e.g., "123.jpg")
        photos_dir: Path to the local photos directory
    """
    # Get credentials and create S3 client
    access_key, secret_key = get_credentials()
    s3_client = create_s3_client(access_key, secret_key)

    # Normalize filename
    if not filename.endswith(".jpg"):
        filename = f"{filename}.jpg"

    local_path = photos_dir / filename
    s3_key = f"{S3_PREFIX}/{filename}"

    print(f"🔍 Checking status for: {filename}\n")

    # Check local file
    print("📁 Local file:")
    if local_path.exists():
        size = local_path.stat().st_size
        print(f"   ✅ Exists at: {local_path}")
        print(f"   📏 Size: {size} bytes")
    else:
        print(f"   ❌ Not found at: {local_path}")
        print(f"   💡 This user likely has no profile photo in Telegram")

    # Check S3
    print(f"\n☁️  S3 (s3://{S3_BUCKET}/{s3_key}):")
    try:
        response = s3_client.head_object(Bucket=S3_BUCKET, Key=s3_key)
        print(f"   ✅ Exists in S3")
        print(f"   📏 Size: {response.get('ContentLength')} bytes")
        print(f"   📄 Content-Type: {response.get('ContentType')}")
        print(f"   📅 Last Modified: {response.get('LastModified')}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            print(f"   ❌ Not found in S3")
            if not local_path.exists():
                print(f"   💡 File doesn't exist locally, so it was never uploaded")
            else:
                print(f"   💡 File exists locally but hasn't been uploaded yet")
                print(f"   💡 Run: python upload_photos.py --file {filename}")
        else:
            print(f"   ❌ Error: {e.response['Error']['Message']}")

    # Show CloudFront URL
    print(f"\n🌐 CloudFront URL:")
    print(f"   https://d3n05cafj616pw.cloudfront.net/{s3_key}")

    print()


def check_all_files(photos_dir: Path):
    """
    Check status of all user photos from database.
    Shows which users have photos locally, in S3, or missing entirely.
    """
    # Get credentials and create S3 client
    access_key, secret_key = get_credentials()
    s3_client = create_s3_client(access_key, secret_key)

    # Get all user IDs from database
    print("📊 Fetching user IDs from database...")
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT DISTINCT user_id FROM trank.channel_users ORDER BY user_id"
        )
        user_ids = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"❌ Error connecting to database: {e}")
        sys.exit(1)

    print(f"   Found {len(user_ids)} unique users\n")

    # Check each user
    local_only = []
    s3_only = []
    both = []
    neither = []

    print("🔍 Checking files...")
    for i, user_id in enumerate(user_ids):
        filename = f"{user_id}.jpg"
        local_path = photos_dir / filename
        s3_key = f"{S3_PREFIX}/{filename}"

        local_exists = local_path.exists()

        # Check S3
        try:
            s3_client.head_object(Bucket=S3_BUCKET, Key=s3_key)
            s3_exists = True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                s3_exists = False
            else:
                print(
                    f"   ⚠️  Error checking {filename}: {e.response['Error']['Message']}"
                )
                s3_exists = False

        if local_exists and s3_exists:
            both.append(user_id)
        elif local_exists and not s3_exists:
            local_only.append(user_id)
        elif not local_exists and s3_exists:
            s3_only.append(user_id)
        else:
            neither.append(user_id)

        # Progress update every 100 users
        if (i + 1) % 100 == 0:
            print(f"   Checked {i + 1}/{len(user_ids)} users...")

    # Print summary
    print(f"\n{'=' * 60}")
    print("📊 Summary")
    print(f"{'=' * 60}")
    print(f"   Total users in database: {len(user_ids)}")
    print(f"   ✅ In both local & S3:   {len(both)}")
    print(f"   📁 Local only (not uploaded): {len(local_only)}")
    print(f"   ☁️  S3 only (deleted locally): {len(s3_only)}")
    print(f"   ❌ No photo (neither):   {len(neither)}")

    # Show details for problematic cases
    if local_only:
        print(f"\n📁 Files to upload ({len(local_only)}):")
        for user_id in local_only[:10]:
            print(f"   - {user_id}.jpg")
        if len(local_only) > 10:
            print(f"   ... and {len(local_only) - 10} more")
        print(f"   💡 Run: python upload_photos.py")

    if neither:
        print(f"\n❌ Users without photos ({len(neither)}):")
        for user_id in neither[:10]:
            print(f"   - {user_id}")
        if len(neither) > 10:
            print(f"   ... and {len(neither) - 10} more")
        print(f"   💡 These users have no profile photo in Telegram")

    print()


def upload_single_file(
    file_path: Path,
    force: bool = False,
    dry_run: bool = False,
):
    """
    Upload a single specific file to S3.

    Args:
        file_path: Path to the file to upload
        force: If True, upload even if file exists in S3
        dry_run: If True, don't actually upload
    """
    # Get credentials and create S3 client
    access_key, secret_key = get_credentials()
    s3_client = create_s3_client(access_key, secret_key)

    s3_key = f"{S3_PREFIX}/{file_path.name}"

    # Check if file exists unless force is True
    if not force and not dry_run:
        if check_file_exists(s3_client, S3_BUCKET, s3_key):
            print(f"⏭️  File already exists in S3: {file_path.name}")
            print(f"   Use --force to overwrite")
            return

    filename, success, message = upload_file(
        s3_client, file_path, S3_BUCKET, s3_key, dry_run
    )

    if success:
        print(f"✅ {filename}: {message}")
    else:
        print(f"❌ {filename}: {message}")


def upload_photos(
    photos_dir: Path,
    force: bool = False,
    dry_run: bool = False,
    max_workers: int = 5,
):
    """
    Upload all photos from the photos directory to S3.

    Args:
        photos_dir: Path to the directory containing photos
        force: If True, re-upload all files (overwrite existing)
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

    # Filter out existing files unless force is True
    photos_to_upload = []
    if not force and not dry_run:
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
    python3 upload_photos.py                    # Upload new photos (skips existing)
    python3 upload_photos.py --file 123.jpg     # Upload a specific file
    python3 upload_photos.py --check 123.jpg    # Check why an image might be missing
    python3 upload_photos.py --check-all        # Check all user photos from database
    python3 upload_photos.py --force            # Re-upload all (overwrite existing)
    python3 upload_photos.py --dry-run          # Preview without uploading
    python3 upload_photos.py --workers 10       # Use 10 concurrent uploads
        """,
    )
    parser.add_argument(
        "--file",
        "-f",
        type=str,
        help="Upload a specific file (filename or path)",
    )
    parser.add_argument(
        "--check",
        "-c",
        type=str,
        help="Check status of a file (locally and in S3)",
    )
    parser.add_argument(
        "--check-all",
        action="store_true",
        help="Check status of all user photos from database",
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        default=True,
        help="Skip photos that already exist in S3 (default: True)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-upload all files, overwriting existing ones in S3",
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

    # Handle check mode
    if args.check:
        check_file_status(args.check, photos_dir)
        return

    # Handle check-all mode
    if args.check_all:
        check_all_files(photos_dir)
        return

    # Handle single file upload
    if args.file:
        file_path = Path(args.file)

        # If just a filename, look in photos_dir
        if not file_path.exists():
            file_path = photos_dir / args.file

        if not file_path.exists():
            print(f"❌ Error: File not found: {args.file}")
            print(f"   Looked in: {photos_dir}")
            sys.exit(1)

        upload_single_file(
            file_path=file_path,
            force=args.force,
            dry_run=args.dry_run,
        )
        return

    # Upload all photos
    if not photos_dir.exists():
        print(f"❌ Error: Photos directory not found: {photos_dir}")
        print("   Run download_photos.py first to download profile photos")
        sys.exit(1)

    upload_photos(
        photos_dir=photos_dir,
        force=args.force,
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
