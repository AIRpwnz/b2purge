import argparse
import b2sdk.v2 as b2
import os
from datetime import datetime, timedelta
from humanize import naturalsize


def delete_old_files(bucket_name, folder_path, days, dry_run):
    info = b2.InMemoryAccountInfo()
    b2_api = b2.B2Api(info)
    application_key_id = os.getenv("B2_APPLICATION_KEY_ID")
    application_key = os.getenv("B2_APPLICATION_KEY")
    b2_api.authorize_account("production", application_key_id, application_key)

    bucket = b2_api.get_bucket_by_name(bucket_name)
    folder_path = folder_path.rstrip("/") + '/'

    cutoff_date = datetime.now() - timedelta(days=days)

    total_bytes = 0
    file_count = 0

    for file_version, _ in bucket.ls(folder_path, recursive=True):
        file_mod_time = datetime.fromtimestamp(file_version.upload_timestamp / 1000)

        if file_mod_time < cutoff_date:
            file_size = file_version.size
            total_bytes += file_size
            file_count += 1

            if dry_run:
                print(
                    f"Dry run: Would delete {file_version.file_name} (last modified: {file_mod_time}, size: {naturalsize(file_size)})"
                )
            else:
                print(
                    f"Deleting {file_version.file_name} (last modified: {file_mod_time}, size: {naturalsize(file_size)})"
                )
                bucket.delete_file_version(file_version.id_, file_version.file_name)

    if dry_run:
        print(
            f"\nDry run summary: Would delete {file_count} files ({naturalsize(total_bytes)} would be freed)"
        )
    else:
        print(
            f"\nOperation complete: Deleted {file_count} files ({naturalsize(total_bytes)} freed)"
        )


def main():
    parser = argparse.ArgumentParser(
        description="Delete old files from B2 bucket.", allow_abbrev=False
    )
    parser.add_argument("bucket_name", type=str, help="Name of the B2 bucket")
    parser.add_argument(
        "folder_path", type=str, help="Path to the folder in the B2 bucket"
    )
    parser.add_argument(
        "days", type=int, help="Number of days old the files should be to be deleted"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Perform a dry run without deleting files",
    )

    args = parser.parse_args()

    delete_old_files(args.bucket_name, args.folder_path, args.days, args.dry_run)


if __name__ == "__main__":
    main()
