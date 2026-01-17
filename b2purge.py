import argparse
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import cast

import b2sdk.v2 as b2
from humanize import naturalsize

DEFAULT_WORKERS = max(1, min(8, (os.cpu_count() or 1) * 2))


@dataclass(frozen=True)
class FileCandidate:
    file_id: str
    file_name: str
    file_size: int
    upload_timestamp_ms: int


def delete_old_files(bucket_name, folder_path, days, dry_run, workers):
    info = cast(b2.AbstractAccountInfo, b2.InMemoryAccountInfo())
    b2_api = b2.B2Api(info)
    application_key_id = os.getenv("B2_APPLICATION_KEY_ID")
    application_key = os.getenv("B2_APPLICATION_KEY")
    if not application_key_id or not application_key:
        raise RuntimeError(
            "Missing B2 credentials. Set B2_APPLICATION_KEY_ID and B2_APPLICATION_KEY."
        )
    b2_api.authorize_account("production", application_key_id, application_key)

    bucket = b2_api.get_bucket_by_name(bucket_name)
    folder_path = folder_path.rstrip("/") + "/"

    cutoff_ms = int((datetime.now() - timedelta(days=days)).timestamp() * 1000)

    candidates = []
    for file_version, _ in bucket.ls(folder_path, recursive=True):
        if file_version.upload_timestamp < cutoff_ms:
            candidates.append(
                FileCandidate(
                    file_id=file_version.id_,
                    file_name=file_version.file_name,
                    file_size=file_version.size,
                    upload_timestamp_ms=file_version.upload_timestamp,
                )
            )

    if dry_run:
        total_bytes = 0
        for candidate in candidates:
            file_mod_time = datetime.fromtimestamp(candidate.upload_timestamp_ms / 1000)
            total_bytes += candidate.file_size
            print(
                f"Dry run: Would delete {candidate.file_name} (last modified: {file_mod_time}, size: {naturalsize(candidate.file_size)})"
            )
        print(
            f"\nDry run summary: Would delete {len(candidates)} files ({naturalsize(total_bytes)} would be cleared)"
        )
        return

    def delete_candidate(candidate):
        bucket.delete_file_version(candidate.file_id, candidate.file_name)
        return candidate

    deleted_bytes = 0
    deleted_count = 0
    failed = []

    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_candidate = {
            executor.submit(delete_candidate, candidate): candidate
            for candidate in candidates
        }
        for future in as_completed(future_to_candidate):
            candidate = future_to_candidate[future]
            file_mod_time = datetime.fromtimestamp(candidate.upload_timestamp_ms / 1000)
            try:
                future.result()
            except Exception as exc:
                failed.append(candidate)
                print(
                    f"Failed to delete {candidate.file_name} (last modified: {file_mod_time}, size: {naturalsize(candidate.file_size)}): {exc}"
                )
            else:
                deleted_bytes += candidate.file_size
                deleted_count += 1
                print(
                    f"Deleted {candidate.file_name} (last modified: {file_mod_time}, size: {naturalsize(candidate.file_size)})"
                )

    if failed:
        failed_bytes = sum(candidate.file_size for candidate in failed)
        print(
            f"\nOperation complete: Deleted {deleted_count} files ({naturalsize(deleted_bytes)} cleared), {len(failed)} failed ({naturalsize(failed_bytes)} not cleared)"
        )
    else:
        print(
            f"\nOperation complete: Deleted {deleted_count} files ({naturalsize(deleted_bytes)} cleared)"
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
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help="Number of concurrent delete workers (ignored for dry run)",
    )

    args = parser.parse_args()

    if args.days <= 0:
        parser.error("days must be a positive integer")

    if args.workers <= 0:
        parser.error("workers must be a positive integer")

    delete_old_files(
        args.bucket_name,
        args.folder_path,
        args.days,
        args.dry_run,
        args.workers,
    )


if __name__ == "__main__":
    main()
