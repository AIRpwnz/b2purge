import argparse
import os
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import cast

import b2sdk.v2 as b2
from humanize import naturalsize

# Keep a conservative default to reduce API rate-limit risk.
DEFAULT_WORKERS = max(1, min(8, (os.cpu_count() or 1) * 2))
DEFAULT_MAX_RETRIES = 5
RETRY_BASE_DELAY = 0.5
RETRY_MAX_DELAY = 8.0


@dataclass(frozen=True)
class FileCandidate:
    # Immutable snapshot so threaded deletes don't share mutable state.
    file_id: str
    file_name: str
    file_size: int
    upload_timestamp_ms: int


def is_rate_limit_error(exc):
    status = getattr(exc, "status", None) or getattr(exc, "status_code", None)
    if status == 429:
        return True
    code = getattr(exc, "code", None) or getattr(exc, "error_code", None)
    if isinstance(code, str) and code.lower() in {
        "too_many_requests",
        "rate_limit_exceeded",
    }:
        return True
    message = str(exc).lower()
    return "too many requests" in message or "rate limit" in message or "429" in message


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

    # Compare raw timestamps to avoid per-file datetime conversions.
    cutoff_ms = int((datetime.now() - timedelta(days=days)).timestamp() * 1000)

    candidates = []
    for file_version, _ in bucket.ls(folder_path, recursive=True):
        if file_version.upload_timestamp < cutoff_ms:
            # Collect once so dry-run and delete paths share the same candidate set.
            candidates.append(
                FileCandidate(
                    file_id=file_version.id_,
                    file_name=file_version.file_name,
                    file_size=file_version.size,
                    upload_timestamp_ms=file_version.upload_timestamp,
                )
            )

    if dry_run:
        # Dry-run is intentionally sequential to keep output deterministic.
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
        # Delete a specific version; B2 identifies versions by (id, name).
        for attempt in range(DEFAULT_MAX_RETRIES + 1):
            try:
                bucket.delete_file_version(candidate.file_id, candidate.file_name)
                return candidate
            except Exception as exc:
                if not is_rate_limit_error(exc) or attempt == DEFAULT_MAX_RETRIES:
                    raise
                delay = min(RETRY_MAX_DELAY, RETRY_BASE_DELAY * (2**attempt))
                delay *= 0.8 + random.random() * 0.4
                time.sleep(delay)

    deleted_bytes = 0
    deleted_count = 0
    failed = []

    with ThreadPoolExecutor(max_workers=workers) as executor:
        # Schedule deletes in parallel with bounded concurrency.
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
