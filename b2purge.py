import argparse
import logging
import os
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler
from typing import cast

import b2sdk.v2 as b2
from humanize import naturalsize

# Keep a conservative default to reduce API rate-limit risk.
DEFAULT_WORKERS = max(1, min(8, (os.cpu_count() or 1) * 2))
DEFAULT_BATCH_SIZE = 10000
DEFAULT_MAX_RETRIES = 5
RETRY_BASE_DELAY = 0.5
RETRY_MAX_DELAY = 8.0


def setup_logging(log_level: str, log_file: str | None = None) -> logging.Logger:
    logger = logging.getLogger("b2purge")
    logger.setLevel(getattr(logging, log_level))
    logger.handlers.clear()

    console_handler = logging.StreamHandler()
    console_formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    if log_file:
        file_handler = RotatingFileHandler(
            log_file, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
        )
        file_formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

    return logger


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


def batch_generator(bucket, folder_path, cutoff_ms, batch_size):
    """Yields batches of FileCandidate objects to limit memory usage."""
    batch = []
    for file_version, _ in bucket.ls(folder_path, recursive=True):
        if file_version.upload_timestamp < cutoff_ms:
            batch.append(
                FileCandidate(
                    file_id=file_version.id_,
                    file_name=file_version.file_name,
                    file_size=file_version.size,
                    upload_timestamp_ms=file_version.upload_timestamp,
                )
            )
            if len(batch) >= batch_size:
                yield batch
                batch = []
    if batch:
        yield batch


def delete_old_files(bucket_name, folder_path, days, dry_run, workers, batch_size, logger):
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

    if dry_run:
        total_bytes = 0
        total_candidates = 0
        total_scanned = 0
        batch_num = 0
        
        for batch in batch_generator(bucket, folder_path, cutoff_ms, batch_size):
            batch_num += 1
            batch_candidates = len(batch)
            total_candidates += batch_candidates
            total_scanned += batch_size
            
            for candidate in batch:
                file_mod_time = datetime.fromtimestamp(candidate.upload_timestamp_ms / 1000)
                total_bytes += candidate.file_size
                logger.info(
                    f"Dry run: Would delete {candidate.file_name} (last modified: {file_mod_time}, size: {naturalsize(candidate.file_size)})"
                )
            
            logger.info(
                f"Batch {batch_num} complete: {batch_candidates} candidates in this batch, {total_candidates} total candidates found"
            )
        
        logger.info(
            f"Dry run summary: Would delete {total_candidates} files ({naturalsize(total_bytes)} would be cleared)"
        )
        return

    def delete_candidate(candidate):
        for attempt in range(DEFAULT_MAX_RETRIES + 1):
            try:
                bucket.delete_file_version(candidate.file_id, candidate.file_name)
                return candidate
            except Exception as exc:
                if not is_rate_limit_error(exc) or attempt == DEFAULT_MAX_RETRIES:
                    raise
                delay = min(RETRY_MAX_DELAY, RETRY_BASE_DELAY * (2**attempt))
                delay *= 0.8 + random.random() * 0.4
                logger.warning(
                    f"Rate limit hit for {candidate.file_name}, retrying in {delay:.2f}s (attempt {attempt + 1}/{DEFAULT_MAX_RETRIES})"
                )
                time.sleep(delay)

    deleted_bytes = 0
    deleted_count = 0
    failed_count = 0
    failed_bytes = 0
    batch_num = 0

    for batch in batch_generator(bucket, folder_path, cutoff_ms, batch_size):
        batch_num += 1
        
        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_candidate = {
                executor.submit(delete_candidate, candidate): candidate
                for candidate in batch
            }
            for future in as_completed(future_to_candidate):
                candidate = future_to_candidate[future]
                file_mod_time = datetime.fromtimestamp(candidate.upload_timestamp_ms / 1000)
                try:
                    future.result()
                except Exception as exc:
                    failed_count += 1
                    failed_bytes += candidate.file_size
                    logger.error(
                        f"Failed to delete {candidate.file_name} (last modified: {file_mod_time}, size: {naturalsize(candidate.file_size)}): {exc}"
                    )
                else:
                    deleted_bytes += candidate.file_size
                    deleted_count += 1
                    logger.info(
                        f"Deleted {candidate.file_name} (last modified: {file_mod_time}, size: {naturalsize(candidate.file_size)})"
                    )
        
        logger.info(
            f"Batch {batch_num} complete: {deleted_count} total deleted, {failed_count} total failed"
        )

    if failed_count > 0:
        logger.warning(
            f"Operation complete: Deleted {deleted_count} files ({naturalsize(deleted_bytes)} cleared), {failed_count} failed ({naturalsize(failed_bytes)} not cleared)"
        )
    else:
        logger.info(
            f"Operation complete: Deleted {deleted_count} files ({naturalsize(deleted_bytes)} cleared)"
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
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help="Number of files to process in each batch (affects memory usage)",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level (default: INFO)",
    )
    parser.add_argument(
        "--log-file",
        type=str,
        help="Path to log file (optional, logs to console if not specified)",
    )

    args = parser.parse_args()

    if args.days <= 0:
        parser.error("days must be a positive integer")

    if args.workers <= 0:
        parser.error("workers must be a positive integer")

    if args.batch_size <= 0:
        parser.error("batch-size must be a positive integer")

    logger = setup_logging(args.log_level, args.log_file)

    delete_old_files(
        args.bucket_name,
        args.folder_path,
        args.days,
        args.dry_run,
        args.workers,
        args.batch_size,
        logger,
    )


if __name__ == "__main__":
    main()
