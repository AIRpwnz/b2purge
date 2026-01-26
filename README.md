# B2 Purge Script

This script deletes old files from a specified Backblaze B2 bucket based on their age in days. It supports both dry-run and actual deletion modes.

## Requirements

- Python 3.13
- `b2sdk` version 2.10.0
- `humanize` version 4.13.0

## Installation

1. Clone the repository:
    ```sh
    git clone <repository_url>
    cd <repository_directory>
    ```

2. Syncing the environment:
    ```sh
    uv sync
    ```

## Usage

To use the script, you need to set the `B2_APPLICATION_KEY_ID` and `B2_APPLICATION_KEY` environment variables with your Backblaze B2 credentials.

### Arguments

- `bucket_name`: Name of the B2 bucket.
- `folder_path`: Path to the folder in the B2 bucket.
- `days`: Number of days old the files should be to be deleted.
- `--dry-run`: Perform a dry run without deleting files.
- `--workers N`: Number of concurrent delete workers (default: auto-detected, ignored for dry run).
- `--batch-size N`: Number of files to process in each batch (default: 10000, affects memory usage).
- `--log-level LEVEL`: Logging level: DEBUG, INFO, WARNING, or ERROR (default: INFO).
- `--log-file PATH`: Path to log file (optional, logs to console if not specified).

### Example

Perform a dry run:
```sh
uv run python3 b2purge.py super-bucket folder 5 --dry-run
```

Delete files with DEBUG logging:
```sh
uv run python3 b2purge.py super-bucket folder 5 --log-level DEBUG
```

Save logs to file:
```sh
uv run python3 b2purge.py super-bucket folder 5 --log-file b2purge.log
```

Delete files with custom worker count and log to file:
```sh
uv run python3 b2purge.py super-bucket folder 5 --workers 4 --log-file b2purge.log
```

Process files with smaller batches for lower memory usage:
```sh
uv run python3 b2purge.py super-bucket folder 5 --batch-size 1000
```
