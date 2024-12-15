# B2 Purge Script

This script deletes old files from a specified Backblaze B2 bucket based on their age in days. It supports both dry-run and actual deletion modes.

## Requirements

- Python 3.13
- `b2sdk` version 2.7.0

## Installation

1. Clone the repository:
    ```sh
    git clone <repository_url>
    cd <repository_directory>
    ```

2. Create a virtual environment and activate it:
    ```sh
    python3 -m venv .venv
    source .venv/bin/activate
    ```

3. Install the required packages:
    ```sh
    pip install -r requirements.txt
    ```

## Usage

To use the script, you need to set the `B2_APPLICATION_KEY_ID` and `B2_APPLICATION_KEY` environment variables with your Backblaze B2 credentials.

### Arguments

- `bucket_name`: Name of the B2 bucket.
- `folder_path`: Path to the folder in the B2 bucket.
- `days`: Number of days old the files should be to be deleted.
- `--dry-run`: Perform a dry run without deleting files.

### Example

Perform a dry run:
```sh
python3 b2purge.py super-bucket folder 5 --dry-run
```