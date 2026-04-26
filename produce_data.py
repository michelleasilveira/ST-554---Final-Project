"""
produce_data.py
---------------
Streaming data producer for the Final Project (Power Consumption / Tetouan).

Reads the file ``power_streaming_data.csv`` into a regular pandas DataFrame and
drops a small CSV file containing 5 randomly sampled rows into the watched
folder every 10 seconds, repeating for 20 iterations.

The Spark Structured Streaming notebook monitors that folder and predicts on
every newly-arrived file.

Usage
-----
    python produce_data.py

Optional environment variables / CLI overrides:
    --source       Path to power_streaming_data.csv         (default: ./power_streaming_data.csv)
    --stream-dir   Folder the notebook watches               (default: ./stream_data)
    --iters        Number of CSV files to write              (default: 20)
    --rows         Rows to sample per file                   (default: 5)
    --pause        Seconds to sleep between files            (default: 10)

The script will create the stream folder if it does not already exist. 
I do this because I am building mostly in Colab so sometimes  the user could forget to create the streamming folder
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd


# Read the CLI flags so we can override the defaults without touching the code.
def parse_args() -> argparse.Namespace:
    """Read command-line flags. The defaults match what the project description asks for."""
    parser = argparse.ArgumentParser(
        description="Stream rows from power_streaming_data.csv into a watched folder.",
    )
    # Where the source CSV lives. Falls back to the env var, then to the cwd.
    parser.add_argument(
        "--source",
        default=os.environ.get("STREAM_SOURCE", "power_streaming_data.csv"),
        help="Path to the source CSV (power_streaming_data.csv).",
    )
    # Where to drop the new CSV files. The notebook needs to be watching this same folder.
    parser.add_argument(
        "--stream-dir",
        default=os.environ.get("STREAM_DIR", "stream_data"),
        help="Folder the Spark stream is monitoring.",
    )
    parser.add_argument("--iters", type=int, default=20, help="Number of files to write.")
    parser.add_argument("--rows", type=int, default=5, help="Rows to sample per file.")
    parser.add_argument("--pause", type=int, default=10, help="Seconds to sleep between writes.")
    return parser.parse_args()


# Main loop: read the data once, then drop a small sample into the stream folder every few seconds.
def main() -> int:
    """Read the source data once, then drop sampled CSV chunks on a fixed cadence."""
    args = parse_args()

    # expanduser handles ~, resolve gives an absolute path so the messages are clearer.
    source_path = Path(args.source).expanduser().resolve()
    stream_dir = Path(args.stream_dir).expanduser().resolve()

    # Bail out early with a clear message if the source CSV isn't where we expect.
    if not source_path.is_file():
        print(f"[producer] ERROR: source file not found: {source_path}", file=sys.stderr)
        return 1

    # Create the watched folder if it doesn't exist yet. Spark will start picking
    # up files the moment they show up here.
    stream_dir.mkdir(parents=True, exist_ok=True)

    # Load the whole CSV once up front -- no point re-reading it on every loop.
    print(f"[producer] reading source data from {source_path}")
    df = pd.read_csv(source_path)
    print(f"[producer] loaded {len(df):,} rows; columns = {list(df.columns)}")
    print(f"[producer] writing {args.iters} files of {args.rows} rows to {stream_dir}")
    print(f"[producer] pausing {args.pause}s between writes\n")

    # Drop one small CSV per iteration. The stream picks each one up as a micro-batch.
    for i in range(1, args.iters + 1):
        # Pull a random handful of rows from the full dataset.
        sample = df.sample(n=args.rows)

        # Tag the filename with a microsecond timestamp so two files never collide.
        ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        out_path = stream_dir / f"stream_{i:03d}_{ts}.csv"
        # index=False keeps the pandas row numbers out of the file, which keeps
        # the stream schema clean.
        sample.to_csv(out_path, index=False)

        print(f"[producer] ({i:02d}/{args.iters}) wrote {args.rows} rows -> {out_path.name}")

        # Sleep between writes so Spark sees them as separate batches. Skip the
        # sleep after the very last file -- nothing to wait for.
        if i < args.iters:
            time.sleep(args.pause)

    print("\n[producer] done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
