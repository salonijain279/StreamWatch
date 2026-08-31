"""Generate atomic text-file micro-batches for a file-based stream source."""

from __future__ import annotations

import argparse
from pathlib import Path
import time


def chunked(lines: list[str], size: int) -> list[list[str]]:
    if size <= 0:
        raise ValueError("batch size must be positive")
    return [lines[index : index + size] for index in range(0, len(lines), size)]


def write_batches(source: Path, output: Path, batch_size: int, delay: float) -> int:
    lines = [line.strip() for line in source.read_text().splitlines() if line.strip()]
    output.mkdir(parents=True, exist_ok=True)
    for index, batch in enumerate(chunked(lines, batch_size), start=1):
        temporary = output / f".batch_{index:04d}.tmp"
        destination = output / f"batch_{index:04d}.txt"
        temporary.write_text("\n".join(batch) + "\n")
        temporary.replace(destination)
        if delay:
            time.sleep(delay)
    return len(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--delay", type=float, default=0.0)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    count = write_batches(args.source, args.output, args.batch_size, args.delay)
    print(f"records_written={count}")


if __name__ == "__main__":
    main()

