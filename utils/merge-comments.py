"""
Merge comment text files from the data folder into one output file.

This script skips the first post block marked with [1] and removes
standalone index markers like:
    [1]
    [10]

Usage:
    python utils/merge-comments.py
    python utils/merge-comments.py --output data/all_comments.txt
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path


INDEX_LINE_PATTERN = re.compile(r"^\[(\d+)\]$")
DEFAULT_OUTPUT_NAME = "merged_comments.txt"


def parse_args() -> argparse.Namespace:
    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent
    data_dir = project_root / "data"

    parser = argparse.ArgumentParser(
        description="Merge all text files in the data folder and remove index lines like [10]."
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=data_dir,
        help="Directory containing the source text files.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=data_dir / DEFAULT_OUTPUT_NAME,
        help="File path for the merged output.",
    )
    return parser.parse_args()


def clean_text(raw_text: str) -> str:
    cleaned_lines: list[str] = []
    previous_blank = False
    skip_current_block = False

    for line in raw_text.splitlines():
        stripped = line.strip()
        index_match = INDEX_LINE_PATTERN.fullmatch(stripped)
        if index_match:
            skip_current_block = index_match.group(1) == "1"
            continue

        if skip_current_block:
            continue

        if not stripped:
            if previous_blank:
                continue
            cleaned_lines.append("")
            previous_blank = True
            continue

        cleaned_lines.append(line.rstrip())
        previous_blank = False

    return "\n".join(cleaned_lines).strip()


def collect_input_files(input_dir: Path, output_path: Path) -> list[Path]:
    output_resolved = output_path.resolve()
    files = []
    for path in sorted(input_dir.glob("*.txt")):
        if path.resolve() == output_resolved:
            continue
        files.append(path)
    return files


def main() -> int:
    args = parse_args()
    input_dir = args.input_dir.resolve()
    output_path = args.output.resolve()

    if not input_dir.exists():
        raise SystemExit(f"Input directory does not exist: {input_dir}")

    input_files = collect_input_files(input_dir, output_path)
    if not input_files:
        raise SystemExit(f"No .txt files found in: {input_dir}")

    merged_parts = []
    for path in input_files:
        cleaned = clean_text(path.read_text(encoding="utf-8"))
        if cleaned:
            merged_parts.append(cleaned)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n\n".join(merged_parts).strip() + "\n", encoding="utf-8")

    print(f"Merged {len(input_files)} files into {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
