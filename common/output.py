from __future__ import annotations

import json
import os
import sys
from collections.abc import Iterable
from pathlib import Path
from typing import Any


def write_jsonl(rows: Iterable[dict[str, Any]], output_path: Path | None) -> None:
    """Write rows as JSONL. When output_path is None, write to stdout."""

    if output_path is None:
        _write_jsonl_stream(rows, sys.stdout)
        return

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="\n") as handle:
        _write_jsonl_stream(rows, handle)


def _write_jsonl_stream(rows: Iterable[dict[str, Any]], handle) -> None:
    try:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False))
            handle.write("\n")
    except BrokenPipeError:
        # Prevent unraisable BrokenPipeError during interpreter shutdown when piping to `head`.
        if handle is sys.stdout:
            try:
                sys.stdout = open(os.devnull, "w", encoding="utf-8")
            except OSError:
                pass
        return
