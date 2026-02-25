from __future__ import annotations

from pathlib import Path


def resolve_output_path(output_arg: str | None, source_name: str) -> Path | None:
    if output_arg is None or output_arg == "":
        return Path("data") / "outputs" / f"{source_name}.jsonl"
    if output_arg == "-":
        return None
    return Path(output_arg)
