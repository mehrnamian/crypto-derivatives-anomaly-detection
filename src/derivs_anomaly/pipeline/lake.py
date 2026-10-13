from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from fastavro import reader as avro_reader

from derivs_anomaly.domain.envelope import EventEnvelope


def discover_bronze_files(bronze_dir: Path) -> list[Path]:
    if not bronze_dir.exists():
        return []
    return sorted(
        path
        for path in bronze_dir.rglob("*")
        if (
            path.is_file()
            and not path.name.startswith(".")
            and path.suffix.lower() in {".jsonl", ".json", ".avro"}
        )
    )


def iter_bronze_records(path: Path) -> Iterator[tuple[int, dict[str, Any]]]:
    """Read local JSONL/JSON or Event Hubs Capture Avro envelopes."""
    if path.suffix.lower() == ".avro":
        with path.open("rb") as handle:
            for index, captured in enumerate(avro_reader(handle), start=1):
                body = captured.get("Body")
                if not isinstance(body, (bytes, bytearray)):
                    raise ValueError(f"Capture record {index} has no byte Body")
                yield index, json.loads(bytes(body).decode("utf-8"))
        return

    with path.open("r", encoding="utf-8") as handle:
        for index, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            yield index, json.loads(line)


def safe_stem(path: Path) -> str:
    stem = "".join(
        character if character.isalnum() or character in "-_." else "_"
        for character in path.stem
    )
    return stem[:120] or "bronze"


def atomic_write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=path.parent, delete=False
    ) as handle:
        temp_path = Path(handle.name)
        for row in rows:
            handle.write(
                json.dumps(row, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
            )
            handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temp_path, path)


def atomic_write_parquet(path: Path, frame: Any) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "wb", dir=path.parent, suffix=".parquet", delete=False
    ) as handle:
        temp_path = Path(handle.name)
    try:
        pq.write_table(
            pa.Table.from_pandas(frame, preserve_index=False),
            temp_path,
            compression="zstd",
        )
        with temp_path.open("r+b") as handle:
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, path)
    finally:
        temp_path.unlink(missing_ok=True)


def envelope_from_mapping(data: dict[str, Any]) -> EventEnvelope:
    return EventEnvelope.from_dict(data)
