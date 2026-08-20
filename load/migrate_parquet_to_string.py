from __future__ import annotations

import argparse
import logging
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
from google.api_core.exceptions import PreconditionFailed
from google.cloud import storage

from load.export_to_gcs import documents_to_table


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class GcsLocation:
    bucket: str
    prefix: str


@dataclass(frozen=True)
class MigrationResult:
    source: str
    destination: str
    status: str
    rows: int = 0
    error: str | None = None


def parse_gcs_uri(uri: str) -> GcsLocation:
    if not uri.startswith("gs://"):
        raise ValueError(f"GCS URI phải bắt đầu bằng gs://: {uri}")
    bucket, separator, prefix = uri[5:].partition("/")
    if not bucket:
        raise ValueError(f"GCS URI thiếu bucket: {uri}")
    return GcsLocation(bucket=bucket, prefix=prefix.strip("/") if separator else "")


def _object_name(prefix: str, relative_name: str) -> str:
    return f"{prefix}/{relative_name}" if prefix else relative_name


def _relative_name(object_name: str, source_prefix: str) -> str:
    if not source_prefix:
        return object_name
    prefix_with_slash = f"{source_prefix}/"
    if not object_name.startswith(prefix_with_slash):
        raise ValueError(f"Object nằm ngoài source prefix: {object_name}")
    return object_name[len(prefix_with_slash) :]


def convert_parquet_file(source: Path, destination: Path) -> int:
    source_table = pq.read_table(source)
    try:
        converted_table = documents_to_table(source_table.to_pylist())
    finally:
        source_table = None

    destination.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        converted_table,
        destination,
        compression="snappy",
        use_compliant_nested_type=True,
    )
    return converted_table.num_rows


def _migrate_blob(
    client: storage.Client,
    source: GcsLocation,
    destination: GcsLocation,
    source_name: str,
) -> MigrationResult:
    relative_name = _relative_name(source_name, source.prefix)
    destination_name = _object_name(destination.prefix, relative_name)
    source_uri = f"gs://{source.bucket}/{source_name}"
    destination_uri = f"gs://{destination.bucket}/{destination_name}"

    destination_blob = client.bucket(destination.bucket).blob(destination_name)
    if destination_blob.exists(client=client):
        return MigrationResult(source_uri, destination_uri, "skipped")

    source_blob = client.bucket(source.bucket).blob(source_name)
    try:
        with tempfile.TemporaryDirectory(prefix="parquet-string-migration-") as directory:
            input_path = Path(directory) / "input.parquet"
            output_path = Path(directory) / "output.parquet"
            source_blob.download_to_filename(input_path)
            rows = convert_parquet_file(input_path, output_path)

            metadata = dict(source_blob.metadata or {})
            metadata.update(
                {
                    "migration": "all-leaves-to-string",
                    "source_object": source_uri,
                    "row_count": str(rows),
                }
            )
            destination_blob.metadata = metadata
            try:
                destination_blob.upload_from_filename(
                    output_path,
                    content_type="application/octet-stream",
                    if_generation_match=0,
                )
            except PreconditionFailed:
                return MigrationResult(source_uri, destination_uri, "skipped")

        return MigrationResult(source_uri, destination_uri, "migrated", rows=rows)
    except Exception as exc:
        return MigrationResult(
            source_uri,
            destination_uri,
            "failed",
            error=f"{type(exc).__name__}: {exc}",
        )


def migrate_prefix(
    source_uri: str,
    destination_uri: str,
    *,
    workers: int = 8,
    max_files: int | None = None,
    storage_client: storage.Client | None = None,
) -> list[MigrationResult]:
    source = parse_gcs_uri(source_uri)
    destination = parse_gcs_uri(destination_uri)
    if source == destination:
        raise ValueError("Source và destination không được trùng nhau")
    if (
        source.bucket == destination.bucket
        and source.prefix
        and destination.prefix.startswith(f"{source.prefix}/")
    ):
        raise ValueError("Destination không được nằm bên trong source prefix")
    if workers < 1:
        raise ValueError("workers phải lớn hơn hoặc bằng 1")

    client = storage_client or storage.Client()
    listing_prefix = f"{source.prefix}/" if source.prefix else ""
    source_names = sorted(
        blob.name
        for blob in client.list_blobs(source.bucket, prefix=listing_prefix)
        if blob.name.endswith(".parquet")
    )
    if max_files is not None:
        source_names = source_names[:max_files]

    logger.info(
        "Bắt đầu migrate Parquet | source=%s | destination=%s | files=%d | workers=%d",
        source_uri,
        destination_uri,
        len(source_names),
        workers,
    )
    results: list[MigrationResult] = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(_migrate_blob, client, source, destination, name): name
            for name in source_names
        }
        for completed, future in enumerate(as_completed(futures), start=1):
            result = future.result()
            results.append(result)
            if result.status == "failed":
                logger.error(
                    "Migrate thất bại | source=%s | error=%s",
                    result.source,
                    result.error,
                )
            elif result.status == "migrated":
                logger.info(
                    "Đã migrate | file=%d/%d | destination=%s | rows=%d",
                    completed,
                    len(source_names),
                    result.destination,
                    result.rows,
                )
            elif completed % 100 == 0:
                logger.info("Đã kiểm tra %d/%d file", completed, len(source_names))

    return results


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Migrate Parquet trên GCS sang schema có toàn bộ leaf là STRING"
    )
    parser.add_argument("--source", default="gs://raw_glamira/mongodb_data")
    parser.add_argument("--destination", default="gs://raw_glamira/mongodb_data_string")
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument(
        "--max-files",
        type=int,
        help="Chỉ migrate N file đầu tiên; hữu ích khi chạy thử",
    )
    return parser


def main() -> None:
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure:
            reconfigure(encoding="utf-8", errors="replace")
    args = _parser().parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-7s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    results = migrate_prefix(
        args.source,
        args.destination,
        workers=args.workers,
        max_files=args.max_files,
    )
    migrated = sum(result.status == "migrated" for result in results)
    skipped = sum(result.status == "skipped" for result in results)
    failed = sum(result.status == "failed" for result in results)
    rows = sum(result.rows for result in results if result.status == "migrated")
    logger.info(
        "Migration hoàn tất | migrated=%d | skipped=%d | failed=%d | rows=%d",
        migrated,
        skipped,
        failed,
        rows,
    )
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
