from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys

from config.config import Settings, load_settings
from .crawler import crawl_pending
from .discovery import discover
from .locations import DEFAULT_WORKERS, LocationStats, export_locations
from .state import StateStore
from load.export_to_gcs import export_to_gcs


def _positive_int(value: str) -> int:
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("phải lớn hơn hoặc bằng 1")
    return number


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Thu thập thông tin sản phẩm Glamira từ MongoDB")
    parser.add_argument("--config", default="config/config.yml", help="Đường dẫn file YAML")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("discover", help="Quét MongoDB và tạo hàng đợi product_id/URL")
    crawl = commands.add_parser("crawl", help="Crawl các sản phẩm pending")
    crawl.add_argument("--retry-failed", action="store_true", help="Thử lại các sản phẩm failed một lần")
    export = commands.add_parser("export", help="Xuất kết quả trong SQLite ra JSONL")
    export.add_argument("--no-metadata", action="store_true", help="Không thêm object _crawl")
    run = commands.add_parser("run", help="Chạy discover, crawl, rồi export")
    run.add_argument("--retry-failed", action="store_true")
    locations = commands.add_parser("locations", help="Xuất location của các IP unique ra JSONL")
    locations.add_argument(
        "--workers",
        type=_positive_int,
        default=DEFAULT_WORKERS,
        help="Số worker tra cứu IP2Location database local",
    )
    load = commands.add_parser("load", help="Xuất MongoDB thành Parquet và upload lên GCS")
    load.add_argument(
        "--documents-per-file",
        type=_positive_int,
        default=None,
        help="Số document tối đa trong mỗi file (mặc định lấy từ config)",
    )
    commands.add_parser("stats", help="Xem trạng thái hàng đợi")
    return parser


def _progress(scanned: int, added: int) -> None:
    print(f"MongoDB: đã quét={scanned:,}, product_id mới={added:,}", end="\r", flush=True)


def _run_discover(settings: Settings, state: StateStore) -> None:
    scanned, added = discover(settings, state, _progress)
    print(f"\nDiscover hoàn tất: đã quét={scanned:,}, product_id mới={added:,}")


def _run_crawl(settings: Settings, state: StateStore, retry_failed: bool) -> None:
    try:
        successes, failures = asyncio.run(crawl_pending(settings, state, retry_failed=retry_failed))
        print(f"Crawl hoàn tất: thành công={successes:,}, thất bại={failures:,}")
    finally:
        count = state.export_failed_urls(settings.failed_urls_output)
        print(f"Đã xuất {count:,} URL lỗi ra {settings.failed_urls_output}")


def _run_export(settings: Settings, state: StateStore, include_metadata: bool = True) -> None:
    count = state.export_jsonl(settings.output, include_metadata=include_metadata)
    print(f"Đã xuất {count:,} sản phẩm ra {settings.output}")
    failed_count = state.export_failed_urls(settings.failed_urls_output)
    print(f"Đã xuất {failed_count:,} URL lỗi ra {settings.failed_urls_output}")


def _location_progress(stats: LocationStats) -> None:
    print(
        f"Locations: Mongo unique={stats.mongo_unique:,}, hợp lệ={stats.valid_unique:,}, "
        f"đã ghi={stats.written:,}, lỗi={stats.lookup_errors:,}",
        end="\r",
        flush=True,
    )


def _run_locations(settings: Settings, workers: int) -> None:
    stats = export_locations(settings, workers=workers, progress=_location_progress)
    print(
        f"\nLocations hoàn tất: đã ghi={stats.written:,}, IP không hợp lệ={stats.invalid:,}, "
        f"trùng sau chuẩn hóa={stats.normalized_duplicates:,}, lỗi lookup={stats.lookup_errors:,}"
    )


def _run_load(settings: Settings, documents_per_file: int | None) -> None:
    result = export_to_gcs(settings, documents_per_file=documents_per_file)
    print(
        f"Load hoàn tất: files={result.files_uploaded:,}, "
        f"documents={result.documents_uploaded:,}, "
        f"file tiếp theo={result.checkpoint.next_file_number:,}"
    )


def main() -> None:
    # Windows may otherwise use cp1252 and fail while printing Vietnamese help text.
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure:
            reconfigure(encoding="utf-8", errors="replace")
    args = _parser().parse_args()
    settings = load_settings(args.config)
    logging.basicConfig(
        level=getattr(logging, settings.log_level),
        format="%(asctime)s | %(levelname)-7s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
        force=True,
    )
    if args.command == "locations":
        _run_locations(settings, args.workers)
        return
    if args.command == "load":
        _run_load(settings, args.documents_per_file)
        return

    state = StateStore(settings.state_db)
    try:
        if args.command == "discover":
            _run_discover(settings, state)
        elif args.command == "crawl":
            _run_crawl(settings, state, args.retry_failed)
        elif args.command == "export":
            _run_export(settings, state, include_metadata=not args.no_metadata)
        elif args.command == "stats":
            print(json.dumps(state.stats(), ensure_ascii=False, indent=2))
        elif args.command == "run":
            _run_discover(settings, state)
            _run_crawl(settings, state, args.retry_failed)
            _run_export(settings, state)
    finally:
        state.close()


if __name__ == "__main__":
    main()
