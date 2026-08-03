from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys

from .config import Settings, load_settings
from .browser import PlaywrightFetcher
from .crawler import crawl_pending
from .discovery import discover
from .state import StateStore


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Thu thập thông tin sản phẩm Glamira từ MongoDB")
    parser.add_argument("--config", default="config.yml", help="Đường dẫn file YAML")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("discover", help="Quét MongoDB và tạo hàng đợi product_id/URL")
    crawl = commands.add_parser("crawl", help="Crawl các sản phẩm pending")
    crawl.add_argument("--retry-failed", action="store_true", help="Thử lại các sản phẩm failed một lần")
    export = commands.add_parser("export", help="Xuất kết quả trong SQLite ra JSONL")
    export.add_argument("--no-metadata", action="store_true", help="Không thêm object _crawl")
    run = commands.add_parser("run", help="Chạy discover, crawl, rồi export")
    run.add_argument("--retry-failed", action="store_true")
    commands.add_parser("stats", help="Xem trạng thái hàng đợi")
    commands.add_parser("browser-check", help="Kiểm tra Chrome/Edge và User-Agent Playwright")
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


async def _check_browser_profiles(settings: Settings) -> list[dict[str, str | int | bool]]:
    fetcher = PlaywrightFetcher(settings)
    try:
        return await fetcher.check_profiles()
    finally:
        await fetcher.close()


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
        elif args.command == "browser-check":
            profiles = asyncio.run(_check_browser_profiles(settings))
            print(json.dumps(profiles, ensure_ascii=False, indent=2))
        elif args.command == "run":
            _run_discover(settings, state)
            _run_crawl(settings, state, args.retry_failed)
            _run_export(settings, state)
    finally:
        state.close()


if __name__ == "__main__":
    main()
