from __future__ import annotations

import ipaddress
import json
import logging
import math
import threading
from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import IP2Location
from pymongo import MongoClient
from pymongo.read_preferences import SecondaryPreferred

from config.config import Settings


logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
IP2LOCATION_DATABASE = PROJECT_ROOT / "data" / "IP2LOCATION-LITE-DB5.BIN"
LOCATIONS_OUTPUT = PROJECT_ROOT / "data" / "locations.jsonl"
DEFAULT_WORKERS = 16
LOCATION_FIELDS = (
    "ip",
    "city_name",
    "region_name",
    "country_code",
    "country_name",
    "latitude",
    "longitude",
)

_worker_state = threading.local()


@dataclass
class LocationStats:
    mongo_unique: int = 0
    valid_unique: int = 0
    invalid: int = 0
    normalized_duplicates: int = 0
    written: int = 0
    lookup_errors: int = 0


def normalize_ip(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    try:
        return str(ipaddress.ip_address(value.strip()))
    except ValueError:
        return None


def unique_normalized_ips(values: Iterable[Any], stats: LocationStats) -> Iterator[str]:
    seen: set[str] = set()
    for value in values:
        stats.mongo_unique += 1
        ip = normalize_ip(value)
        if ip is None:
            stats.invalid += 1
            continue
        if ip in seen:
            stats.normalized_duplicates += 1
            continue
        seen.add(ip)
        stats.valid_unique += 1
        yield ip


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return None if text in {"", "-", "N/A"} else text


def _clean_coordinate(value: Any) -> float | None:
    try:
        coordinate = float(value)
    except (TypeError, ValueError):
        return None
    return coordinate if math.isfinite(coordinate) else None


def record_to_location(ip: str, record: Any) -> dict[str, Any]:
    country_code = _clean_text(record.country_short)
    if country_code is not None and (len(country_code) != 2 or not country_code.isalpha()):
        country_code = None

    if country_code is None:
        city_name = None
        region_name = None
        country_name = None
        latitude = None
        longitude = None
    else:
        city_name = _clean_text(record.city)
        region_name = _clean_text(record.region)
        country_name = _clean_text(record.country_long)
        latitude = _clean_coordinate(record.latitude)
        longitude = _clean_coordinate(record.longitude)

    return {
        "ip": ip,
        "city_name": city_name,
        "region_name": region_name,
        "country_code": country_code,
        "country_name": country_name,
        "latitude": latitude,
        "longitude": longitude,
    }


def _empty_location(ip: str) -> dict[str, Any]:
    return {field: ip if field == "ip" else None for field in LOCATION_FIELDS}


def _initialize_lookup_worker(database_path: str) -> None:
    _worker_state.database = IP2Location.IP2Location(database_path)


def _lookup_ip(ip: str) -> dict[str, Any]:
    database = _worker_state.database
    return record_to_location(ip, database.get_all(ip))


def _mongo_client_options(settings: Settings) -> dict[str, Any]:
    options: dict[str, Any] = {
        "read_preference": SecondaryPreferred(),
        "appname": "glamira-location-exporter",
    }
    if settings.mongo_username:
        options["username"] = settings.mongo_username
    if settings.mongo_password:
        options["password"] = settings.mongo_password
    if settings.mongo_username or settings.mongo_password:
        options["authSource"] = settings.mongo_auth_source
    return options


def _mongo_unique_ip_values(settings: Settings) -> Iterator[Any]:
    pipeline = [
        {"$sort": {"ip": 1}},
        {"$group": {"_id": "$ip"}},
    ]
    with MongoClient(settings.mongo_uri, **_mongo_client_options(settings)) as client:
        collection = client[settings.mongo_db][settings.mongo_collection]
        cursor = collection.aggregate(
            pipeline,
            allowDiskUse=True,
            hint="ip_1",
            batchSize=settings.mongo_batch_size,
        )
        for document in cursor:
            yield document.get("_id")


def write_locations(
    ips: Iterable[str],
    *,
    database_path: Path = IP2LOCATION_DATABASE,
    output_path: Path = LOCATIONS_OUTPUT,
    workers: int = DEFAULT_WORKERS,
    stats: LocationStats | None = None,
    progress: Callable[[LocationStats], None] | None = None,
    lookup: Callable[[str], dict[str, Any]] | None = None,
) -> LocationStats:
    if workers < 1:
        raise ValueError("workers must be greater than or equal to 1")
    if lookup is None and not database_path.is_file():
        raise FileNotFoundError(f"IP2Location database not found: {database_path}")

    stats = stats or LocationStats()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    pending: dict[Future[dict[str, Any]], str] = {}
    max_pending = workers * 4
    last_flushed = 0

    executor_kwargs: dict[str, Any] = {"max_workers": workers}
    if lookup is None:
        executor_kwargs.update(
            initializer=_initialize_lookup_worker,
            initargs=(str(database_path),),
        )
        lookup = _lookup_ip

    def write_completed(
        completed: set[Future[dict[str, Any]]],
        handle: Any,
    ) -> None:
        nonlocal last_flushed
        for future in completed:
            ip = pending.pop(future)
            try:
                location = future.result()
            except Exception as exc:
                stats.lookup_errors += 1
                logger.error("IP2Location lookup failed | ip=%s | error=%s", ip, exc)
                location = _empty_location(ip)
            handle.write(json.dumps(location, ensure_ascii=False, separators=(",", ":")))
            handle.write("\n")
            stats.written += 1
        if stats.written - last_flushed >= 1000:
            handle.flush()
            last_flushed = stats.written
            if progress:
                progress(stats)

    with output_path.open("w", encoding="utf-8", newline="\n") as handle:
        with ThreadPoolExecutor(**executor_kwargs) as executor:
            for ip in ips:
                future = executor.submit(lookup, ip)
                pending[future] = ip
                if len(pending) >= max_pending:
                    completed, _ = wait(pending, return_when=FIRST_COMPLETED)
                    write_completed(completed, handle)
            while pending:
                completed, _ = wait(pending, return_when=FIRST_COMPLETED)
                write_completed(completed, handle)
        handle.flush()
        if progress and stats.written != last_flushed:
            progress(stats)
    return stats


def export_locations(
    settings: Settings,
    *,
    workers: int = DEFAULT_WORKERS,
    progress: Callable[[LocationStats], None] | None = None,
) -> LocationStats:
    stats = LocationStats()
    ips = unique_normalized_ips(_mongo_unique_ip_values(settings), stats)
    return write_locations(ips, workers=workers, stats=stats, progress=progress)
