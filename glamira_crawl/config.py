from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    mongo_uri: str
    mongo_username: str | None
    mongo_password: str | None
    mongo_auth_source: str
    mongo_db: str
    mongo_collection: str
    mongo_batch_size: int
    max_urls_per_product: int
    checkpoint_every: int
    concurrency: int
    per_host_limit: int
    request_delay_seconds: float
    request_jitter_seconds: float
    request_timeout_seconds: float
    retries: int
    retry_backoff_seconds: float
    retry_http_statuses: tuple[int, ...]
    user_agents: tuple[str, ...]
    fallback_product_url_template: str
    playwright_enabled: bool
    playwright_primary: bool
    playwright_concurrency: int
    playwright_navigation_timeout_seconds: float
    playwright_wait_after_load_seconds: float
    playwright_fallback_http_statuses: tuple[int, ...]
    playwright_fallback_when_react_data_url_missing: bool
    playwright_browser_channels: tuple[str, ...]
    state_db: Path
    output: Path
    failed_urls_output: Path
    product_fields: tuple[str, ...] | None
    log_level: str


def _section(raw: dict[str, Any], name: str) -> dict[str, Any]:
    value = raw.get(name, {})
    if not isinstance(value, dict):
        raise ValueError(f"Cấu hình '{name}' phải là một object")
    return value


def load_settings(path: str | Path) -> Settings:
    config_path = Path(path).resolve()
    # Shell/process variables take precedence over values from the local .env.
    load_dotenv(config_path.parent / ".env", override=False)
    with config_path.open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle) or {}
    if not isinstance(raw, dict):
        raise ValueError("File cấu hình phải chứa một YAML object")

    mongo = _section(raw, "mongodb")
    discovery = _section(raw, "discovery")
    crawler = _section(raw, "crawler")
    storage = _section(raw, "storage")
    logging_config = _section(raw, "logging")
    playwright_config = _section(raw, "playwright")
    base_dir = config_path.parent

    fields = raw.get("product_fields")
    if fields is not None and not isinstance(fields, list):
        raise ValueError("product_fields phải là một list hoặc null")

    def local_path(value: str) -> Path:
        candidate = Path(value)
        return candidate if candidate.is_absolute() else base_dir / candidate

    username = os.environ.get("MONGODB_USERNAME")
    password = os.environ.get("MONGODB_PASSWORD")
    if bool(username) != bool(password):
        raise ValueError("Phải khai báo cả MONGODB_USERNAME và MONGODB_PASSWORD")
    fallback_template = str(
        crawler.get(
            "fallback_product_url_template",
            "https://www.glamira.co.uk/catalog/product/view/id/{product_id}",
        )
    )
    if "{product_id}" not in fallback_template:
        raise ValueError("crawler.fallback_product_url_template phải chứa {product_id}")
    raw_user_agents = crawler.get("user_agents")
    if raw_user_agents is None:
        raw_user_agents = [crawler.get("user_agent", "GlamiraProductCollector/1.0")]
    if not isinstance(raw_user_agents, list):
        raise ValueError("crawler.user_agents phải là một list")
    user_agents = tuple(str(value).strip() for value in raw_user_agents if str(value).strip())
    if not user_agents:
        raise ValueError("crawler.user_agents phải có ít nhất một giá trị")
    browser_ua_pattern = re.compile(r"^Mozilla/5\.0 .*(?:Chrome|Firefox|Edg)/\d+(?:\.\d+)+")
    invalid_agents = [value for value in user_agents if not browser_ua_pattern.search(value)]
    if invalid_agents:
        raise ValueError(
            "Mỗi crawler.user_agents phải có định dạng User-Agent trình duyệt "
            "Mozilla/5.0 với Chrome, Firefox hoặc Edge và version cụ thể"
        )
    raw_retry_statuses = crawler.get("retry_http_statuses", [429, 500, 502, 503, 504])
    if not isinstance(raw_retry_statuses, list):
        raise ValueError("crawler.retry_http_statuses phải là một list")
    retry_http_statuses = tuple(int(value) for value in raw_retry_statuses)
    if any(value < 400 or value > 599 for value in retry_http_statuses):
        raise ValueError("crawler.retry_http_statuses chỉ chấp nhận HTTP status từ 400 đến 599")
    raw_browser_channels = playwright_config.get("browser_channels", ["chrome"] * len(user_agents))
    if not isinstance(raw_browser_channels, list):
        raise ValueError("playwright.browser_channels phải là một list")
    browser_channels = tuple(str(value).strip() for value in raw_browser_channels)
    if len(browser_channels) != len(user_agents):
        raise ValueError(
            "playwright.browser_channels phải có cùng số phần tử với crawler.user_agents"
        )
    allowed_channels = {"chrome", "msedge", "chromium"}
    if any(value not in allowed_channels for value in browser_channels):
        raise ValueError("playwright.browser_channels chỉ hỗ trợ chrome, msedge hoặc chromium")
    raw_fallback_statuses = playwright_config.get("fallback_http_statuses", [403])
    if not isinstance(raw_fallback_statuses, list):
        raise ValueError("playwright.fallback_http_statuses phải là một list")
    playwright_fallback_statuses = tuple(int(value) for value in raw_fallback_statuses)
    if any(value < 400 or value > 599 for value in playwright_fallback_statuses):
        raise ValueError(
            "playwright.fallback_http_statuses chỉ chấp nhận HTTP status từ 400 đến 599"
        )
    log_level = str(logging_config.get("level", "INFO")).upper()
    if log_level not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
        raise ValueError("logging.level phải là DEBUG, INFO, WARNING, ERROR hoặc CRITICAL")

    return Settings(
        mongo_uri=os.environ.get("MONGODB_URI", str(mongo.get("uri", "mongodb://localhost:27017"))),
        mongo_username=username,
        mongo_password=password,
        mongo_auth_source=os.environ.get("MONGODB_AUTH_SOURCE", str(mongo.get("auth_source", "admin"))),
        mongo_db=str(mongo.get("db", "countly")),
        mongo_collection=str(mongo.get("collection", "summary")),
        mongo_batch_size=max(1, int(mongo.get("batch_size", 2000))),
        max_urls_per_product=max(1, int(discovery.get("max_urls_per_product", 3))),
        checkpoint_every=max(1, int(discovery.get("checkpoint_every", 2000))),
        concurrency=max(1, int(crawler.get("concurrency", 3))),
        per_host_limit=max(1, int(crawler.get("per_host_limit", 1))),
        request_delay_seconds=max(0.0, float(crawler.get("request_delay_seconds", 2.0))),
        request_jitter_seconds=max(0.0, float(crawler.get("request_jitter_seconds", 1.0))),
        request_timeout_seconds=max(1.0, float(crawler.get("request_timeout_seconds", 30))),
        retries=max(1, int(crawler.get("retries", 3))),
        retry_backoff_seconds=max(0.0, float(crawler.get("retry_backoff_seconds", 1.0))),
        retry_http_statuses=retry_http_statuses,
        user_agents=user_agents,
        fallback_product_url_template=fallback_template,
        playwright_enabled=bool(playwright_config.get("enabled", True)),
        playwright_primary=bool(playwright_config.get("primary", True)),
        playwright_concurrency=max(1, int(playwright_config.get("concurrency", 1))),
        playwright_navigation_timeout_seconds=max(
            1.0, float(playwright_config.get("navigation_timeout_seconds", 45))
        ),
        playwright_wait_after_load_seconds=max(
            0.0, float(playwright_config.get("wait_after_load_seconds", 1.5))
        ),
        playwright_fallback_http_statuses=playwright_fallback_statuses,
        playwright_fallback_when_react_data_url_missing=bool(
            playwright_config.get("fallback_when_react_data_url_missing", True)
        ),
        playwright_browser_channels=browser_channels,
        state_db=local_path(str(storage.get("state_db", "data/crawl-state.sqlite3"))),
        output=local_path(str(storage.get("output", "data/products.jsonl"))),
        failed_urls_output=local_path(
            str(storage.get("failed_urls_output", "data/failed-urls.jsonl"))
        ),
        product_fields=None if fields is None else tuple(str(field) for field in fields),
        log_level=log_level,
    )
