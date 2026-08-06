from __future__ import annotations

import asyncio
import json
import logging
import random
from contextlib import AsyncExitStack
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

from curl_cffi.requests import AsyncSession
from curl_cffi.requests.exceptions import RequestException

from config.config import Settings
from .parsing import (
    extract_react_data_url,
    find_product_object,
    same_product_id,
)
from .state import StateStore


logger = logging.getLogger(__name__)


def _display_url(url: str, limit: int = 240) -> str:
    return url if len(url) <= limit else url[: limit - 3] + "..."


class HttpStatusError(RuntimeError):
    def __init__(self, status: int, url: str):
        self.status = status
        super().__init__(f"HTTP {status} khi mở {url}")


class RequestPacer:
    """Space request start times globally across all concurrent workers."""

    def __init__(self, delay_seconds: float, jitter_seconds: float):
        self.delay_seconds = delay_seconds
        self.jitter_seconds = jitter_seconds
        self._lock = asyncio.Lock()
        self._next_request_at = 0.0

    async def wait(self) -> None:
        async with self._lock:
            loop = asyncio.get_running_loop()
            wait_seconds = max(0.0, self._next_request_at - loop.time())
            if wait_seconds:
                logger.debug("Rate limit | chờ %.2fs trước request tiếp theo", wait_seconds)
                await asyncio.sleep(wait_seconds)
            interval = self.delay_seconds + random.uniform(0.0, self.jitter_seconds)
            self._next_request_at = loop.time() + interval


@dataclass(frozen=True)
class CrawlSuccess:
    product_id: str
    product: dict[str, Any]
    source_url: str
    react_data_url: str
    url_errors: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class CrawlFailure:
    product_id: str
    error: str
    url_errors: tuple[tuple[str, str], ...]


async def _get_bytes(
    session: AsyncSession,
    url: str,
    settings: Settings,
    *,
    accept: str,
    max_bytes: int,
    pacer: RequestPacer,
    user_agent: str,
    extra_headers: dict[str, str] | None = None,
) -> tuple[bytes, str, str | None]:
    last_error = "unknown error"
    for attempt in range(settings.retries):
        await pacer.wait()
        try:
            logger.debug(
                "HTTP request | attempt=%d/%d | url=%s",
                attempt + 1,
                settings.retries,
                _display_url(url),
            )
            headers = {
                "User-Agent": user_agent,
                "Accept": accept,
                "Accept-Language": "en-GB,en;q=0.9",
                "Cache-Control": "no-cache",
                **(extra_headers or {}),
            }
            response = await session.get(
                url,
                headers=headers,
                allow_redirects=True,
                timeout=settings.request_timeout_seconds,
            )
            if response.status_code < 200 or response.status_code >= 300:
                error = HttpStatusError(response.status_code, url)
                last_error = str(error)
                retryable = response.status_code in settings.retry_http_statuses
                if attempt + 1 < settings.retries:
                    if retryable:
                        retry_after = response.headers.get("Retry-After", "")
                        try:
                            delay = max(0.0, float(retry_after))
                        except ValueError:
                            delay = settings.retry_backoff_seconds * 2**attempt
                        logger.warning(
                            "HTTP %d, sẽ retry sau %.1fs | attempt=%d/%d | url=%s",
                            response.status_code,
                            delay,
                            attempt + 1,
                            settings.retries,
                            _display_url(url),
                        )
                        await asyncio.sleep(delay)
                        continue
                if not retryable:
                    logger.warning(
                        "HTTP %d không retry cùng URL | url=%s",
                        response.status_code,
                        _display_url(url),
                    )
                raise error
            body = response.content
            if len(body) > max_bytes:
                raise RuntimeError(f"Response vượt quá giới hạn {max_bytes} bytes")
            logger.debug(
                "curl-cffi thành công | status=%d | bytes=%d | final_url=%s",
                response.status_code,
                len(body),
                _display_url(str(response.url)),
            )
            return body, str(response.url), response.encoding
        except HttpStatusError:
            raise
        except (RequestException, asyncio.TimeoutError) as exc:
            last_error = str(exc)
            if attempt + 1 < settings.retries:
                delay = settings.retry_backoff_seconds * 2**attempt
                logger.warning(
                    "Request lỗi, sẽ retry sau %.1fs | attempt=%d/%d | error=%s | url=%s",
                    delay,
                    attempt + 1,
                    settings.retries,
                    exc,
                    _display_url(url),
                )
                await asyncio.sleep(delay)
    raise RuntimeError(last_error)


def _decode(body: bytes, charset: str | None) -> str:
    try:
        return body.decode(charset or "utf-8")
    except (LookupError, UnicodeDecodeError):
        return body.decode("utf-8", errors="replace")


async def crawl_one(
    session: AsyncSession,
    product_id: str,
    urls: list[str],
    settings: Settings,
    *,
    pacer: RequestPacer | None = None,
    user_agent: str | None = None,
    user_agent_number: int = 1,
) -> CrawlSuccess | CrawlFailure:
    if pacer is None:
        pacer = RequestPacer(0.0, 0.0)
    if user_agent is None:
        configured_agents = getattr(settings, "user_agents", ())
        user_agent = configured_agents[0] if configured_agents else "GlamiraProductCollector/1.0"
    url_errors: list[tuple[str, str]] = []
    fallback_url = settings.fallback_product_url_template.format(
        product_id=quote(product_id, safe="")
    )
    urls_to_try = list(dict.fromkeys([*urls, fallback_url]))

    logger.info(
        "[%s] Bắt đầu crawl | tracking_urls=%d | total_urls=%d | user_agent=%d",
        product_id,
        len(urls),
        len(urls_to_try),
        user_agent_number,
    )
    for index, source_url in enumerate(urls_to_try, start=1):
        url_type = "fallback" if source_url == fallback_url else "tracking"
        logger.info(
            "[%s] Thử URL %d/%d | type=%s | url=%s",
            product_id,
            index,
            len(urls_to_try),
            url_type,
            _display_url(source_url),
        )
        try:
            html_body, final_page_url, charset = await _get_bytes(
                session,
                source_url,
                settings,
                accept="text/html,application/xhtml+xml",
                max_bytes=10 * 1024 * 1024,
                pacer=pacer,
                user_agent=user_agent,
            )
            react_url = extract_react_data_url(_decode(html_body, charset), final_page_url)
            json_body, _, json_charset = await _get_bytes(
                session,
                react_url,
                settings,
                accept="application/json,text/plain;q=0.9,*/*;q=0.1",
                max_bytes=20 * 1024 * 1024,
                pacer=pacer,
                user_agent=user_agent,
                extra_headers={"Referer": final_page_url},
            )
            payload = json.loads(_decode(json_body, json_charset))
            product = find_product_object(payload)
            if not same_product_id(product_id, product.get("product_id")):
                raise ValueError(
                    f"product_id không khớp (MongoDB={product_id}, JSON={product.get('product_id')})"
                )
            if settings.product_fields is not None:
                product = {field: product.get(field) for field in settings.product_fields}
            logger.info(
                "[%s] Crawl thành công | type=%s | source=%s | react_data=%s",
                product_id,
                url_type,
                _display_url(final_page_url),
                _display_url(react_url),
            )
            return CrawlSuccess(
                product_id,
                product,
                final_page_url,
                react_url,
                tuple(url_errors),
            )
        except (RequestException, asyncio.TimeoutError, RuntimeError, ValueError, json.JSONDecodeError) as exc:
            url_errors.append((source_url, str(exc)))
            logger.warning(
                "[%s] URL thất bại | type=%s | error=%s | url=%s",
                product_id,
                url_type,
                exc,
                _display_url(source_url),
            )
    message = " | ".join(f"{url}: {error}" for url, error in url_errors)
    logger.error("[%s] Crawl thất bại sau %d URL", product_id, len(urls_to_try))
    return CrawlFailure(product_id, message, tuple(url_errors))


async def crawl_pending(
    settings: Settings,
    state: StateStore,
    *,
    retry_failed: bool = False,
) -> tuple[int, int]:
    reset_count = state.reset_interrupted()
    if reset_count:
        logger.warning("Đã đưa %d job bị gián đoạn về pending", reset_count)
    if retry_failed:
        requeued = state.requeue_failed()
        logger.info("Đã đưa %d job failed về pending để retry", requeued)

    pacer = RequestPacer(settings.request_delay_seconds, settings.request_jitter_seconds)
    successes = 0
    failures = 0
    logger.info(
        "Bắt đầu crawler curl-cffi | workers=%d | impersonate=%s | retries=%d | "
        "delay=%.1fs | jitter=%.1fs | user_agents=%d | timeout=%.1fs",
        settings.concurrency,
        settings.curl_impersonate,
        settings.retries,
        settings.request_delay_seconds,
        settings.request_jitter_seconds,
        len(settings.user_agents),
        settings.request_timeout_seconds,
    )

    async with AsyncExitStack() as stack:
        sessions: list[AsyncSession] = []
        for worker_index in range(settings.concurrency):
            user_agent = settings.user_agents[worker_index % len(settings.user_agents)]
            session = await stack.enter_async_context(
                AsyncSession(
                    max_clients=1,
                    timeout=settings.request_timeout_seconds,
                    impersonate=settings.curl_impersonate,
                    headers={"User-Agent": user_agent},
                )
            )
            sessions.append(session)

        async def worker_loop(worker_index: int) -> None:
            nonlocal successes, failures
            agent_index = worker_index % len(settings.user_agents)
            worker_number = worker_index + 1
            session = sessions[worker_index]

            while True:
                jobs = state.claim(1)
                if not jobs:
                    logger.info("Worker %d không còn job pending", worker_number)
                    return

                product_id, urls = jobs[0]
                logger.debug("Worker %d nhận product_id=%s", worker_number, product_id)
                result = await crawl_one(
                    session,
                    product_id,
                    urls,
                    settings,
                    pacer=pacer,
                    user_agent=settings.user_agents[agent_index],
                    user_agent_number=agent_index + 1,
                )

                if isinstance(result, CrawlSuccess):
                    state.save_success(
                        result.product_id,
                        result.product,
                        result.source_url,
                        result.react_data_url,
                        result.url_errors,
                    )
                    successes += 1
                else:
                    state.save_failure(result.product_id, result.error, result.url_errors)
                    failures += 1
                logger.info(
                    "Tiến độ crawl | worker=%d | thành công=%d | thất bại=%d | đã xử lý=%d",
                    worker_number,
                    successes,
                    failures,
                    successes + failures,
                )

        async with asyncio.TaskGroup() as workers:
            for worker_index in range(settings.concurrency):
                workers.create_task(
                    worker_loop(worker_index),
                    name=f"crawler-worker-{worker_index + 1}",
                )
    logger.info("Crawler kết thúc | thành công=%d | thất bại=%d", successes, failures)
    return successes, failures
