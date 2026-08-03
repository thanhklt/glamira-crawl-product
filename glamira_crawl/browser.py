from __future__ import annotations

import asyncio
import html
import json
import logging
from dataclasses import dataclass
from collections.abc import Awaitable, Callable
from typing import Any
from urllib.parse import urljoin, urlparse

from playwright.async_api import Browser, BrowserContext, Page, Playwright, async_playwright

from .config import Settings
from .parsing import ReactDataUrlNotFound, extract_react_data_url


logger = logging.getLogger(__name__)


class BrowserBlockedError(RuntimeError):
    pass


@dataclass(frozen=True)
class BrowserProductResponse:
    payload: Any
    source_url: str
    react_data_url: str


class PlaywrightFetcher:
    """Render product pages without attempting to bypass challenges or CAPTCHA."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self._playwright: Playwright | None = None
        self._profiles: dict[int, tuple[Browser, BrowserContext]] = {}
        self._profile_lock = asyncio.Lock()
        self._semaphore = asyncio.Semaphore(settings.playwright_concurrency)

    async def start(self) -> None:
        if self._playwright is None:
            self._playwright = await async_playwright().start()
            logger.info(
                "Playwright sẵn sàng | concurrency=%d | profiles=%d",
                self.settings.playwright_concurrency,
                len(self.settings.user_agents),
            )

    async def close(self) -> None:
        for browser, context in self._profiles.values():
            await context.close()
            await browser.close()
        self._profiles.clear()
        if self._playwright is not None:
            await self._playwright.stop()
            self._playwright = None

    async def _get_profile(self, profile_index: int) -> BrowserContext:
        if profile_index in self._profiles:
            return self._profiles[profile_index][1]
        async with self._profile_lock:
            if profile_index in self._profiles:
                return self._profiles[profile_index][1]
            if self._playwright is None:
                await self.start()
            assert self._playwright is not None
            channel = self.settings.playwright_browser_channels[profile_index]
            browser = await self._playwright.chromium.launch(
                channel=None if channel == "chromium" else channel,
                headless=True,
            )
            context = await browser.new_context(
                user_agent=self.settings.user_agents[profile_index],
                locale="en-GB",
                viewport={"width": 1366, "height": 768},
                extra_http_headers={"Accept-Language": "en-GB,en;q=0.9"},
            )
            self._profiles[profile_index] = (browser, context)
            logger.info("Đã mở Playwright browser profile %d | channel=%s", profile_index + 1, channel)
            return context

    async def fetch_product(
        self,
        url: str,
        profile_index: int,
        before_request: Callable[[], Awaitable[None]] | None = None,
    ) -> BrowserProductResponse:
        async with self._semaphore:
            context = await self._get_profile(profile_index)
            page = await context.new_page()
            try:
                return await self._fetch_with_page(
                    page,
                    context,
                    url,
                    before_request=before_request,
                )
            finally:
                await page.close()

    async def check_profiles(self) -> list[dict[str, str | int | bool]]:
        results: list[dict[str, str | int | bool]] = []
        for profile_index, expected_user_agent in enumerate(self.settings.user_agents):
            context = await self._get_profile(profile_index)
            page = await context.new_page()
            try:
                await page.goto("about:blank")
                actual_user_agent = await page.evaluate("() => navigator.userAgent")
                results.append(
                    {
                        "profile": profile_index + 1,
                        "channel": self.settings.playwright_browser_channels[profile_index],
                        "matches_config": actual_user_agent == expected_user_agent,
                        "user_agent": actual_user_agent,
                    }
                )
            finally:
                await page.close()
        return results

    async def _fetch_with_page(
        self,
        page: Page,
        context: BrowserContext,
        url: str,
        *,
        before_request: Callable[[], Awaitable[None]] | None,
    ) -> BrowserProductResponse:
        timeout_ms = self.settings.playwright_navigation_timeout_seconds * 1000
        if before_request is not None:
            await before_request()
        response = await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
        if response is None:
            raise RuntimeError(f"Playwright không nhận được response khi mở {url}")
        final_url = page.url
        if response.status < 200 or response.status >= 300:
            raise RuntimeError(f"Playwright HTTP {response.status} khi mở {url}")
        if self.settings.playwright_wait_after_load_seconds:
            await page.wait_for_timeout(self.settings.playwright_wait_after_load_seconds * 1000)

        title = (await page.title()).strip()
        page_html = await page.content()
        lowered = f"{title}\n{page_html[:4000]}".lower()
        blocked_markers = ("access denied", "captcha", "verify you are human", "challenge-platform")
        if any(marker in lowered for marker in blocked_markers):
            raise BrowserBlockedError(
                f"Playwright nhận trang blocked/challenge: title={title!r}, url={final_url}"
            )

        react_url = await page.evaluate(
            """() => typeof window.react_data_url === 'string'
                ? window.react_data_url
                : null"""
        )
        if not react_url:
            try:
                react_url = extract_react_data_url(page_html, final_url)
            except ReactDataUrlNotFound as exc:
                raise ReactDataUrlNotFound(
                    f"Playwright không tìm thấy react_data_url: title={title!r}, url={final_url}"
                ) from exc
        react_url = urljoin(final_url, str(react_url))
        parsed = urlparse(react_url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError(f"Playwright nhận react_data_url không hợp lệ: {react_url!r}")

        if before_request is not None:
            await before_request()
        json_response = await context.request.get(
            react_url,
            headers={"Referer": final_url, "Accept": "application/json,text/plain,*/*"},
            timeout=timeout_ms,
        )
        if not json_response.ok:
            raise RuntimeError(
                f"Playwright JSON HTTP {json_response.status} khi mở {react_url}"
            )
        body = await json_response.body()
        if len(body) > 20 * 1024 * 1024:
            raise RuntimeError("Playwright JSON response vượt quá 20 MB")
        try:
            payload = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            prefix = html.escape(body[:200].decode("utf-8", errors="replace"))
            raise ValueError(f"Playwright JSON không hợp lệ, prefix={prefix!r}") from exc
        return BrowserProductResponse(payload, final_url, react_url)
