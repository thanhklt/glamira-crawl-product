import json
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from glamira_crawl.crawler import (
    CrawlSuccess,
    HttpStatusError,
    RequestPacer,
    _get_bytes,
    crawl_one,
)
from glamira_crawl.browser import BrowserProductResponse


class _FakeContent:
    def __init__(self, body=b"ok"):
        self.body = body

    async def read(self, _limit):
        return self.body


class _FakeResponse:
    def __init__(self, status, body=b"ok", headers=None):
        self.status = status
        self.headers = headers or {}
        self.url = "https://example.com/product"
        self.charset = "utf-8"
        self.content = _FakeContent(body)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None


class _FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = 0

    def get(self, *_args, **_kwargs):
        self.calls += 1
        return self.responses.pop(0)


class _FakePlaywrightFetcher:
    def __init__(self, response):
        self.response = response
        self.calls = []

    async def fetch_product(self, url, profile_index, before_request=None):
        self.calls.append((url, profile_index))
        if before_request is not None:
            await before_request()
        return self.response


class CrawlerTests(unittest.IsolatedAsyncioTestCase):
    async def test_uses_playwright_as_primary_without_calling_http_client(self):
        source_url = "https://example.com/product"
        settings = SimpleNamespace(
            fallback_product_url_template=(
                "https://www.glamira.co.uk/catalog/product/view/id/{product_id}"
            ),
            product_fields=("product_id", "name"),
            playwright_primary=True,
        )
        browser = _FakePlaywrightFetcher(
            BrowserProductResponse(
                {"product_id": 85796, "name": "Louisa"},
                source_url,
                "https://example.com/react-data.json",
            )
        )
        mocked_get = AsyncMock()
        with patch("glamira_crawl.crawler._get_bytes", mocked_get):
            result = await crawl_one(
                object(),
                "85796",
                [source_url],
                settings,
                playwright_fetcher=browser,
                browser_profile_index=0,
            )

        self.assertIsInstance(result, CrawlSuccess)
        self.assertEqual(result.product, {"product_id": 85796, "name": "Louisa"})
        mocked_get.assert_not_awaited()

    async def test_uses_playwright_fallback_for_403(self):
        source_url = "https://example.com/product"
        settings = SimpleNamespace(
            fallback_product_url_template=(
                "https://www.glamira.co.uk/catalog/product/view/id/{product_id}"
            ),
            product_fields=("product_id", "name"),
            playwright_fallback_http_statuses=(403,),
            playwright_fallback_when_react_data_url_missing=True,
        )
        browser = _FakePlaywrightFetcher(
            BrowserProductResponse(
                {"product_id": 85796, "name": "Louisa"},
                source_url,
                "https://example.com/react-data.json",
            )
        )
        mocked_get = AsyncMock(side_effect=HttpStatusError(403, source_url))
        with patch("glamira_crawl.crawler._get_bytes", mocked_get):
            result = await crawl_one(
                object(),
                "85796",
                [source_url],
                settings,
                playwright_fetcher=browser,
                browser_profile_index=1,
            )

        self.assertIsInstance(result, CrawlSuccess)
        self.assertEqual(result.product, {"product_id": 85796, "name": "Louisa"})
        self.assertEqual(browser.calls, [(source_url, 1)])
        self.assertIn("Playwright đã phục hồi", result.url_errors[0][1])

    async def test_does_not_retry_403_on_same_url(self):
        session = _FakeSession([_FakeResponse(403)])
        settings = SimpleNamespace(
            retries=3,
            retry_http_statuses=(429, 500, 502, 503, 504),
            retry_backoff_seconds=0,
        )
        with self.assertRaises(HttpStatusError):
            await _get_bytes(
                session,
                "https://example.com/product",
                settings,
                accept="text/html",
                max_bytes=1024,
                pacer=RequestPacer(0, 0),
                user_agent="test-agent",
            )
        self.assertEqual(session.calls, 1)

    async def test_retries_temporary_500_then_succeeds(self):
        session = _FakeSession([_FakeResponse(500), _FakeResponse(200, b"product")])
        settings = SimpleNamespace(
            retries=3,
            retry_http_statuses=(429, 500, 502, 503, 504),
            retry_backoff_seconds=0,
        )
        body, _, _ = await _get_bytes(
            session,
            "https://example.com/product",
            settings,
            accept="text/html",
            max_bytes=1024,
            pacer=RequestPacer(0, 0),
            user_agent="test-agent",
        )
        self.assertEqual(body, b"product")
        self.assertEqual(session.calls, 2)

    async def test_tries_id_fallback_after_candidate_url_fails(self):
        settings = SimpleNamespace(
            fallback_product_url_template=(
                "https://www.glamira.co.uk/catalog/product/view/id/{product_id}"
            ),
            product_fields=("product_id", "name"),
        )
        fallback = "https://www.glamira.co.uk/catalog/product/view/id/85796"
        html = b"<script>var react_data_url='/react-data.json';</script>"
        product_json = json.dumps({"product_id": 85796, "name": "Louisa"}).encode()

        mocked_get = AsyncMock(
            side_effect=[
                RuntimeError("HTTP 404"),
                (html, fallback, "utf-8"),
                (product_json, "https://www.glamira.co.uk/react-data.json", "utf-8"),
            ]
        )
        with patch("glamira_crawl.crawler._get_bytes", mocked_get):
            result = await crawl_one(
                object(),
                "85796",
                ["https://example.com/dead"],
                settings,
            )

        self.assertIsInstance(result, CrawlSuccess)
        self.assertEqual(result.source_url, fallback)
        self.assertEqual(result.product, {"product_id": 85796, "name": "Louisa"})
        self.assertEqual(result.url_errors, (("https://example.com/dead", "HTTP 404"),))


if __name__ == "__main__":
    unittest.main()
