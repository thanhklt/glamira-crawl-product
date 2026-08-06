import asyncio
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
    crawl_pending,
)


class _FakeResponse:
    def __init__(self, status, body=b"ok", headers=None):
        self.status_code = status
        self.headers = headers or {}
        self.url = "https://example.com/product"
        self.encoding = "utf-8"
        self.content = body


class _FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = 0

    async def get(self, *_args, **_kwargs):
        self.calls += 1
        return self.responses.pop(0)


class _FakeAsyncSession:
    def __init__(self, **_kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, _exc_type, _exc, _traceback):
        return False


class _FakeState:
    def __init__(self, product_ids):
        self.pending = list(product_ids)
        self.claim_limits = []
        self.saved = []

    def reset_interrupted(self):
        return 0

    def requeue_failed(self):
        return 0

    def claim(self, limit):
        self.claim_limits.append(limit)
        if not self.pending:
            return []
        return [(self.pending.pop(0), [])]

    def save_success(
        self,
        product_id,
        _product,
        _source_url,
        _react_data_url,
        _url_errors,
    ):
        self.saved.append(product_id)

    def save_failure(self, product_id, _error, _url_errors):
        self.saved.append(product_id)


class CrawlerTests(unittest.IsolatedAsyncioTestCase):
    async def test_worker_claims_next_job_without_waiting_for_other_workers(self):
        settings = SimpleNamespace(
            concurrency=2,
            request_delay_seconds=0,
            request_jitter_seconds=0,
            request_timeout_seconds=30,
            curl_impersonate="chrome",
            retries=1,
            user_agents=("agent-1", "agent-2"),
        )
        state = _FakeState(["fast-1", "slow", "fast-2"])
        finished = []

        async def fake_crawl_one(_session, product_id, _urls, _settings, **_kwargs):
            await asyncio.sleep(0.05 if product_id == "slow" else 0)
            finished.append(product_id)
            return CrawlSuccess(
                product_id,
                {"product_id": product_id},
                f"https://example.com/{product_id}",
                f"https://example.com/{product_id}.json",
                (),
            )

        with (
            patch("glamira_crawl.crawler.AsyncSession", _FakeAsyncSession),
            patch("glamira_crawl.crawler.crawl_one", fake_crawl_one),
        ):
            successes, failures = await crawl_pending(settings, state)

        self.assertEqual((successes, failures), (3, 0))
        self.assertEqual(finished, ["fast-1", "fast-2", "slow"])
        self.assertEqual(state.saved, ["fast-1", "fast-2", "slow"])
        self.assertTrue(all(limit == 1 for limit in state.claim_limits))

    async def test_does_not_retry_403_on_same_url(self):
        session = _FakeSession([_FakeResponse(403)])
        settings = SimpleNamespace(
            retries=3,
            retry_http_statuses=(429, 500, 502, 503, 504),
            retry_backoff_seconds=0,
            request_timeout_seconds=30,
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
            request_timeout_seconds=30,
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
