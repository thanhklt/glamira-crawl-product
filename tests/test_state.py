import json
import tempfile
import unittest
from pathlib import Path

from glamira_crawl.state import StateStore


class StateStoreTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.state = StateStore(self.root / "state.sqlite3")

    def tearDown(self):
        self.state.close()
        self.temporary.cleanup()

    def test_deduplicates_products_and_limits_urls(self):
        candidates = [
            ("85796", "https://example.com/a", "view_product_detail"),
            ("85796", "https://example.com/b", "add_to_cart_action"),
            ("85796", "https://example.com/c", "select_product_option"),
        ]
        inserted = self.state.add_candidates(
            candidates, max_urls=2, checkpoint_key="checkpoint", checkpoint="abc"
        )
        self.assertEqual(inserted, 1)
        self.assertEqual(self.state.get_metadata("checkpoint"), "abc")
        jobs = self.state.claim(10)
        self.assertEqual(jobs, [("85796", ["https://example.com/a", "https://example.com/b"])])

    def test_success_is_exported_once(self):
        self.state.add_candidates(
            [("85796", "https://example.com/a", "view_product_detail")],
            max_urls=3,
            checkpoint_key="checkpoint",
            checkpoint="abc",
        )
        self.state.claim(1)
        self.state.save_success(
            "85796",
            {"product_id": 85796, "name": "Women's Earring Louisa"},
            "https://example.com/a",
            "https://example.com/data.json",
        )
        output = self.root / "products.jsonl"
        self.assertEqual(self.state.export_jsonl(output), 1)
        item = json.loads(output.read_text(encoding="utf-8"))
        self.assertEqual(item["product_id"], 85796)
        self.assertEqual(item["_crawl"]["requested_product_id"], "85796")

    def test_records_failed_url_even_when_fallback_recovers_product(self):
        self.state.add_candidates(
            [("85796", "https://example.com/dead", "view_product_detail")],
            max_urls=3,
            checkpoint_key="checkpoint",
            checkpoint="abc",
        )
        self.state.claim(1)
        fallback = "https://www.glamira.co.uk/catalog/product/view/id/85796"
        self.state.save_success(
            "85796",
            {"product_id": 85796},
            fallback,
            "https://www.glamira.co.uk/react-data.json",
            [("https://example.com/dead", "HTTP 404")],
        )
        output = self.root / "failed-urls.jsonl"
        self.assertEqual(self.state.export_failed_urls(output), 1)
        item = json.loads(output.read_text(encoding="utf-8"))
        self.assertEqual(item["failed_url"], "https://example.com/dead")
        self.assertEqual(item["recovered_via"], fallback)

    def test_product_without_candidate_url_can_still_be_claimed(self):
        self.state.add_candidates(
            [("85796", None, "view_product_detail")],
            max_urls=3,
            checkpoint_key="checkpoint",
            checkpoint="abc",
        )
        self.assertEqual(self.state.claim(1), [("85796", [])])


if __name__ == "__main__":
    unittest.main()
