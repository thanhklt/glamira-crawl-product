import unittest

from glamira_crawl.discovery import document_to_candidate


class DiscoveryTests(unittest.TestCase):
    def test_standard_event_uses_product_id_and_current_url(self):
        self.assertEqual(
            document_to_candidate(
                {
                    "collection": "view_product_detail",
                    "product_id": "110474",
                    "viewing_product_id": "999",
                    "current_url": "https://www.glamira.fr/product.html",
                }
            ),
            ("110474", "https://www.glamira.fr/product.html", "view_product_detail"),
        )

    def test_standard_event_falls_back_to_viewing_product_id(self):
        self.assertEqual(
            document_to_candidate(
                {
                    "collection": "select_product_option",
                    "viewing_product_id": 85796,
                    "current_url": "https://www.glamira.co.uk/product.html",
                }
            ),
            ("85796", "https://www.glamira.co.uk/product.html", "select_product_option"),
        )

    def test_recommend_click_uses_referrer(self):
        self.assertEqual(
            document_to_candidate(
                {
                    "collection": "product_view_all_recommend_clicked",
                    "viewing_product_id": "85796",
                    "referrer_url": "https://www.glamira.co.uk/product.html",
                }
            ),
            (
                "85796",
                "https://www.glamira.co.uk/product.html",
                "product_view_all_recommend_clicked",
            ),
        )

    def test_keeps_id_when_url_is_missing_or_invalid(self):
        self.assertEqual(
            document_to_candidate(
                {"collection": "view_product_detail", "product_id": "1", "current_url": "javascript:x"}
            ),
            ("1", None, "view_product_detail"),
        )


if __name__ == "__main__":
    unittest.main()
