import unittest

from glamira_crawl.parsing import extract_react_data_url, find_product_object, same_product_id


class ParsingTests(unittest.TestCase):
    def test_extracts_unicode_escaped_url(self):
        page = (
            "<script>var react_data_url = "
            "'https\\u003A\\u002F\\u002Fwww.glamira.co.uk\\u002Fproductcustomizer\\u002Freactdata';"
            "</script>"
        )
        self.assertEqual(
            extract_react_data_url(page, "https://www.glamira.co.uk/product.html"),
            "https://www.glamira.co.uk/productcustomizer/reactdata",
        )

    def test_resolves_relative_url(self):
        page = '<script>window.react_data_url="/api/react-data?id=10";</script>'
        self.assertEqual(
            extract_react_data_url(page, "https://example.com/product/10"),
            "https://example.com/api/react-data?id=10",
        )

    def test_finds_wrapped_product(self):
        product = find_product_object({"data": {"product": {"product_id": 85796, "sku": "G100735"}}})
        self.assertEqual(product["product_id"], 85796)

    def test_normalizes_integer_like_id(self):
        self.assertTrue(same_product_id("85796", 85796))
        self.assertTrue(same_product_id("85796", "85796.0"))


if __name__ == "__main__":
    unittest.main()
