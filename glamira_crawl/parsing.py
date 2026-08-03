from __future__ import annotations

import html
import re
from typing import Any
from urllib.parse import urljoin, urlparse


_REACT_DATA_URL = re.compile(
    r"\breact_data_url\s*=\s*(?P<quote>['\"])(?P<value>(?:\\.|(?!\1).)*?)(?P=quote)",
    re.IGNORECASE | re.DOTALL,
)
_UNICODE_ESCAPE = re.compile(r"\\u([0-9a-fA-F]{4})")


class ReactDataUrlNotFound(ValueError):
    pass


def _decode_javascript_string(value: str) -> str:
    value = _UNICODE_ESCAPE.sub(lambda match: chr(int(match.group(1), 16)), value)
    replacements = {
        r"\/": "/",
        r"\\": "\\",
        r"\'": "'",
        r'\"': '"',
        r"\n": "\n",
        r"\r": "\r",
        r"\t": "\t",
    }
    for escaped, decoded in replacements.items():
        value = value.replace(escaped, decoded)
    return html.unescape(value)


def extract_react_data_url(page_html: str, page_url: str) -> str:
    match = _REACT_DATA_URL.search(page_html)
    if not match:
        raise ReactDataUrlNotFound("Không tìm thấy biến react_data_url trong HTML")
    url = urljoin(page_url, _decode_javascript_string(match.group("value")))
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError(f"react_data_url không hợp lệ: {url!r}")
    return url


def find_product_object(payload: Any) -> dict[str, Any]:
    """Locate the product object even when the endpoint wraps it in another object."""
    queue: list[Any] = [payload]
    fallback: dict[str, Any] | None = None
    visited = 0
    while queue and visited < 20_000:
        current = queue.pop(0)
        visited += 1
        if isinstance(current, dict):
            if "product_id" in current:
                if "sku" in current or "name" in current:
                    return current
                fallback = fallback or current
            queue.extend(current.values())
        elif isinstance(current, list):
            queue.extend(current)
    if fallback is not None:
        return fallback
    raise ValueError("JSON không chứa object sản phẩm có product_id")


def same_product_id(expected: str, actual: Any) -> bool:
    def normalize(value: Any) -> str:
        text = str(value).strip()
        if text.endswith(".0") and text[:-2].isdigit():
            return text[:-2]
        return text

    return normalize(expected) == normalize(actual)
