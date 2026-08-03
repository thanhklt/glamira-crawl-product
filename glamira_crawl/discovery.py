from __future__ import annotations

from collections.abc import Callable
from typing import Any
from urllib.parse import urlparse

from bson import ObjectId
from pymongo import ASCENDING, MongoClient
from pymongo.read_preferences import SecondaryPreferred

from .config import Settings
from .state import StateStore


STANDARD_EVENTS = (
    "view_product_detail",
    "select_product_option",
    "select_product_option_quality",
    "add_to_cart_action",
    "product_detail_recommendation_visible",
    "product_detail_recommendation_noticed",
)
RECOMMEND_CLICK_EVENT = "product_view_all_recommend_clicked"
ALL_EVENTS = (*STANDARD_EVENTS, RECOMMEND_CLICK_EVENT)


def _clean_id(value: Any) -> str | None:
    if value is None or isinstance(value, bool):
        return None
    text = str(value).strip()
    return text or None


def _clean_url(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    url = value.strip()
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return None
    return url


def document_to_candidate(document: dict[str, Any]) -> tuple[str, str | None, str] | None:
    event = document.get("collection")
    if event == RECOMMEND_CLICK_EVENT:
        product_id = _clean_id(document.get("viewing_product_id"))
        url = _clean_url(document.get("referrer_url"))
    else:
        product_id = _clean_id(document.get("product_id")) or _clean_id(document.get("viewing_product_id"))
        url = _clean_url(document.get("current_url"))
    if not product_id:
        return None
    return product_id, url, str(event)


def discover(
    settings: Settings,
    state: StateStore,
    progress: Callable[[int, int], None] | None = None,
) -> tuple[int, int]:
    checkpoint_key = f"mongo:{settings.mongo_db}.{settings.mongo_collection}:last_id"
    last_id = state.get_metadata(checkpoint_key)
    query: dict[str, Any] = {"collection": {"$in": list(ALL_EVENTS)}}
    if last_id:
        try:
            query["_id"] = {"$gt": ObjectId(last_id)}
        except Exception as exc:
            raise ValueError(f"Checkpoint MongoDB không hợp lệ: {last_id}") from exc

    client_options: dict[str, Any] = {
        "read_preference": SecondaryPreferred(),
        "appname": "glamira-product-collector",
    }
    if settings.mongo_username:
        client_options["username"] = settings.mongo_username
    if settings.mongo_password:
        client_options["password"] = settings.mongo_password
    if settings.mongo_username or settings.mongo_password:
        client_options["authSource"] = settings.mongo_auth_source

    scanned = 0
    added = 0
    buffer: list[tuple[str, str | None, str]] = []
    buffer_last_id: ObjectId | None = None
    projection = {
        "_id": 1,
        "collection": 1,
        "product_id": 1,
        "viewing_product_id": 1,
        "current_url": 1,
        "referrer_url": 1,
    }

    with MongoClient(settings.mongo_uri, **client_options) as client:
        collection = client[settings.mongo_db][settings.mongo_collection]
        cursor = (
            collection.find(query, projection=projection, no_cursor_timeout=True)
            .sort("_id", ASCENDING)
            .batch_size(settings.mongo_batch_size)
        )
        try:
            for document in cursor:
                scanned += 1
                buffer_last_id = document["_id"]
                candidate = document_to_candidate(document)
                if candidate:
                    buffer.append(candidate)
                if scanned % settings.checkpoint_every == 0:
                    added += state.add_candidates(
                        buffer,
                        max_urls=settings.max_urls_per_product,
                        checkpoint_key=checkpoint_key,
                        checkpoint=str(buffer_last_id),
                    )
                    buffer.clear()
                    if progress:
                        progress(scanned, added)
            if buffer_last_id is not None:
                added += state.add_candidates(
                    buffer,
                    max_urls=settings.max_urls_per_product,
                    checkpoint_key=checkpoint_key,
                    checkpoint=str(buffer_last_id),
                )
        finally:
            cursor.close()
    if progress:
        progress(scanned, added)
    return scanned, added
