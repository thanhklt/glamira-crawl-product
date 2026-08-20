"""Route newly created GCS objects to their BigQuery landing tables."""

from __future__ import annotations

import hashlib
import logging

import functions_framework
from cloudevents.http import CloudEvent
from google.api_core.exceptions import Conflict
from google.cloud import bigquery


logger = logging.getLogger(__name__)

PROJECT_ID = "glamira-project-502214"
DATASET_ID = "landing"
BIGQUERY_LOCATION = "asia-southeast1"
SOURCE_BUCKET = "raw_glamira"

MONGO_PREFIX = "mongodb_data_string/"
LOCATION_PREFIX = "location_data/"
PRODUCT_PREFIX = "product_data/"

MONGO_DESTINATION = f"{PROJECT_ID}.{DATASET_ID}.raw_mongo"
LOCATION_DESTINATION = f"{PROJECT_ID}.{DATASET_ID}.raw_location"
PRODUCT_DESTINATION = f"{PROJECT_ID}.{DATASET_ID}.raw_product"

# Backward compatibility for callers that used the old Mongo-only constant.
DESTINATION = MONGO_DESTINATION


def _job_id(bucket_name: str, object_name: str, generation: str | None) -> str:
    """Return a stable job ID so a redelivered GCS event is not appended twice."""
    identity = f"{bucket_name}\n{object_name}\n{generation or ''}"
    digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()
    return f"gcs_load_{digest}"


def _load_object(
    *,
    client: bigquery.Client,
    source_uri: str,
    destination: str,
    source_format: str,
    job_id: str,
    event_id: str | None,
    enable_list_inference: bool = False,
) -> None:
    job_config = bigquery.LoadJobConfig(
        source_format=source_format,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    if enable_list_inference:
        parquet_options = bigquery.ParquetOptions()
        parquet_options.enable_list_inference = True
        job_config.parquet_options = parquet_options

    logger.info(
        "Bắt đầu BigQuery load job | event_id=%s | source=%s | destination=%s",
        event_id,
        source_uri,
        destination,
    )
    try:
        load_job = client.load_table_from_uri(
            source_uris=source_uri,
            destination=destination,
            job_config=job_config,
            location=BIGQUERY_LOCATION,
            job_id=job_id,
        )
    except Conflict:
        # The same GCS generation was already submitted. Reuse that job rather
        # than submitting another append job when Cloud Functions retries.
        load_job = client.get_job(
            job_id,
            project=PROJECT_ID,
            location=BIGQUERY_LOCATION,
        )
        logger.info(
            "Sử dụng lại BigQuery job đã tồn tại | event_id=%s | job_id=%s",
            event_id,
            job_id,
        )
    logger.info(
        "Đã gửi BigQuery load job | event_id=%s | job_id=%s",
        event_id,
        load_job.job_id,
    )

    try:
        load_job.result()
    except Exception:
        logger.exception(
            "BigQuery load job thất bại | event_id=%s | job_id=%s | source=%s | destination=%s",
            event_id,
            load_job.job_id,
            source_uri,
            destination,
        )
        raise

    logger.info(
        "BigQuery load job hoàn tất | event_id=%s | job_id=%s | source=%s | destination=%s | output_rows=%s",
        event_id,
        load_job.job_id,
        source_uri,
        destination,
        load_job.output_rows,
    )


def load_mongo_parquet(
    client: bigquery.Client,
    source_uri: str,
    job_id: str,
    event_id: str | None = None,
) -> None:
    """Append one MongoDB Parquet object to ``landing.raw_mongo``."""
    _load_object(
        client=client,
        source_uri=source_uri,
        destination=MONGO_DESTINATION,
        source_format=bigquery.SourceFormat.PARQUET,
        job_id=job_id,
        event_id=event_id,
        enable_list_inference=True,
    )


def load_location_jsonl(
    client: bigquery.Client,
    source_uri: str,
    job_id: str,
    event_id: str | None = None,
) -> None:
    """Append one location JSONL object to ``landing.raw_location``."""
    _load_object(
        client=client,
        source_uri=source_uri,
        destination=LOCATION_DESTINATION,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        job_id=job_id,
        event_id=event_id,
    )


def load_product_jsonl(
    client: bigquery.Client,
    source_uri: str,
    job_id: str,
    event_id: str | None = None,
) -> None:
    """Append one product JSONL object to ``landing.raw_product``."""
    _load_object(
        client=client,
        source_uri=source_uri,
        destination=PRODUCT_DESTINATION,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        job_id=job_id,
        event_id=event_id,
    )


@functions_framework.cloud_event
def trigger_bigquery_load(cloud_event: CloudEvent) -> None:
    """Route a Gen 2 GCS finalize CloudEvent to the matching loader."""
    event = cloud_event.data
    bucket_name = event.get("bucket")
    object_name = event.get("name")
    generation = event.get("generation")
    event_id = cloud_event.get("id")

    # Valid check
    if not bucket_name or not object_name:
        logger.error(
            "Sự kiện GCS không hợp lệ | event_id=%s | bucket=%s | object=%s",
            event_id,
            bucket_name,
            object_name,
        )
        raise ValueError("Sự kiện GCS phải có trường 'bucket' và 'name'")

    route = None
    lower_name = object_name.lower()
    if object_name.startswith(MONGO_PREFIX) and lower_name.endswith(".parquet"):
        route = load_mongo_parquet
    elif object_name.startswith(LOCATION_PREFIX) and lower_name.endswith(".jsonl"):
        route = load_location_jsonl
    elif object_name.startswith(PRODUCT_PREFIX) and lower_name.endswith(".jsonl"):
        route = load_product_jsonl

    if bucket_name != SOURCE_BUCKET or route is None:
        logger.info(
            "Bỏ qua object không thuộc nguồn dữ liệu | event_id=%s | gs://%s/%s",
            event_id,
            bucket_name,
            object_name,
        )
        return

    client = bigquery.Client(project=PROJECT_ID)
    route(
        client,
        f"gs://{bucket_name}/{object_name}",
        _job_id(bucket_name, object_name, str(generation) if generation is not None else None),
        event_id,
    )
