"""Cloud Function triggered when a Parquet object is created in GCS."""

from __future__ import annotations

import logging
from typing import Any

from google.cloud import bigquery


logger = logging.getLogger(__name__)

PROJECT_ID = "glamira-project-502214"
DATASET_ID = "landing"
TABLE_ID = "raw_mongo"
BIGQUERY_LOCATION = "asia-southeast1"

SOURCE_BUCKET = "raw_glamira"
SOURCE_PREFIX = "mongodb_data_string/"
DESTINATION = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"


def trigger_bigquery_load(event: dict[str, Any], context: Any) -> None:
    """Load a newly created GCS Parquet object into BigQuery.

    This entry point uses the background-event signature required by Google
    Cloud Functions Gen 1.
    """
    bucket_name = event.get("bucket")
    object_name = event.get("name")
    event_id = getattr(context, "event_id", None)

    if not bucket_name or not object_name:
        logger.error(
            "Sự kiện GCS không hợp lệ | event_id=%s | bucket=%s | object=%s",
            event_id,
            bucket_name,
            object_name,
        )
        raise ValueError("Sự kiện GCS phải có trường 'bucket' và 'name'")

    # Valid data source
    if (
        bucket_name != SOURCE_BUCKET
        or not object_name.startswith(SOURCE_PREFIX)
        or not object_name.lower().endswith(".parquet")
    ):
        logger.info(
            "Bỏ qua object không thuộc nguồn Parquet | event_id=%s | gs://%s/%s",
            event_id,
            bucket_name,
            object_name,
        )
        return

    source_uri = f"gs://{bucket_name}/{object_name}"
    logger.info(
        "Bắt đầu BigQuery load job | event_id=%s | source=%s | destination=%s",
        event_id,
        source_uri,
        DESTINATION,
    )

    client = bigquery.Client(project=PROJECT_ID)
    parquet_options = bigquery.ParquetOptions()
    parquet_options.enable_list_inference = True
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        parquet_options=parquet_options,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    load_job = client.load_table_from_uri(
        source_uris=source_uri,
        destination=DESTINATION,
        job_config=job_config,
        location=BIGQUERY_LOCATION,
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
            DESTINATION,
        )
        raise

    logger.info(
        "BigQuery load job hoàn tất | event_id=%s | job_id=%s | source=%s | destination=%s | output_rows=%s",
        event_id,
        load_job.job_id,
        source_uri,
        DESTINATION,
        load_job.output_rows,
    )
