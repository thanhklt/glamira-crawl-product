import logging

from google.cloud import bigquery


logger = logging.getLogger(__name__)

PROJECT_ID = "glamira-project-502214"
DATASET_ID = "landing"
TABLE_ID = "raw_mongo"

SOURCE_URI = "gs://raw_glamira/mongodb_data_string/*.parquet"
DESTINATION = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"


def trigger_bigquery_load() -> None:
    logger.info(
        "Bắt đầu load Parquet vào BigQuery | source=%s | destination=%s",
        SOURCE_URI,
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
        source_uris=SOURCE_URI,
        destination=DESTINATION,
        job_config=job_config,
        location="asia-southeast1",
    )
    logger.info("Đã gửi BigQuery load job | job_id=%s", load_job.job_id)

    try:
        # Chờ cho đến khi BigQuery load job hoàn thành.
        load_job.result()
        table = client.get_table(DESTINATION)
    except Exception:
        logger.exception(
            "Load BigQuery thất bại | job_id=%s | destination=%s",
            load_job.job_id,
            DESTINATION,
        )
        raise

    logger.info(
        "Load BigQuery hoàn tất | job_id=%s | destination=%s | total_rows=%d",
        load_job.job_id,
        DESTINATION,
        table.num_rows,
    )

    for field in table.schema:
        logger.debug(
            "BigQuery schema | name=%s | type=%s | mode=%s",
            field.name,
            field.field_type,
            field.mode,
        )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-7s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    trigger_bigquery_load()
