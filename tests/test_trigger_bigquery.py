import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from google.cloud import bigquery

from load.trigger_bigquery import (
    BIGQUERY_LOCATION,
    DESTINATION,
    PROJECT_ID,
    trigger_bigquery_load,
)


class TriggerBigQueryLoadTests(unittest.TestCase):
    @patch("load.trigger_bigquery.bigquery.Client")
    def test_loads_the_new_parquet_object(self, client_class):
        client = client_class.return_value
        load_job = MagicMock(job_id="job-123", output_rows=25)
        client.load_table_from_uri.return_value = load_job
        event = {
            "bucket": "raw_glamira",
            "name": "mongodb_data_string/12.parquet",
        }

        trigger_bigquery_load(event, SimpleNamespace(event_id="event-123"))

        client_class.assert_called_once_with(project=PROJECT_ID)
        client.load_table_from_uri.assert_called_once()
        call = client.load_table_from_uri.call_args
        self.assertEqual(
            call.kwargs["source_uris"],
            "gs://raw_glamira/mongodb_data_string/12.parquet",
        )
        self.assertEqual(call.kwargs["destination"], DESTINATION)
        self.assertEqual(call.kwargs["location"], BIGQUERY_LOCATION)
        config = call.kwargs["job_config"]
        self.assertEqual(config.source_format, bigquery.SourceFormat.PARQUET)
        self.assertEqual(
            config.write_disposition,
            bigquery.WriteDisposition.WRITE_APPEND,
        )
        self.assertTrue(config.parquet_options.enable_list_inference)
        load_job.result.assert_called_once_with()

    @patch("load.trigger_bigquery.bigquery.Client")
    def test_ignores_objects_outside_the_source_prefix(self, client_class):
        trigger_bigquery_load(
            {"bucket": "raw_glamira", "name": "mongodb_data/12.parquet"},
            SimpleNamespace(event_id="event-123"),
        )

        client_class.assert_not_called()

    @patch("load.trigger_bigquery.bigquery.Client")
    def test_ignores_non_parquet_objects(self, client_class):
        trigger_bigquery_load(
            {
                "bucket": "raw_glamira",
                "name": "mongodb_data_string/_checkpoint.json",
            },
            SimpleNamespace(event_id="event-123"),
        )

        client_class.assert_not_called()

    @patch("load.trigger_bigquery.bigquery.Client")
    def test_raises_when_load_job_fails(self, client_class):
        load_job = MagicMock(job_id="job-123")
        load_job.result.side_effect = RuntimeError("load failed")
        client_class.return_value.load_table_from_uri.return_value = load_job

        with self.assertRaisesRegex(RuntimeError, "load failed"):
            trigger_bigquery_load(
                {
                    "bucket": "raw_glamira",
                    "name": "mongodb_data_string/12.parquet",
                },
                SimpleNamespace(event_id="event-123"),
            )

    @patch("load.trigger_bigquery.bigquery.Client")
    def test_rejects_an_invalid_event(self, client_class):
        with self.assertRaisesRegex(ValueError, "bucket.*name"):
            trigger_bigquery_load({}, SimpleNamespace(event_id="event-123"))

        client_class.assert_not_called()


if __name__ == "__main__":
    unittest.main()
