import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from google.api_core.exceptions import Conflict
from google.cloud import bigquery

from load.trigger_bigquery import (
    BIGQUERY_LOCATION,
    LOCATION_DESTINATION,
    MONGO_DESTINATION,
    PRODUCT_DESTINATION,
    PROJECT_ID,
    trigger_bigquery_load,
)


class TriggerBigQueryLoadTests(unittest.TestCase):
    def _run_event(self, name, generation="123"):
        client_patch = patch("load.trigger_bigquery.bigquery.Client")
        client_class = client_patch.start()
        self.addCleanup(client_patch.stop)
        load_job = MagicMock(job_id="job-123", output_rows=25)
        client_class.return_value.load_table_from_uri.return_value = load_job

        trigger_bigquery_load(
            {"bucket": "raw_glamira", "name": name, "generation": generation},
            SimpleNamespace(event_id="event-123"),
        )
        return client_class, load_job

    def _assert_load(self, name, destination, source_format):
        client_class, load_job = self._run_event(name)
        client_class.assert_called_once_with(project=PROJECT_ID)
        call = client_class.return_value.load_table_from_uri.call_args
        self.assertEqual(call.kwargs["source_uris"], f"gs://raw_glamira/{name}")
        self.assertEqual(call.kwargs["destination"], destination)
        self.assertEqual(call.kwargs["location"], BIGQUERY_LOCATION)
        self.assertTrue(call.kwargs["job_id"].startswith("gcs_load_"))
        config = call.kwargs["job_config"]
        self.assertEqual(config.source_format, source_format)
        self.assertEqual(
            config.write_disposition,
            bigquery.WriteDisposition.WRITE_APPEND,
        )
        load_job.result.assert_called_once_with()
        return config

    def test_routes_mongo_parquet(self):
        config = self._assert_load(
            "mongodb_data_string/12.parquet",
            MONGO_DESTINATION,
            bigquery.SourceFormat.PARQUET,
        )
        self.assertTrue(config.parquet_options.enable_list_inference)

    def test_routes_location_jsonl(self):
        self._assert_load(
            "location_data/locations-2026-08-20.jsonl",
            LOCATION_DESTINATION,
            bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )

    def test_routes_product_jsonl(self):
        self._assert_load(
            "product_data/products-2026-08-20.jsonl",
            PRODUCT_DESTINATION,
            bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )

    @patch("load.trigger_bigquery.bigquery.Client")
    def test_ignores_other_bucket(self, client_class):
        trigger_bigquery_load(
            {"bucket": "other", "name": "product_data/products.jsonl"},
            SimpleNamespace(event_id="event-123"),
        )
        client_class.assert_not_called()

    @patch("load.trigger_bigquery.bigquery.Client")
    def test_ignores_unknown_folder(self, client_class):
        trigger_bigquery_load(
            {"bucket": "raw_glamira", "name": "other/products.jsonl"},
            SimpleNamespace(event_id="event-123"),
        )
        client_class.assert_not_called()

    @patch("load.trigger_bigquery.bigquery.Client")
    def test_ignores_wrong_extension_and_checkpoint(self, client_class):
        context = SimpleNamespace(event_id="event-123")
        trigger_bigquery_load(
            {"bucket": "raw_glamira", "name": "product_data/products.parquet"},
            context,
        )
        trigger_bigquery_load(
            {"bucket": "raw_glamira", "name": "mongodb_data_string/_checkpoint.json"},
            context,
        )
        client_class.assert_not_called()

    @patch("load.trigger_bigquery.bigquery.Client")
    def test_rejects_invalid_event(self, client_class):
        with self.assertRaisesRegex(ValueError, "bucket.*name"):
            trigger_bigquery_load({}, SimpleNamespace(event_id="event-123"))
        client_class.assert_not_called()

    def test_uses_stable_job_id_for_same_object_generation(self):
        first_client, _ = self._run_event("product_data/products-1.jsonl", "100")
        second_client, _ = self._run_event("product_data/products-1.jsonl", "100")
        first_job_id = first_client.return_value.load_table_from_uri.call_args.kwargs[
            "job_id"
        ]
        second_job_id = second_client.return_value.load_table_from_uri.call_args.kwargs[
            "job_id"
        ]
        self.assertEqual(first_job_id, second_job_id)

    @patch("load.trigger_bigquery.bigquery.Client")
    def test_reuses_existing_job_when_event_is_redelivered(self, client_class):
        client = client_class.return_value
        existing_job = MagicMock(job_id="existing-job", output_rows=25)
        client.load_table_from_uri.side_effect = Conflict("job already exists")
        client.get_job.return_value = existing_job

        trigger_bigquery_load(
            {
                "bucket": "raw_glamira",
                "name": "product_data/products-1.jsonl",
                "generation": "100",
            },
            SimpleNamespace(event_id="event-123"),
        )

        submitted_job_id = client.load_table_from_uri.call_args.kwargs["job_id"]
        client.get_job.assert_called_once_with(
            submitted_job_id,
            project=PROJECT_ID,
            location=BIGQUERY_LOCATION,
        )
        existing_job.result.assert_called_once_with()

    @patch("load.trigger_bigquery.bigquery.Client")
    def test_raises_when_load_job_fails(self, client_class):
        load_job = MagicMock(job_id="job-123")
        load_job.result.side_effect = RuntimeError("load failed")
        client_class.return_value.load_table_from_uri.return_value = load_job

        with self.assertRaisesRegex(RuntimeError, "load failed"):
            trigger_bigquery_load(
                {
                    "bucket": "raw_glamira",
                    "name": "location_data/locations-1.jsonl",
                    "generation": "123",
                },
                SimpleNamespace(event_id="event-123"),
            )


if __name__ == "__main__":
    unittest.main()
