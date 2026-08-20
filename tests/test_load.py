import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
from bson import ObjectId
from google.api_core.exceptions import NotFound

from load.export_to_gcs import (
    batched,
    documents_to_table,
    export_to_gcs,
    upload_jsonl_exports,
    write_parquet,
)
from load.migrate_parquet_to_string import convert_parquet_file, parse_gcs_uri


class _FakeBlob:
    def __init__(self, bucket, name):
        self.bucket = bucket
        self.name = name
        self.metadata = None
        self.generation = None

    def download_as_text(self):
        if self.name not in self.bucket.objects:
            raise NotFound("missing")
        stored = self.bucket.objects[self.name]
        self.generation = stored["generation"]
        self.metadata = stored["metadata"]
        return stored["content"].decode()

    def upload_from_string(self, payload, **_kwargs):
        generation = self.bucket.next_generation
        self.bucket.next_generation += 1
        self.generation = generation
        self.bucket.objects[self.name] = {
            "content": payload.encode(),
            "metadata": self.metadata,
            "generation": generation,
        }

    def upload_from_filename(self, filename, **_kwargs):
        generation = self.bucket.next_generation
        self.bucket.next_generation += 1
        self.generation = generation
        self.bucket.objects[self.name] = {
            "content": Path(filename).read_bytes(),
            "metadata": dict(self.metadata or {}),
            "generation": generation,
        }


class _FakeBucket:
    def __init__(self, name):
        self.name = name
        self.objects = {}
        self.next_generation = 1

    def blob(self, name):
        return _FakeBlob(self, name)


class _FakeStorageClient:
    def __init__(self):
        self.buckets = {}

    def bucket(self, name):
        return self.buckets.setdefault(name, _FakeBucket(name))


class LoadTests(unittest.TestCase):
    def test_parses_gcs_uri(self):
        location = parse_gcs_uri("gs://raw_glamira/mongodb_data/")

        self.assertEqual(location.bucket, "raw_glamira")
        self.assertEqual(location.prefix, "mongodb_data")

    def test_migrates_existing_parquet_leaves_to_string(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "source.parquet"
            destination = Path(directory) / "destination.parquet"
            pq.write_table(
                pa.Table.from_pylist(
                    [
                        {
                            "is_paypal": True,
                            "amount": 10,
                            "metadata": {"score": 1.5, "empty": None},
                            "items": [{"quantity": 2}],
                        }
                    ]
                ),
                source,
            )

            rows = convert_parquet_file(source, destination)
            migrated = pq.read_table(destination)

            self.assertEqual(rows, 1)
            self.assertEqual(
                migrated.to_pylist(),
                [
                    {
                        "is_paypal": "true",
                        "amount": "10",
                        "metadata": {"empty": None, "score": "1.5"},
                        "items": [{"quantity": "2"}],
                    }
                ],
            )
            self.assertTrue(
                pa.types.is_string(
                    migrated.schema.field("metadata").type.field("empty").type
                )
            )

    def test_batches_keep_short_final_file(self):
        result = list(batched(({"_id": number} for number in range(5)), 2))
        self.assertEqual([len(batch) for batch in result], [2, 2, 1])

    def test_preserves_nested_array_as_list_of_struct(self):
        table = documents_to_table(
            [
                {
                    "_id": ObjectId("5e931f9633eacf36f47aa4c4"),
                    "created_at": datetime(2020, 4, 12, tzinfo=timezone.utc),
                    "options": [{"alloy": "white-585", "size": 42}],
                },
                {
                    "_id": ObjectId("5e931f9633eacf36f47aa4c5"),
                    "options": [{"alloy": "yellow-585", "size": None}],
                },
            ]
        )

        options_type = table.schema.field("options").type
        self.assertTrue(pa.types.is_list(options_type))
        self.assertTrue(pa.types.is_struct(options_type.value_type))
        self.assertEqual(table.column("_id")[0].as_py(), "5e931f9633eacf36f47aa4c4")
        self.assertEqual(table.column("created_at")[0].as_py(), "2020-04-12T00:00:00+00:00")
        self.assertEqual(table.column("options")[0].as_py()[0]["size"], "42")

    def test_promotes_scalar_to_list_when_mongodb_field_changes_type(self):
        table = documents_to_table(
            [
                {"_id": ObjectId(), "options": [{"alloy": "white-585"}]},
                {"_id": ObjectId(), "options": {"alloy": "yellow-585"}},
                {"_id": ObjectId(), "options": "legacy-value"},
                {"_id": ObjectId(), "options": None},
            ]
        )

        options_type = table.schema.field("options").type
        self.assertTrue(pa.types.is_list(options_type))
        self.assertTrue(pa.types.is_struct(options_type.value_type))
        self.assertEqual(table.column("options")[1].as_py()[0]["alloy"], "yellow-585")
        self.assertEqual(table.column("options")[2].as_py()[0]["_value"], "legacy-value")

    def test_harmonizes_nested_scalar_types(self):
        table = documents_to_table(
            [
                {"_id": ObjectId(), "items": [{"value": 10}]},
                {"_id": ObjectId(), "items": [{"value": "unknown"}]},
            ]
        )

        value_type = table.schema.field("items").type.value_type.field("value").type
        self.assertTrue(pa.types.is_string(value_type))

    def test_is_paypal_has_string_schema_across_batches(self):
        all_null = documents_to_table([{"_id": ObjectId(), "is_paypal": None}])
        integers = documents_to_table(
            [
                {"_id": ObjectId(), "is_paypal": 0},
                {"_id": ObjectId(), "is_paypal": 1},
            ]
        )
        booleans = documents_to_table(
            [
                {"_id": ObjectId(), "is_paypal": False},
                {"_id": ObjectId(), "is_paypal": True},
            ]
        )

        for table in (all_null, integers, booleans):
            self.assertTrue(pa.types.is_string(table.schema.field("is_paypal").type))
        self.assertEqual(integers.column("is_paypal").to_pylist(), ["0", "1"])
        self.assertEqual(booleans.column("is_paypal").to_pylist(), ["false", "true"])

    def test_all_null_nested_leaves_have_string_schema(self):
        table = documents_to_table(
            [{"_id": ObjectId(), "metadata": {"value": None}, "items": [None]}]
        )

        metadata_type = table.schema.field("metadata").type
        items_type = table.schema.field("items").type
        self.assertTrue(pa.types.is_string(metadata_type.field("value").type))
        self.assertTrue(pa.types.is_string(items_type.value_type))

    def test_writes_nested_parquet(self):
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "nested.parquet"
            write_parquet(
                [{"_id": ObjectId(), "items": [{"name": "ring", "tags": ["gold"]}]}],
                destination,
            )
            table = pq.read_table(destination)
            self.assertEqual(table.num_rows, 1)
            self.assertTrue(pa.types.is_list(table.schema.field("items").type))

    def test_writes_bigquery_compatible_nested_lists(self):
        document = {
            "_id": ObjectId("5e931f9633eacf36f47aa4c4"),
            "cart_products": [
                {
                    "amount": 1,
                    "currency": "USD",
                    "option": [
                        {
                            "option_id": 10,
                            "option_label": "Color",
                            "value_id": 20,
                            "value_label": "White",
                        }
                    ],
                    "price": "100.00",
                    "product_id": 30,
                }
            ],
        }

        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "cart-products.parquet"
            write_parquet([document], destination)

            parquet_file = pq.ParquetFile(destination)
            parquet_schema = str(parquet_file.schema)
            table = parquet_file.read()
            parquet_file.close()
            cart_products_type = table.schema.field("cart_products").type
            option_type = cart_products_type.value_type.field("option").type

            # The LIST annotations let BigQuery omit the physical list/element
            # nodes when Parquet List inference is enabled on the load job.
            self.assertGreaterEqual(parquet_schema.count("(List)"), 2)
            self.assertTrue(pa.types.is_list(cart_products_type))
            self.assertTrue(pa.types.is_struct(cart_products_type.value_type))
            self.assertTrue(pa.types.is_list(option_type))
            self.assertTrue(pa.types.is_struct(option_type.value_type))
            self.assertEqual(
                table.to_pylist(),
                [
                    {
                        **document,
                        "_id": str(document["_id"]),
                        "cart_products": [
                            {
                                **document["cart_products"][0],
                                "amount": "1",
                                "option": [
                                    {
                                        "option_id": "10",
                                        "option_label": "Color",
                                        "value_id": "20",
                                        "value_label": "White",
                                    }
                                ],
                                "product_id": "30",
                            }
                        ],
                    }
                ],
            )

    def test_uploads_numbered_files_and_saves_checkpoint(self):
        with tempfile.TemporaryDirectory() as directory:
            locations = Path(directory) / "locations.jsonl"
            products = Path(directory) / "products.jsonl"
            locations.write_text('{"location":"VN"}\n', encoding="utf-8")
            products.write_text('{"product_id":"1"}\n', encoding="utf-8")
            settings = SimpleNamespace(
                load_gcs_bucket="raw_glamira",
                load_gcs_prefix="mongodb_data",
                load_documents_per_file=2,
                load_checkpoint=Path(directory) / "load-checkpoint.json",
            )
            client = _FakeStorageClient()
            documents = [{"_id": ObjectId(), "items": [{"value": number}]} for number in range(5)]

            result = export_to_gcs(
                settings,
                storage_client=client,
                documents=documents,
                jsonl_files={
                    "locations.jsonl": locations,
                    "products.jsonl": products,
                },
            )

            bucket = client.bucket("raw_glamira")
            self.assertEqual(result.files_uploaded, 3)
            self.assertEqual(result.documents_uploaded, 5)
            self.assertEqual(result.checkpoint.next_file_number, 4)
            self.assertIn("mongodb_data/1.parquet", bucket.objects)
            self.assertIn("mongodb_data/2.parquet", bucket.objects)
            self.assertIn("mongodb_data/3.parquet", bucket.objects)
            self.assertIn("mongodb_data/_checkpoint.json", bucket.objects)
            self.assertEqual(
                bucket.objects["locations.jsonl"]["content"], locations.read_bytes()
            )
            self.assertEqual(
                bucket.objects["products.jsonl"]["content"], products.read_bytes()
            )
            self.assertTrue(settings.load_checkpoint.is_file())

    def test_jsonl_upload_overwrites_objects_at_bucket_root(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "products.jsonl"
            source.write_text('{"version":2}\n', encoding="utf-8")
            bucket = _FakeBucket("raw_glamira")
            bucket.objects["products.jsonl"] = {
                "content": b'{"version":1}\n',
                "metadata": None,
                "generation": 1,
            }

            uploaded = upload_jsonl_exports(bucket, {"products.jsonl": source})

            self.assertEqual(uploaded, ("products.jsonl",))
            self.assertEqual(bucket.objects["products.jsonl"]["content"], source.read_bytes())
            self.assertNotIn("mongodb_data/products.jsonl", bucket.objects)


if __name__ == "__main__":
    unittest.main()
