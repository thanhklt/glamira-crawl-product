import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from glamira_crawl.locations import (
    LOCATION_FIELDS,
    LocationStats,
    normalize_ip,
    record_to_location,
    unique_normalized_ips,
    write_locations,
)


class LocationTests(unittest.TestCase):
    def test_normalizes_and_deduplicates_ip_values(self):
        stats = LocationStats()

        result = list(
            unique_normalized_ips(
                [" 8.8.8.8 ", "8.8.8.8", "2001:0db8::1", "bad", None],
                stats,
            )
        )

        self.assertEqual(result, ["8.8.8.8", "2001:db8::1"])
        self.assertEqual(stats.mongo_unique, 5)
        self.assertEqual(stats.valid_unique, 2)
        self.assertEqual(stats.invalid, 2)
        self.assertEqual(stats.normalized_duplicates, 1)

    def test_rejects_non_string_ip(self):
        self.assertIsNone(normalize_ip(1234))

    def test_maps_ip2location_record_to_requested_fields(self):
        record = SimpleNamespace(
            city="Paris",
            region="Ile-de-France",
            country_short="FR",
            country_long="France",
            latitude="48.8566",
            longitude=2.3522,
        )

        result = record_to_location("37.170.17.183", record)

        self.assertEqual(tuple(result), LOCATION_FIELDS)
        self.assertEqual(result["country_code"], "FR")
        self.assertEqual(result["latitude"], 48.8566)
        self.assertEqual(result["longitude"], 2.3522)

    def test_writes_each_result_directly_to_jsonl(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "locations.jsonl"

            stats = write_locations(
                ["8.8.8.8", "1.1.1.1"],
                output_path=output,
                workers=2,
                lookup=lambda ip: {field: ip if field == "ip" else None for field in LOCATION_FIELDS},
            )

            rows = [json.loads(line) for line in output.read_text(encoding="utf-8").splitlines()]
            self.assertEqual({row["ip"] for row in rows}, {"8.8.8.8", "1.1.1.1"})
            self.assertEqual(stats.written, 2)

    def test_lookup_error_still_writes_ip_with_null_location(self):
        def failed_lookup(_ip):
            raise RuntimeError("lookup failed")

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "locations.jsonl"
            stats = write_locations(
                ["8.8.8.8"],
                output_path=output,
                workers=1,
                lookup=failed_lookup,
            )

            row = json.loads(output.read_text(encoding="utf-8"))
            self.assertEqual(row["ip"], "8.8.8.8")
            self.assertIsNone(row["city_name"])
            self.assertEqual(stats.lookup_errors, 1)


if __name__ == "__main__":
    unittest.main()
