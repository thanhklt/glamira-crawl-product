from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


class StateStore:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(path, timeout=60)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=NORMAL")
        self._create_schema()

    def _create_schema(self) -> None:
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS products (
                product_id TEXT PRIMARY KEY,
                status TEXT NOT NULL DEFAULT 'pending',
                attempts INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                updated_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_products_status ON products(status, product_id);

            CREATE TABLE IF NOT EXISTS candidate_urls (
                product_id TEXT NOT NULL,
                url TEXT NOT NULL,
                source TEXT NOT NULL,
                priority INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (product_id, url),
                FOREIGN KEY (product_id) REFERENCES products(product_id)
            );
            CREATE INDEX IF NOT EXISTS idx_candidate_product ON candidate_urls(product_id, priority);

            CREATE TABLE IF NOT EXISTS results (
                product_id TEXT PRIMARY KEY,
                payload TEXT NOT NULL,
                source_url TEXT NOT NULL,
                react_data_url TEXT NOT NULL,
                crawled_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS url_failures (
                product_id TEXT NOT NULL,
                url TEXT NOT NULL,
                last_error TEXT NOT NULL,
                occurrences INTEGER NOT NULL DEFAULT 1,
                last_failed_at TEXT NOT NULL,
                recovered_via TEXT,
                PRIMARY KEY (product_id, url)
            );

            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            """
        )
        self.connection.commit()

    def close(self) -> None:
        self.connection.close()

    def get_metadata(self, key: str) -> str | None:
        row = self.connection.execute("SELECT value FROM metadata WHERE key = ?", (key,)).fetchone()
        return None if row is None else str(row["value"])

    def add_candidates(
        self,
        candidates: Iterable[tuple[str, str | None, str]],
        *,
        max_urls: int,
        checkpoint_key: str,
        checkpoint: str,
    ) -> int:
        inserted = 0
        now = utc_now()
        with self.connection:
            for product_id, url, source in candidates:
                cursor = self.connection.execute(
                    "INSERT OR IGNORE INTO products(product_id, updated_at) VALUES (?, ?)",
                    (product_id, now),
                )
                inserted += cursor.rowcount
                if url:
                    self.connection.execute(
                        """
                        INSERT OR IGNORE INTO candidate_urls(product_id, url, source, priority)
                        SELECT ?, ?, ?, COALESCE(MAX(priority), -1) + 1
                        FROM candidate_urls
                        WHERE product_id = ?
                        HAVING COUNT(*) < ?
                        """,
                        (product_id, url, source, product_id, max_urls),
                    )
            self.connection.execute(
                "INSERT INTO metadata(key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (checkpoint_key, checkpoint),
            )
        return inserted

    def reset_interrupted(self) -> int:
        with self.connection:
            cursor = self.connection.execute(
                "UPDATE products SET status = 'pending', updated_at = ? WHERE status = 'in_progress'",
                (utc_now(),),
            )
        return cursor.rowcount

    def requeue_failed(self) -> int:
        with self.connection:
            cursor = self.connection.execute(
                "UPDATE products SET status = 'pending', updated_at = ? WHERE status = 'failed'",
                (utc_now(),),
            )
        return cursor.rowcount

    def claim(self, limit: int) -> list[tuple[str, list[str]]]:
        with self.connection:
            rows = self.connection.execute(
                "SELECT product_id FROM products WHERE status = 'pending' ORDER BY product_id LIMIT ?",
                (limit,),
            ).fetchall()
            product_ids = [str(row["product_id"]) for row in rows]
            if product_ids:
                ids = ",".join("?" for _ in product_ids)
                self.connection.execute(
                    f"UPDATE products SET status = 'in_progress', attempts = attempts + 1, "
                    f"updated_at = ? WHERE product_id IN ({ids})",
                    (utc_now(), *product_ids),
                )

        claimed: list[tuple[str, list[str]]] = []
        for product_id in product_ids:
            urls = self.connection.execute(
                "SELECT url FROM candidate_urls WHERE product_id = ? ORDER BY priority",
                (product_id,),
            ).fetchall()
            claimed.append((product_id, [str(row["url"]) for row in urls]))
        return claimed

    def save_success(
        self,
        product_id: str,
        payload: dict[str, Any],
        source_url: str,
        react_data_url: str,
        url_errors: Iterable[tuple[str, str]] = (),
    ) -> None:
        now = utc_now()
        encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        with self.connection:
            self._record_url_failures(product_id, url_errors, recovered_via=source_url, now=now)
            self.connection.execute(
                """
                INSERT INTO results(product_id, payload, source_url, react_data_url, crawled_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(product_id) DO UPDATE SET
                    payload = excluded.payload,
                    source_url = excluded.source_url,
                    react_data_url = excluded.react_data_url,
                    crawled_at = excluded.crawled_at
                """,
                (product_id, encoded, source_url, react_data_url, now),
            )
            self.connection.execute(
                "UPDATE products SET status = 'done', last_error = NULL, updated_at = ? WHERE product_id = ?",
                (now, product_id),
            )

    def save_failure(
        self,
        product_id: str,
        error: str,
        url_errors: Iterable[tuple[str, str]] = (),
    ) -> None:
        now = utc_now()
        with self.connection:
            self._record_url_failures(product_id, url_errors, recovered_via=None, now=now)
            self.connection.execute(
                "UPDATE products SET status = 'failed', last_error = ?, updated_at = ? WHERE product_id = ?",
                (error[-2000:], now, product_id),
            )

    def _record_url_failures(
        self,
        product_id: str,
        url_errors: Iterable[tuple[str, str]],
        *,
        recovered_via: str | None,
        now: str,
    ) -> None:
        for url, error in url_errors:
            self.connection.execute(
                """
                INSERT INTO url_failures(
                    product_id, url, last_error, occurrences, last_failed_at, recovered_via
                ) VALUES (?, ?, ?, 1, ?, ?)
                ON CONFLICT(product_id, url) DO UPDATE SET
                    last_error = excluded.last_error,
                    occurrences = url_failures.occurrences + 1,
                    last_failed_at = excluded.last_failed_at,
                    recovered_via = excluded.recovered_via
                """,
                (product_id, url, error[-2000:], now, recovered_via),
            )

    def export_jsonl(self, output: Path, include_metadata: bool = True) -> int:
        output.parent.mkdir(parents=True, exist_ok=True)
        temporary = output.with_suffix(output.suffix + ".tmp")
        count = 0
        with temporary.open("w", encoding="utf-8", newline="\n") as handle:
            cursor = self.connection.execute(
                "SELECT product_id, payload, source_url, react_data_url, crawled_at FROM results ORDER BY product_id"
            )
            for row in cursor:
                payload = json.loads(row["payload"])
                if include_metadata:
                    payload["_crawl"] = {
                        "requested_product_id": row["product_id"],
                        "source_url": row["source_url"],
                        "react_data_url": row["react_data_url"],
                        "crawled_at": row["crawled_at"],
                    }
                handle.write(json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n")
                count += 1
        temporary.replace(output)
        return count

    def export_failed_urls(self, output: Path) -> int:
        output.parent.mkdir(parents=True, exist_ok=True)
        temporary = output.with_suffix(output.suffix + ".tmp")
        count = 0
        with temporary.open("w", encoding="utf-8", newline="\n") as handle:
            cursor = self.connection.execute(
                """
                SELECT product_id, url, last_error, occurrences, last_failed_at, recovered_via
                FROM url_failures
                ORDER BY product_id, url
                """
            )
            for row in cursor:
                item = {
                    "product_id": row["product_id"],
                    "failed_url": row["url"],
                    "last_error": row["last_error"],
                    "occurrences": row["occurrences"],
                    "last_failed_at": row["last_failed_at"],
                    "recovered_via": row["recovered_via"],
                }
                handle.write(json.dumps(item, ensure_ascii=False, separators=(",", ":")) + "\n")
                count += 1
        temporary.replace(output)
        return count

    def stats(self) -> dict[str, int]:
        result = {"pending": 0, "in_progress": 0, "done": 0, "failed": 0}
        for row in self.connection.execute("SELECT status, COUNT(*) AS total FROM products GROUP BY status"):
            result[str(row["status"])] = int(row["total"])
        result["results"] = int(self.connection.execute("SELECT COUNT(*) FROM results").fetchone()[0])
        result["candidate_urls"] = int(
            self.connection.execute("SELECT COUNT(*) FROM candidate_urls").fetchone()[0]
        )
        result["failed_urls"] = int(self.connection.execute("SELECT COUNT(*) FROM url_failures").fetchone()[0])
        return result
