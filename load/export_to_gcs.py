from __future__ import annotations

import json
import logging
import os
import tempfile
from base64 import b64encode
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping

import pyarrow as pa
import pyarrow.parquet as pq
from bson import Binary, Decimal128, ObjectId, Regex, Timestamp
from google.api_core.exceptions import NotFound, PreconditionFailed
from google.cloud import storage
from pymongo import ASCENDING, MongoClient
from pymongo.read_preferences import SecondaryPreferred

from config.config import Settings


logger = logging.getLogger(__name__)
CHECKPOINT_OBJECT = "_checkpoint.json"
PARQUET_LEAF_TYPE = pa.string()
DEFAULT_JSONL_EXPORTS = {
    "locations.jsonl": Path(__file__).resolve().parent.parent / "data" / "locations.jsonl",
    "products.jsonl": Path(__file__).resolve().parent.parent / "data" / "products.jsonl",
}


@dataclass(frozen=True)
class LoadCheckpoint:
    last_mongo_id: str | None = None
    next_file_number: int = 1
    documents_uploaded: int = 0
    last_gcs_object: str | None = None
    updated_at: str | None = None

    @classmethod
    def from_json(cls, payload: str) -> "LoadCheckpoint":
        raw = json.loads(payload)
        return cls(
            last_mongo_id=raw.get("last_mongo_id"),
            next_file_number=max(1, int(raw.get("next_file_number", 1))),
            documents_uploaded=max(0, int(raw.get("documents_uploaded", 0))),
            last_gcs_object=raw.get("last_gcs_object"),
            updated_at=raw.get("updated_at"),
        )

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False, indent=2) + "\n"


@dataclass(frozen=True)
class LoadResult:
    files_uploaded: int
    documents_uploaded: int
    checkpoint: LoadCheckpoint


def _normalize_bson_legacy(value: Any) -> Any:
    """Chuẩn hóa dữ liệu có trong MongoDB BSON thành các kiểu dữ liệu tương thích với Arrow mà không làm phẳng cấu trúc lồng nhau."""
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, Decimal128):
        return value.to_decimal()
    if isinstance(value, Binary):
        return bytes(value)
    if isinstance(value, Timestamp):
        return datetime.fromtimestamp(value.time, tz=timezone.utc)
    if isinstance(value, Regex):
        return str(value.pattern)
    if isinstance(value, dict):
        return {str(key): normalize_bson(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [normalize_bson(item) for item in value]
    if isinstance(value, (str, bytes, bool, int, float, Decimal, datetime, date, type(None))):
        return value
    return str(value)


def normalize_bson(value: Any) -> Any:
    """Convert every BSON leaf to string while preserving dict/list structure."""
    if value is None:
        return None
    if isinstance(value, dict):
        return {str(key): normalize_bson(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [normalize_bson(item) for item in value]
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, Decimal128):
        return str(value.to_decimal())
    if isinstance(value, (Binary, bytes)):
        return b64encode(bytes(value)).decode("ascii")
    if isinstance(value, Timestamp):
        return datetime.fromtimestamp(value.time, tz=timezone.utc).isoformat()
    if isinstance(value, Regex):
        return str(value.pattern)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _harmonize_values(values: list[Any]) -> list[Any]:
    """Make heterogeneous MongoDB values Arrow-compatible without flattening nesting."""
    # Tạo danh sách non null values
    non_null = [value for value in values if value is not None]
    if not non_null:
        return values

    if any(isinstance(value, list) for value in non_null):
        # Bọc vào list nều tồn tại ít nhất 1 list
        promoted = [
            value if value is None or isinstance(value, list) else [value]
            for value in values
        ]
        flat: list[Any] = []
        lengths: list[int | None] = []
        for value in promoted:
            if value is None:
                lengths.append(None)
            else:
                lengths.append(len(value))
                flat.extend(value)
        harmonized_flat = _harmonize_values(flat) if flat else []
        result: list[Any] = []
        offset = 0
        for length in lengths:
            if length is None:
                result.append(None)
            else:
                result.append(harmonized_flat[offset : offset + length])
                offset += length
        return result

    if any(isinstance(value, dict) for value in non_null):
        # A scalar mixed with objects becomes a struct with a reserved value field.
        promoted = [
            value if value is None or isinstance(value, dict) else {"_value": value}
            for value in values
        ]
        keys = sorted(
            {key for value in promoted if isinstance(value, dict) for key in value}
        )
        columns = {
            key: _harmonize_values(
                [value.get(key) if isinstance(value, dict) else None for value in promoted]
            )
            for key in keys
        }
        return [
            None
            if value is None
            else {key: columns[key][index] for key in keys}
            for index, value in enumerate(promoted)
        ]

    types = {type(value) for value in non_null}
    if len(types) == 1:
        return values
    if all(isinstance(value, (bool, int, float)) for value in non_null):
        # Keep integers when possible; promote mixed numeric values to float.
        if all(isinstance(value, (bool, int)) for value in non_null):
            return [None if value is None else int(value) for value in values]
        return [None if value is None else float(value) for value in values]
    if all(isinstance(value, (date, datetime)) for value in non_null):
        return [
            datetime.combine(value, datetime.min.time())
            if isinstance(value, date) and not isinstance(value, datetime)
            else value
            for value in values
        ]
    # MongoDB permits a field to change scalar type. String is the only lossless,
    # broadly queryable common representation for otherwise incompatible scalars.
    return [None if value is None else str(value) for value in values]


def harmonize_documents(documents: list[dict[str, Any]]) -> list[dict[str, Any]]:
    harmonized = _harmonize_values(documents)
    return [value for value in harmonized if isinstance(value, dict)]


def _normalize_is_paypal_legacy(rows: list[dict[str, Any]]) -> None:
    """Keep ``is_paypal`` as nullable BOOLEAN in every Parquet file."""
    for row in rows:
        value = row.get("is_paypal")
        if value is None or isinstance(value, bool):
            row["is_paypal"] = value
        elif isinstance(value, int) and value in (0, 1):
            row["is_paypal"] = bool(value)
        else:
            raise ValueError(
                "is_paypal chỉ chấp nhận null, boolean hoặc integer 0/1; "
                f"nhận được {value!r}"
            )


def _string_leaf_field(field: pa.Field) -> pa.Field:
    data_type = field.type
    if pa.types.is_null(data_type):
        normalized_type = PARQUET_LEAF_TYPE
    elif pa.types.is_struct(data_type):
        normalized_type = pa.struct(
            [_string_leaf_field(child) for child in data_type]
        )
    elif pa.types.is_list(data_type):
        normalized_type = pa.list_(_string_leaf_field(data_type.value_field))
    elif pa.types.is_large_list(data_type):
        normalized_type = pa.large_list(_string_leaf_field(data_type.value_field))
    else:
        normalized_type = PARQUET_LEAF_TYPE
    return pa.field(field.name, normalized_type, nullable=True)


def _string_leaf_schema(schema: pa.Schema) -> pa.Schema:
    """Replace inferred null leaves with STRING and retain nested containers."""
    return pa.schema([_string_leaf_field(field) for field in schema])


def documents_to_table(documents: Iterable[dict[str, Any]]) -> pa.Table:
    rows = harmonize_documents([normalize_bson(document) for document in documents])
    if not rows:
        raise ValueError("Không thể tạo Parquet từ batch rỗng")
    try:
        inferred_schema = pa.Table.from_pylist(rows).schema
        return pa.Table.from_pylist(rows, schema=_string_leaf_schema(inferred_schema))
    except (pa.ArrowInvalid, pa.ArrowTypeError) as exc:
        raise ValueError(
            "Không thể hợp nhất schema BSON trong batch; một field đang chứa các kiểu "
            f"không tương thích: {exc}"
        ) from exc


def write_parquet(documents: Iterable[dict[str, Any]], destination: Path) -> pa.Schema:
    table = documents_to_table(documents)
    destination.parent.mkdir(parents=True, exist_ok=True)
    # BigQuery can collapse Parquet's physical ``list``/``element`` wrapper into
    # a REPEATED field only when the column carries the standard LIST logical
    # annotation. Keep the compliant three-level encoding explicit instead of
    # relying on PyArrow's default, which can change between library versions.
    pq.write_table(
        table,
        destination,
        compression="snappy",
        use_compliant_nested_type=True,
    )
    return table.schema


def _object_name(prefix: str, name: str) -> str:
    clean_prefix = prefix.strip("/")
    return f"{clean_prefix}/{name}" if clean_prefix else name


def _mongo_options(settings: Settings) -> dict[str, Any]:
    options: dict[str, Any] = {
        "read_preference": SecondaryPreferred(),
        "appname": "glamira-mongodb-parquet-loader",
    }
    if settings.mongo_username:
        options.update(
            username=settings.mongo_username,
            password=settings.mongo_password,
            authSource=settings.mongo_auth_source,
        )
    return options


def iter_mongo_documents(settings: Settings, last_mongo_id: str | None) -> Iterator[dict[str, Any]]:
    query: dict[str, Any] = {}
    if last_mongo_id:
        try:
            query["_id"] = {"$gt": ObjectId(last_mongo_id)}
        except Exception as exc:
            raise ValueError(f"Checkpoint chứa ObjectId không hợp lệ: {last_mongo_id}") from exc

    with MongoClient(settings.mongo_uri, **_mongo_options(settings)) as client:
        collection = client[settings.mongo_db][settings.mongo_collection]
        cursor = (
            collection.find(query, no_cursor_timeout=True)
            .sort("_id", ASCENDING)
            .batch_size(settings.mongo_batch_size)
        )
        try:
            yield from cursor
        finally:
            cursor.close()


def batched(documents: Iterable[dict[str, Any]], size: int) -> Iterator[list[dict[str, Any]]]:
    if size < 1:
        raise ValueError("documents_per_file phải lớn hơn hoặc bằng 1")
    batch: list[dict[str, Any]] = []
    for document in documents:
        batch.append(document)
        if len(batch) == size:
            yield batch
            batch = []
    if batch:
        yield batch


def _read_local_checkpoint(path: Path) -> LoadCheckpoint | None:
    if not path.is_file():
        return None
    return LoadCheckpoint.from_json(path.read_text(encoding="utf-8"))


def _write_local_checkpoint(path: Path, checkpoint: LoadCheckpoint) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(checkpoint.to_json(), encoding="utf-8")
    os.replace(temporary, path)


def _read_gcs_checkpoint(bucket: Any, prefix: str) -> tuple[LoadCheckpoint | None, int | None]:
    blob = bucket.blob(_object_name(prefix, CHECKPOINT_OBJECT))
    try:
        payload = blob.download_as_text()
        return LoadCheckpoint.from_json(payload), blob.generation
    except NotFound:
        return None, None


def _write_gcs_checkpoint(
    bucket: Any,
    prefix: str,
    checkpoint: LoadCheckpoint,
    generation: int | None,
) -> int:
    blob = bucket.blob(_object_name(prefix, CHECKPOINT_OBJECT))
    blob.upload_from_string(
        checkpoint.to_json(),
        content_type="application/json",
        if_generation_match=0 if generation is None else generation,
    )
    return int(blob.generation)


def _upload_parquet(bucket: Any, prefix: str, number: int, path: Path, batch: list[dict[str, Any]]) -> str:
    object_name = _object_name(prefix, f"{number}.parquet")
    blob = bucket.blob(object_name)
    first_id = str(batch[0]["_id"])
    last_id = str(batch[-1]["_id"])
    blob.metadata = {
        "first_mongo_id": first_id,
        "last_mongo_id": last_id,
        "document_count": str(len(batch)),
    }
    try:
        blob.upload_from_filename(str(path), if_generation_match=0)
    except PreconditionFailed:
        blob.reload()
        metadata = blob.metadata or {}
        expected = {
            "first_mongo_id": first_id,
            "last_mongo_id": last_id,
            "document_count": str(len(batch)),
        }
        if any(metadata.get(key) != value for key, value in expected.items()):
            raise FileExistsError(
                f"GCS object đã tồn tại nhưng không khớp batch hiện tại: gs://{bucket.name}/{object_name}"
            )
        logger.warning("Khôi phục batch đã upload trước checkpoint: gs://%s/%s", bucket.name, object_name)
    return object_name


def upload_jsonl_exports(bucket: Any, files: Mapping[str, Path]) -> tuple[str, ...]:
    """Upload crawler JSONL outputs to the bucket root, replacing old objects."""
    missing = [str(path) for path in files.values() if not path.is_file()]
    if missing:
        raise FileNotFoundError(
            "Khong tim thay file JSONL de upload: " + ", ".join(missing)
        )

    uploaded: list[str] = []
    for object_name, path in files.items():
        if "/" in object_name or "\\" in object_name:
            raise ValueError(f"JSONL object phai nam o bucket root: {object_name}")
        bucket.blob(object_name).upload_from_filename(
            str(path),
            content_type="application/x-ndjson",
        )
        uploaded.append(object_name)
        logger.info("Da upload va ghi de gs://%s/%s", bucket.name, object_name)
    return tuple(uploaded)


def export_to_gcs(
    settings: Settings,
    *,
    documents_per_file: int | None = None,
    storage_client: Any | None = None,
    documents: Iterable[dict[str, Any]] | None = None,
    jsonl_files: Mapping[str, Path] | None = None,
) -> LoadResult:
    if not settings.load_gcs_bucket:
        raise ValueError("load.gcs_bucket không được để trống")
    size = documents_per_file or settings.load_documents_per_file
    if size < 1:
        raise ValueError("documents_per_file phải lớn hơn hoặc bằng 1")

    client = storage_client or storage.Client()
    bucket = client.bucket(settings.load_gcs_bucket)
    upload_jsonl_exports(bucket, jsonl_files or DEFAULT_JSONL_EXPORTS)
    gcs_checkpoint, generation = _read_gcs_checkpoint(bucket, settings.load_gcs_prefix)
    local_checkpoint = _read_local_checkpoint(settings.load_checkpoint)
    checkpoint = gcs_checkpoint or local_checkpoint or LoadCheckpoint()
    if gcs_checkpoint and local_checkpoint and gcs_checkpoint != local_checkpoint:
        logger.warning("Checkpoint local khác GCS; sử dụng checkpoint GCS")

    source = documents if documents is not None else iter_mongo_documents(settings, checkpoint.last_mongo_id)
    files_uploaded = 0
    run_documents = 0
    with tempfile.TemporaryDirectory(prefix="glamira-parquet-") as temporary_dir:
        for batch in batched(source, size):
            number = checkpoint.next_file_number
            local_parquet = Path(temporary_dir) / f"{number}.parquet"
            schema = write_parquet(batch, local_parquet)
            logger.debug("Parquet schema file=%s: %s", number, schema)
            object_name = _upload_parquet(
                bucket, settings.load_gcs_prefix, number, local_parquet, batch
            )
            checkpoint = LoadCheckpoint(
                last_mongo_id=str(batch[-1]["_id"]),
                next_file_number=number + 1,
                documents_uploaded=checkpoint.documents_uploaded + len(batch),
                last_gcs_object=object_name,
                updated_at=datetime.now(timezone.utc).isoformat(),
            )
            generation = _write_gcs_checkpoint(
                bucket, settings.load_gcs_prefix, checkpoint, generation
            )
            _write_local_checkpoint(settings.load_checkpoint, checkpoint)
            files_uploaded += 1
            run_documents += len(batch)
            logger.info(
                "Đã upload gs://%s/%s | documents=%d | last_id=%s",
                settings.load_gcs_bucket,
                object_name,
                len(batch),
                checkpoint.last_mongo_id,
            )

    return LoadResult(files_uploaded, run_documents, checkpoint)
