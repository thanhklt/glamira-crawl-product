import pyarrow.fs as fs
import pyarrow.parquet as pq


gcs = fs.GcsFileSystem()

selector = fs.FileSelector(
    "raw_glamira/mongodb_data",
    recursive=False,
)

files = gcs.get_file_info(selector)

for file_info in files:
    if not file_info.path.endswith(".parquet"):
        continue

    schema = pq.read_schema(
        file_info.path,
        filesystem=gcs,
    )

    if "is_paypal" in schema.names:
        print(
            file_info.path,
            schema.field("is_paypal").type,
        )