from pymongo import ASCENDING, MongoClient
from bson import ObjectId
from pathlib import Path

from google.api_core.exceptions import Forbidden, GoogleAPIError
from google.cloud import storage

from .config.config import Settings


def export_to_gcs(
    bucket_name: str
    source_directory: str,
    destination_prefix: str = '',
) -> None:
    if settings.mongo_username:
        client_options["username"] = settings.mongo_username
    if settings.mongo_password:
        client_options["password"] = settings.mongo_password
    if settings.mongo_username or settings.mongo_password:
        client_options["authSource"] = settings.mongo_auth_source
