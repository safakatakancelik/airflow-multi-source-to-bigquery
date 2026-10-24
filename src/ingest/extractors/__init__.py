"""One small extractor per source type. Each takes the resolved source options and returns a
DataFrame in the source's own shape; shared cleanup happens later in ``ingest.transform``.

Airflow hooks are imported lazily inside each function so config validation, transforms and
unit tests never need provider packages or credentials.
"""

from __future__ import annotations

import io
from collections.abc import Callable
from typing import Any

import pandas as pd

Extractor = Callable[[dict[str, Any]], pd.DataFrame]


def databricks_sql(opts: dict[str, Any]) -> pd.DataFrame:
    from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook

    hook = DatabricksSqlHook(databricks_conn_id=opts["conn_id"], http_path=opts["http_path"])
    return hook.get_df(opts["query"], df_type="pandas")


def postgres(opts: dict[str, Any]) -> pd.DataFrame:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    hook = PostgresHook(postgres_conn_id=opts["conn_id"])
    return hook.get_df(opts["query"], df_type="pandas")


def mongo(opts: dict[str, Any]) -> pd.DataFrame:
    from airflow.providers.mongo.hooks.mongo import MongoHook

    hook = MongoHook(mongo_conn_id=opts["conn_id"])
    cursor = hook.find(
        mongo_collection=opts["collection"],
        query=opts.get("filter") or {},
        mongo_db=opts["database"],
    )
    docs = list(cursor)
    for doc in docs:
        # ObjectId is not Parquet-serialisable; its string form is the natural key.
        if "_id" in doc:
            doc["_id"] = str(doc["_id"])
    # Nested sub-documents become columns like address.city (snake-cased later).
    return pd.json_normalize(docs, sep=".")


def _read_bytes(content: bytes, fmt: str) -> pd.DataFrame:
    buffer = io.BytesIO(content)
    fmt = fmt.lower()
    if fmt == "csv":
        return pd.read_csv(buffer)
    if fmt == "parquet":
        return pd.read_parquet(buffer)
    raise ValueError(f"Unsupported file format {fmt!r} (use csv or parquet)")


def http_file(opts: dict[str, Any]) -> pd.DataFrame:
    import requests

    response = requests.get(opts["url"], timeout=60)
    response.raise_for_status()
    return _read_bytes(response.content, opts["format"])


def s3_file(opts: dict[str, Any]) -> pd.DataFrame:
    """Read one CSV/Parquet object. S3-compatible stores (MinIO, R2) work via the connection's
    ``endpoint_url`` extra."""
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    hook = S3Hook(aws_conn_id=opts["conn_id"])
    obj = hook.get_key(key=opts["object_key"], bucket_name=opts["bucket"])
    return _read_bytes(obj.get()["Body"].read(), opts["format"])


def rest_api(opts: dict[str, Any]) -> pd.DataFrame:
    """GET a JSON endpoint; optionally follow a ``next`` URL field until it is empty."""
    from airflow.providers.http.hooks.http import HttpHook

    hook = HttpHook(method="GET", http_conn_id=opts["conn_id"])
    session = hook.get_conn()
    url: str | None = hook.url_from_endpoint(opts["endpoint"])
    params: dict[str, Any] | None = opts.get("params")
    records_path: str | None = opts.get("records_path")
    next_path: str | None = opts.get("next_path")
    max_pages = int(opts.get("max_pages", 10))

    records: list[dict[str, Any]] = []
    for _ in range(max_pages):
        response = session.get(url, params=params, timeout=60)
        response.raise_for_status()
        payload = response.json()
        page = payload[records_path] if records_path else payload
        records.extend(page)
        url = payload.get(next_path) if next_path and isinstance(payload, dict) else None
        params = None  # a `next` URL already carries its own query string
        if not url:
            break
    return pd.json_normalize(records, sep=".")


EXTRACTORS: dict[str, Extractor] = {
    "databricks_sql": databricks_sql,
    "postgres": postgres,
    "mongo": mongo,
    "s3_file": s3_file,
    "http_file": http_file,
    "rest_api": rest_api,
}
