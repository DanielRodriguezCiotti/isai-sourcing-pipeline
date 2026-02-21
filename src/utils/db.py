"""Shared Supabase batch helpers used by reconciliation tasks."""

import math

import httpx
import pandas as pd
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

BATCH_SIZE = 1000


@retry(
    retry=retry_if_exception_type(httpx.RemoteProtocolError),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    reraise=True,
)
def _fetch_batch(client, table: str, select: str, column: str, batch: list) -> list:
    resp = client.table(table).select(select).in_(column, batch).execute()
    return resp.data


def fetch_in_batches(
    client,
    table: str,
    column: str,
    values: list,
    select: str = "*",
    batch_size: int = BATCH_SIZE,
) -> list[dict]:
    """Fetch rows from a Supabase table filtering column IN values, batched."""
    rows: list[dict] = []
    for i in range(0, len(values), batch_size):
        batch = values[i : i + batch_size]
        rows.extend(_fetch_batch(client, table, select, column, batch))
    return rows


def keep_latest_per_domain(records: list[dict]) -> list[dict]:
    """Keep only the latest record per domain based on updated_at."""
    latest: dict[str, dict] = {}
    for record in records:
        domain = record["domain"]
        if domain not in latest or record["updated_at"] > latest[domain]["updated_at"]:
            latest[domain] = record
    return list(latest.values())


def fetch_as_dataframe(
    client, table: str, column: str, values: list[str]
) -> pd.DataFrame:
    """Fetch rows from a Supabase table filtered by values, returned as DataFrame."""
    rows = fetch_in_batches(client, table, column, values)
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def delete_in_batches(client, table: str, column: str, values: list):
    """Delete rows from a Supabase table where column IN values, batched."""
    for i in range(0, len(values), BATCH_SIZE):
        batch = values[i : i + BATCH_SIZE]
        client.table(table).delete().in_(column, batch).execute()


def insert_in_batches(client, table: str, records: list[dict], logger):
    """Insert records into a Supabase table in batches."""
    total_batches = math.ceil(len(records) / BATCH_SIZE) if records else 0
    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i : i + BATCH_SIZE]
        client.table(table).insert(batch).execute()
        logger.info(f"Inserted batch {i // BATCH_SIZE + 1}/{total_batches}")


def upsert_in_batches(
    client, table: str, records: list[dict], on_conflict: str, logger
):
    """Upsert records into a Supabase table in batches."""
    total_batches = math.ceil(len(records) / BATCH_SIZE) if records else 0
    for i in range(0, len(records), BATCH_SIZE):
        batch = [_strip_null_bytes(r) for r in records[i : i + BATCH_SIZE]]
        client.table(table).upsert(batch, on_conflict=on_conflict).execute()
        logger.info(f"Upserted batch {i // BATCH_SIZE + 1}/{total_batches}")


def sanitize(val):
    """Replace any non-JSON-compliant float (nan/inf) with None."""
    if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
        return None
    return val


def _strip_null_bytes(val):
    """Recursively strip null bytes (\\u0000) from strings, lists, and dicts."""
    if isinstance(val, str):
        return val.replace("\x00", "")
    if isinstance(val, list):
        return [_strip_null_bytes(v) for v in val]
    if isinstance(val, dict):
        return {k: _strip_null_bytes(v) for k, v in val.items()}
    return val
