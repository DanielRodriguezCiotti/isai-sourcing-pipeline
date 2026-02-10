"""Shared Supabase batch helpers used by reconciliation tasks."""

import math

import pandas as pd

BATCH_SIZE = 1000


def fetch_in_batches(
    client, table: str, column: str, values: list, select: str = "*"
) -> list[dict]:
    """Fetch rows from a Supabase table filtering column IN values, batched."""
    rows: list[dict] = []
    for i in range(0, len(values), BATCH_SIZE):
        batch = values[i : i + BATCH_SIZE]
        resp = client.table(table).select(select).in_(column, batch).execute()
        rows.extend(resp.data)
    return rows


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
        batch = records[i : i + BATCH_SIZE]
        client.table(table).upsert(batch, on_conflict=on_conflict).execute()
        logger.info(f"Upserted batch {i // BATCH_SIZE + 1}/{total_batches}")


def sanitize(val):
    """Replace any non-JSON-compliant float (nan/inf) with None."""
    if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
        return None
    return val
