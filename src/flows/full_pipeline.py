from prefect import flow

from src.tasks import (
    companies_reconciliation,
    founders_reconciliation,
    funding_rounds_reconciliation,
    ingest_traxcn_export,
)

@task(name="sumbit_and_track_batch")
def sumbit_and_track_batch(batch: list[str]):

@flow(name="full-pipeline-flow")
def full_pipeline_flow(supabase_file_path: str):
    domains = ingest_traxcn_export(supabase_file_path)
    batch_size = 500
    for i in range(0, len(domains), batch_size):
        batch = domains[i : i + batch_size]
        companies_reconciliation(batch)
        founders_reconciliation(batch)
        funding_rounds_reconciliation(batch)
