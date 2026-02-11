from prefect import flow

from src.tasks import (
    companies_reconciliation,
    founders_reconciliation,
    funding_rounds_reconciliation,
    ingest_traxcn_export,
)


@flow(name="reconciliation-flow")
def full_pipeline_flow(supabase_file_path: str):
    domains = ingest_traxcn_export(supabase_file_path)
    companies_reconciliation(domains)
    founders_reconciliation(domains)
    funding_rounds_reconciliation(domains)


if __name__ == "__main__":
    full_pipeline_flow(supabase_file_path="traxcn_export_sample.xlsx")
