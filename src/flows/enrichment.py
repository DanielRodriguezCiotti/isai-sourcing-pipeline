from prefect import flow

from src.tasks import (
    companies_reconciliation,
    founders_reconciliation,
    funding_rounds_reconciliation,
)

from .website_enrichment import website_enrichment_task


@flow(name="enrichment-flow")
def enrichment_flow(domains: list[str]):
    domains = list(set(domains))
    companies_reconciliation(domains)
    parallel_tasks = [
        founders_reconciliation.submit(domains),
        funding_rounds_reconciliation.submit(domains),
        website_enrichment_task.submit(domains),
    ]
    for future in parallel_tasks:
        future.result()
