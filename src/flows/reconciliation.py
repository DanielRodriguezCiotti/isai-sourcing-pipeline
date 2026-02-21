from prefect import flow

from src.tasks import (
    companies_reconciliation,
    founders_reconciliation,
    funding_rounds_reconciliation,
)


@flow(name="reconciliation-flow")
def reconciliation_flow(domains: list[str]):
    domains = list(set(domains))
    companies_reconciliation(domains)
    parallel_tasks = [
        founders_reconciliation.submit(domains),
        funding_rounds_reconciliation.submit(domains),
    ]
    for future in parallel_tasks:
        future.result()
