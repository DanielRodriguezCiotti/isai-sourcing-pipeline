from prefect import flow

from src.tasks import (
    companies_reconciliation,
    founders_reconciliation,
    funding_rounds_reconciliation,
)
from src.utils.logger import get_logger


@flow(name="reconciliation-flow")
def reconciliation_flow(domains: list[str]):
    logger = get_logger()
    logger.info(f"Starting reconciliation for {len(domains)} domains")
    batch_size = 500
    for i in range(0, len(domains), batch_size):
        batch = domains[i : i + batch_size]
        companies_reconciliation(batch)
        founders_reconciliation(batch)
        funding_rounds_reconciliation(batch)
    logger.info("Reconciliation completed")
