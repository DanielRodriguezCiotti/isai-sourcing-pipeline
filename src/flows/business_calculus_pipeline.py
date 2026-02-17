from prefect import flow

from src.tasks import (
    annotate_company_tags,
    compute_founders_values,
    compute_funding_metrics,
    compute_scores,
    embed_textual_dimensions,
    fuzzy_matching_metrics,
    pull_attio_status,
)
from src.utils.logger import get_logger


@flow(name="business-calculus-pipeline")
def business_calculus_pipeline(domains: list[str]):
    domains = list(set(domains))
    BATCH_SIZE = 100
    logger = get_logger()
    for i in range(0, len(domains), BATCH_SIZE):
        batch = domains[i : i + BATCH_SIZE]
        logger.info(
            f"Processing batch {i // BATCH_SIZE + 1}/{len(domains) // BATCH_SIZE}"
        )
        pull_attio_status(batch)
        logger.info(f"Pulled attio status for {len(batch)} domains")
        fuzzy_matching_metrics(batch)
        logger.info(f"Computed fuzzy matching metrics for {len(batch)} domains")
        compute_funding_metrics(batch)
        logger.info(f"Computed funding metrics for {len(batch)} domains")
        compute_founders_values(batch)
        logger.info(f"Computed founders values for {len(batch)} domains")
        annotate_company_tags(batch)
        logger.info(f"Annotated company tags for {len(batch)} domains")
        embed_textual_dimensions(batch)
        logger.info(f"Embedded textual dimensions for {len(batch)} domains")
        compute_scores(batch)
        logger.info(f"Computed scores for {len(batch)} domains")


if __name__ == "__main__":
    with open("calcul_domains.txt", "r") as f:
        domains = f.read().splitlines()
    business_calculus_pipeline(domains)
