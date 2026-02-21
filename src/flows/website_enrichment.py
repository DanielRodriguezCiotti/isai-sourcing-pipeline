from prefect import flow, task

from src.tasks import WebsiteEnrichmentQAInput, website_ai_parsing, website_crawling
from src.utils.logger import get_logger

BATCH_SIZE = 20


@task(name="website_enrichment_task")
def website_enrichment_task(domains: list[str]):
    domains = list(set(domains))
    logger = get_logger()
    logger.info(f"Starting website enrichment for {len(domains)} domains")
    for i in range(0, len(domains), BATCH_SIZE):
        logger.info(
            f"Processing batch {i // BATCH_SIZE + 1}/{len(domains) // BATCH_SIZE}"
        )
        batch = domains[i : i + BATCH_SIZE]
        results = website_crawling(batch)
        inputs = [
            WebsiteEnrichmentQAInput(
                company_id=data.record_id, domain=domain, content=data.content
            )
            for domain, data in results.items()
        ]
        website_ai_parsing(inputs)
    logger.info("Website enrichment completed")


@flow(name="website-enrichment-flow")
def website_enrichment_flow(domains: list[str]):
    domains = list(set(domains))
    logger = get_logger()
    logger.info(f"Starting website enrichment for {len(domains)} domains")
    for i in range(0, len(domains), BATCH_SIZE):
        logger.info(
            f"Processing batch {i // BATCH_SIZE + 1}/{len(domains) // BATCH_SIZE}"
        )
        batch = domains[i : i + BATCH_SIZE]
        results = website_crawling(batch)
        inputs = [
            WebsiteEnrichmentQAInput(
                company_id=data.record_id, domain=domain, content=data.content
            )
            for domain, data in results.items()
        ]
        website_ai_parsing(inputs)
    logger.info("Website enrichment completed")
