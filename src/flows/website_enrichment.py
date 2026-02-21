from prefect import flow, task

from src.config.settings import get_settings
from src.tasks import WebsiteEnrichmentQAInput, website_ai_parsing, website_crawling
from src.utils.logger import get_logger


@task(name="website_enrichment_task")
def website_enrichment_task(domains: list[str]):
    settings = get_settings()
    domains = list(set(domains))
    logger = get_logger()
    logger.info(f"Starting website enrichment for {len(domains)} domains")
    for i in range(0, len(domains), settings.website_enrichment_batch_size):
        logger.info(
            f"Processing batch {i // settings.website_enrichment_batch_size + 1}/{len(domains) // settings.website_enrichment_batch_size}"
        )
        batch = domains[i : i + settings.website_enrichment_batch_size]
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
    settings = get_settings()
    domains = list(set(domains))
    logger = get_logger()
    logger.info(f"Starting website enrichment for {len(domains)} domains")
    for i in range(0, len(domains), settings.website_enrichment_batch_size):
        logger.info(
            f"Processing batch {i // settings.website_enrichment_batch_size + 1}/{len(domains) // settings.website_enrichment_batch_size}"
        )
        batch = domains[i : i + settings.website_enrichment_batch_size]
        results = website_crawling(batch)
        inputs = [
            WebsiteEnrichmentQAInput(
                company_id=data.record_id, domain=domain, content=data.content
            )
            for domain, data in results.items()
        ]
        website_ai_parsing(inputs)
    logger.info("Website enrichment completed")
