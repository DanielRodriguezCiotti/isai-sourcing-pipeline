from prefect import flow

from src.tasks import WebsiteEnrichmentQAInput, website_ai_parsing, website_crawling
from src.utils.logger import get_logger

BATCH_SIZE = 20


@flow(name="website_enrichment-flow")
def website_enrichment_flow(domains: list[str]):
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


if __name__ == "__main__":
    with open("domains.txt", "r") as f:
        domains = f.read().splitlines()
    website_enrichment_flow(domains)
