import time

from prefect import task
from prefect.logging import get_run_logger


@task
def save_to_db(companies_enrichment:dict[str,dict]):
    logger = get_run_logger()
    logger.info("Saving to DB")
    time.sleep(3)
    logger.info("Succesfully saved to DB")