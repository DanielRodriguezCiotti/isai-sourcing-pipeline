import time

from loguru import logger
from prefect import task


@task
def save_to_db(companies_enrichment:dict[str,dict]):
    logger.info("Saving to DB")
    time.sleep(3)
    logger.info("Succesfully saved to DB")