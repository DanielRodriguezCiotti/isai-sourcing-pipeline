import time

from loguru import logger
from prefect import task


@task
def get_companies_from_export():
    logger.info("Importing companies from export")
    time.sleep(3)
    logger.info("Imported 10 companies")
    return [f"company_nb_{k+1}" for k in range(10)]
