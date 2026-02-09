import time

from prefect import task
from prefect.logging import get_run_logger


@task
def get_companies_from_export():
    logger = get_run_logger()
    logger.info("Importing companies from export")
    time.sleep(3)
    logger.info("Imported 10 companies")
    return [f"company_nb_{k+1}" for k in range(10)]
