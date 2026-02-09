import time

from prefect import task
from prefect.logging import get_run_logger


@task
def dealroom_enrich(companies:list[str]):
    logger = get_run_logger()
    logger.info(f"Got {len(companies)} to enrich")
    time.sleep(3)
    logger.info("Succesful enrichment")
    return dict(zip(companies,[{"dealroom_headcount":10,"dealroom_traffic":10}]*len(companies)))