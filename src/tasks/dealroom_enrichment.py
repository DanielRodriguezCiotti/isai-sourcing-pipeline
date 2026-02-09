import time

from loguru import logger
from prefect import task


@task
def dealroom_enrich(companies:list[str]):
    logger.info(f"Got {len(companies)} to enrich")
    time.sleep(3)
    logger.info("Succesful enrichment")
    return dict(zip(companies,[{"dealroom_headcount":10,"dealroom_traffic":10}]*len(companies)))