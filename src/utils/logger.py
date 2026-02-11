import logging

from prefect.exceptions import MissingContextError
from prefect.logging import get_run_logger


# Get the logger from prefect if available, otherwise use the default logger
def get_logger() -> logging.Logger:
    try:
        return get_run_logger()
    except MissingContextError:
        return logging.getLogger(__name__)
