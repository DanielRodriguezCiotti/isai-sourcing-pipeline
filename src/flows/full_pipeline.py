from prefect import flow
from prefect.task_runners import ThreadPoolTaskRunner

from .business_processing import business_processing_flow
from .enrichment import enrichment_flow


@flow(name="full-pipeline-flow", task_runner=ThreadPoolTaskRunner(max_workers=8))
def full_pipeline_flow(domains: list[str]):
    enrichment_flow(domains)
    business_processing_flow(domains)
