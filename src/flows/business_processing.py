from typing import Optional

from prefect import flow, task
from prefect.futures import wait
from pydantic import BaseModel, Field

from src.tasks import (
    annotate_company_tags,
    compute_founders_values,
    compute_funding_metrics,
    compute_scores,
    embed_textual_dimensions,
    fuzzy_matching_metrics,
    pull_attio_status,
)


class BusinessProcessingConfig(BaseModel):
    sync_attio_status: bool = Field(default=True)
    compute_fuzzy_matching_metrics: bool = Field(default=True)
    compute_funding_metrics: bool = Field(default=True)
    compute_founders_values: bool = Field(default=True)
    annotate_company_tags: bool = Field(default=True)
    compute_scores: bool = Field(default=True)


@task(name="embed_and_compute_scores")
def embed_and_compute_scores(domains: list[str], scores_enabled: bool = True):
    embed_textual_dimensions(domains)
    if scores_enabled:
        compute_scores(domains)


@flow(name="business-processing-flow")
def business_processing_flow(
    domains: list[str],
    config: Optional[BusinessProcessingConfig] = BusinessProcessingConfig(),
):
    domains = list(set(domains))
    parallel_tasks = []
    if config.sync_attio_status:
        parallel_tasks.append(pull_attio_status.submit(domains))
    if config.compute_fuzzy_matching_metrics:
        parallel_tasks.append(fuzzy_matching_metrics.submit(domains))
    if config.compute_funding_metrics:
        parallel_tasks.append(compute_funding_metrics.submit(domains))
    if config.compute_founders_values:
        parallel_tasks.append(compute_founders_values.submit(domains))
    if config.annotate_company_tags:
        parallel_tasks.append(annotate_company_tags.submit(domains))
    parallel_tasks.append(
        embed_and_compute_scores.submit(domains, config.compute_scores)
    )
    wait(parallel_tasks)
