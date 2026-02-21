from typing import Optional

from prefect import flow, task
from pydantic import BaseModel, Field

from src.config.clients import get_supabase_client
from src.config.settings import get_settings
from src.tasks import (
    annotate_company_tags,
    compute_founders_values,
    compute_funding_metrics,
    compute_scores,
    embed_textual_dimensions,
    fuzzy_matching_metrics,
    pull_attio_status,
)


def retrieve_all_domains_in_business_computed_values():
    client = get_supabase_client()
    rows = client.table("business_computed_values").select("domain").execute().data
    return [row["domain"] for row in rows]


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
    recompute_all: bool = False,
):
    if recompute_all:
        domains = retrieve_all_domains_in_business_computed_values()
    domains = list(set(domains))
    settings = get_settings()
    batches = [
        domains[i : i + settings.compute_business_metric_batch_size]
        for i in range(0, len(domains), settings.compute_business_metric_batch_size)
    ]
    for batch in batches:
        parallel_tasks = []
        if config.sync_attio_status:
            parallel_tasks.append(pull_attio_status.submit(batch))
        if config.compute_fuzzy_matching_metrics:
            parallel_tasks.append(fuzzy_matching_metrics.submit(batch))
        if config.compute_funding_metrics:
            parallel_tasks.append(compute_funding_metrics.submit(batch))
        if config.compute_founders_values:
            parallel_tasks.append(compute_founders_values.submit(batch))
        if config.annotate_company_tags:
            parallel_tasks.append(annotate_company_tags.submit(batch))
        parallel_tasks.append(
            embed_and_compute_scores.submit(batch, config.compute_scores)
        )
        for future in parallel_tasks:
            future.result()
