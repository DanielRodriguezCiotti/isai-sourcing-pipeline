from prefect import flow

from src.tasks import (
    annotate_company_tags,
    compute_funding_metrics,
    fuzzy_matching_metrics,
    pull_attio_status,
)


@flow(name="business-calculus-pipeline")
def business_calculus_pipeline(domains: list[str]):
    pull_attio_status(domains)
    fuzzy_matching_metrics(domains)
    compute_funding_metrics(domains)
    annotate_company_tags(domains)


if __name__ == "__main__":
    domains = [
        "kiter.app",
        "novenda.com",
        "loyal.guru",
        "simplismart.ai",
        "fynxt.com",
        "sagardefence.com",
        "snkpeek.app",
        "kovaibsf.com",
        "aro.homes",
        "fispan.com",
    ]
    business_calculus_pipeline(domains)
