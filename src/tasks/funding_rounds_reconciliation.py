"""
Funding rounds reconciliation task.

Takes a list of unique domains and pulls funding rounds from BOTH
crunchbase_funding_rounds and traxcn_funding_rounds into the unified
funding_rounds table in Supabase.
"""

from prefect import task
from prefect.logging import get_run_logger

from src.config.clients import get_supabase_client
from src.utils.db import fetch_in_batches, sanitize, upsert_in_batches


# ---------------------------------------------------------------------------
# Main task
# ---------------------------------------------------------------------------


@task(name="funding_rounds_reconciliation")
def funding_rounds_reconciliation(domains: list[str]):
    logger = get_run_logger()
    client = get_supabase_client()

    logger.info(f"Starting funding rounds reconciliation for {len(domains)} domains")

    # Step 1 – get company_id for each domain from the companies table
    company_rows = fetch_in_batches(
        client, "companies", "domain", domains, select="id, domain"
    )
    company_map = {r["domain"]: r["id"] for r in company_rows}
    logger.info(f"Found {len(company_map)} companies in companies table")

    all_records: list[dict] = []

    # Step 3a – crunchbase funding rounds
    # Resolve domain → crunchbase_id
    cb_companies = fetch_in_batches(
        client, "crunchbase_companies", "domain", domains,
        select="domain, crunchbase_id",
    )
    domain_to_cb_id = {r["domain"]: r["crunchbase_id"] for r in cb_companies}
    cb_id_to_domain = {v: k for k, v in domain_to_cb_id.items()}

    if domain_to_cb_id:
        cb_ids = list(domain_to_cb_id.values())
        cb_rounds = fetch_in_batches(
            client, "crunchbase_funding_rounds", "crunchbase_company_uuid", cb_ids,
        )
        logger.info(f"Fetched {len(cb_rounds)} crunchbase funding rounds")

        for r in cb_rounds:
            domain = cb_id_to_domain.get(r["crunchbase_company_uuid"])
            if domain and domain in company_map:
                all_records.append({
                    "company_id": company_map[domain],
                    "date": r.get("announced_on"),
                    "stage": r.get("investment_type"),
                    "amount": sanitize(r.get("raised_amount_usd")),
                    "lead_investors": r.get("lead_investors"),
                    "all_investors": r.get("lead_investors"),
                    "source": "crunchbase",
                })

    # Step 3b – tracxn funding rounds
    tx_rounds = fetch_in_batches(
        client, "traxcn_funding_rounds", "domain_name", domains,
    )
    logger.info(f"Fetched {len(tx_rounds)} traxcn funding rounds")

    for r in tx_rounds:
        domain = r.get("domain_name")
        if domain and domain in company_map:
            all_records.append({
                "company_id": company_map[domain],
                "date": r.get("round_date"),
                "stage": r.get("round_name"),
                "amount": sanitize(r.get("round_amount_in_usd")),
                "lead_investors": r.get("lead_investor"),
                "all_investors": r.get("institutional_investors"),
                "source": "traxcn",
            })

    # Step 4 – upsert all records (update on uniqueness constraint conflict)
    if all_records:
        upsert_in_batches(
            client, "funding_rounds", all_records,
            on_conflict="date,company_id,stage,source", logger=logger,
        )
        logger.info(f"Upserted {len(all_records)} funding rounds total")
    else:
        logger.info("No funding rounds to upsert")

    logger.info("Funding rounds reconciliation complete")
