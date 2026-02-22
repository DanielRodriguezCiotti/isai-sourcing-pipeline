"""
Founders reconciliation task.

Takes a list of unique domains and reconciles founder data from
crunchbase_founders or traxcn_founders into the founders table in Supabase.

Priority: if the company source includes crunchbase ("both" or "crunchbase"),
use crunchbase_founders. Otherwise use traxcn_founders.
"""

from prefect import task
from prefect.tasks import exponential_backoff

from src.config.clients import get_supabase_client
from src.utils.db import delete_in_batches, fetch_in_batches, insert_in_batches
from src.utils.logger import get_logger

# ---------------------------------------------------------------------------
# Main task
# ---------------------------------------------------------------------------


@task(
    name="founders_reconciliation",
    retries=3,
    retry_delay_seconds=exponential_backoff(backoff_factor=4),
)
def founders_reconciliation(domains: list[str]):
    logger = get_logger()
    client = get_supabase_client()

    logger.info(f"Starting founders reconciliation for {len(domains)} domains")

    # Step 1 – get company_id and source for each domain from the companies table
    company_rows = fetch_in_batches(
        client, "companies", "domain", domains, select="id, domain, source"
    )
    company_map = {r["domain"]: r for r in company_rows}
    logger.info(f"Found {len(company_map)} companies in companies table")

    # Step 2 – group domains by source
    cb_domains = [
        d
        for d in domains
        if company_map.get(d, {}).get("source") in ("both", "crunchbase")
    ]
    tx_domains = [
        d for d in domains if company_map.get(d, {}).get("source") == "traxcn"
    ]
    logger.info(f"CB source: {len(cb_domains)} | TX source: {len(tx_domains)}")

    # Step 3.1 – delete existing founders for all input companies
    company_ids = [company_map[d]["id"] for d in domains if d in company_map]
    if company_ids:
        delete_in_batches(client, "founders", "company_id", company_ids)
        logger.info(f"Deleted existing founders for {len(company_ids)} companies")

    # Step 3.2 – build new founder records from the appropriate source
    all_records: list[dict] = []

    # --- Crunchbase founders ---
    if cb_domains:
        # Get crunchbase_id for each domain
        cb_companies = fetch_in_batches(
            client,
            "crunchbase_companies",
            "domain",
            cb_domains,
            select="domain, crunchbase_id",
        )
        domain_to_cb_id = {r["domain"]: r["crunchbase_id"] for r in cb_companies}
        cb_id_to_domain = {v: k for k, v in domain_to_cb_id.items()}

        # Fetch founders linked to those crunchbase ids
        cb_ids = list(domain_to_cb_id.values())
        cb_founders = fetch_in_batches(
            client,
            "crunchbase_founders",
            "crunchbase_company_uuid",
            cb_ids,
            batch_size=500,
        )
        logger.info(f"Fetched {len(cb_founders)} crunchbase founders")

        for f in cb_founders:
            domain = cb_id_to_domain.get(f["crunchbase_company_uuid"])
            if domain and domain in company_map:
                all_records.append(
                    {
                        "company_id": company_map[domain]["id"],
                        "name": f.get("name"),
                        "role": f.get("job_title"),
                        "description": f.get("description"),
                        "linkedin_url": f.get("linkedin_url"),
                        "source": "crunchbase",
                    }
                )

    # --- Tracxn founders ---
    if tx_domains:
        tx_founders = fetch_in_batches(
            client,
            "traxcn_founders",
            "domain_name",
            tx_domains,
            batch_size=500,
        )
        logger.info(f"Fetched {len(tx_founders)} traxcn founders")

        for f in tx_founders:
            domain = f.get("domain_name")
            if domain and domain in company_map:
                all_records.append(
                    {
                        "company_id": company_map[domain]["id"],
                        "name": f.get("founder_name"),
                        "role": f.get("title"),
                        "description": f.get("description"),
                        "linkedin_url": f.get("profile_links"),
                        "source": "traxcn",
                    }
                )

    # Step 4 – insert all new founder records
    if all_records:
        insert_in_batches(client, "founders", all_records, logger)
        logger.info(f"Inserted {len(all_records)} founders total")
    else:
        logger.info("No founders to insert")

    logger.info("Founders reconciliation complete")
