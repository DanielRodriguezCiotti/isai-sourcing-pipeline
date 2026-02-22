"""
This code computes the following metrics:
- [x]  vc_current_stage TEXT, if the vc_current_stage is available in companies table we use it, otherwise we take the last stage from funding_rounds table
- [x]  first_vc_round_date DATE, we take the earliest date from funding_rounds table
- [x]  first_vc_round_amount NUMERIC, we take the amount of the first round from funding_rounds table
- [x]  last_vc_round_date DATE, if the last_vc_round_date is available in companies table we use it, otherwise we take the latest date from funding_rounds table
- [x]  last_vc_round_amount NUMERIC, if the last_vc_round_amount is available in companies table we use it, otherwise we take the amount of the last round from funding_rounds table
- [x]  all_investors TEXT[], we concat all_investors from companies table with the concat of all_investors in funding_rounds for all date but the same source. Logic is the following, we have funding round from crunchbase and traxcn in funding rounds, so you will chose the source that has more entries and concat the list or all_investors for it. Donc forget to do a list(set) to remove duplicates
- [x]  last_round_lead_investors TEXT[], the list of all_investors for the last round in funding rounds table
- [x]  total_number_of_funding_rounds INTEGER, the max cross sources btw number of entries in funding_rounds table for crunchbase and traxcn
"""

from prefect import task
from prefect.tasks import exponential_backoff

from src.config.clients import get_supabase_client
from src.utils.db import fetch_in_batches, upsert_in_batches
from src.utils.logger import get_logger


def _compute_for_company(company: dict, rounds: list[dict]) -> dict:
    domain = company["domain"]

    # Sort rounds by date (only keep rounds that have a date)
    dated_rounds = [r for r in rounds if r.get("date") is not None]
    dated_rounds.sort(key=lambda r: r["date"])

    # Split rounds by source
    cb_rounds = [r for r in rounds if r.get("source") == "crunchbase"]
    tx_rounds = [r for r in rounds if r.get("source") == "traxcn"]

    # --- vc_current_stage ---
    vc_current_stage = company.get("vc_current_stage")
    if not vc_current_stage and dated_rounds:
        vc_current_stage = dated_rounds[-1].get("stage")

    # --- first_vc_round_date & first_vc_round_amount ---
    first_vc_round_date = None
    first_vc_round_amount = None
    if dated_rounds:
        first_vc_round_date = dated_rounds[0]["date"]
        first_vc_round_amount = dated_rounds[0].get("amount")

    # --- last_vc_round_date ---
    last_vc_round_date = company.get("last_funding_date")
    if not last_vc_round_date and dated_rounds:
        last_vc_round_date = dated_rounds[-1]["date"]

    # --- last_vc_round_amount ---
    last_vc_round_amount = company.get("last_funding_amount")
    if last_vc_round_amount is None and dated_rounds:
        last_vc_round_amount = dated_rounds[-1].get("amount")

    # --- all_investors ---
    # 1) From companies table
    company_investors = company.get("all_investors") or []

    # 2) From funding_rounds: pick the source with more entries, concat all_investors
    best_source_rounds = cb_rounds if len(cb_rounds) >= len(tx_rounds) else tx_rounds
    fr_investors: list[str] = []
    for r in best_source_rounds:
        inv = r.get("all_investors") or []
        if isinstance(inv, list):
            fr_investors.extend(inv)

    # 3) Merge, strip surrounding quotes, and deduplicate
    raw = [i for i in company_investors if i] + [i for i in fr_investors if i]
    cleaned = [i.strip("'\"").strip() for i in raw]
    seen: dict[str, str] = {}
    for name in cleaned:
        if name and name.lower() not in seen:
            seen[name.lower()] = name
    all_investors = list(seen.values()) if seen else None

    # --- last_round_lead_investors ---
    last_round_lead_investors: list[str] = []
    if dated_rounds:
        last_round = dated_rounds[-1]
        investors = last_round.get("all_investors") or []
        if investors:
            clean_investors = [i.strip("'\"").strip() for i in investors]
            seen: dict[str, str] = {}
            for name in clean_investors:
                if name and name.lower() not in seen:
                    seen[name.lower()] = name
            last_round_lead_investors = list(seen.values()) if seen else None

    # --- total_number_of_funding_rounds ---
    cb_count = len(cb_rounds)
    tx_count = len(tx_rounds)
    total_number = max(cb_count, tx_count) if (cb_count or tx_count) else None
    return {
        "domain": domain,
        "vc_current_stage": vc_current_stage,
        "first_vc_round_date": first_vc_round_date,
        "first_vc_round_amount": first_vc_round_amount,
        "last_vc_round_date": last_vc_round_date,
        "last_vc_round_amount": last_vc_round_amount,
        "all_investors": all_investors,
        "last_round_lead_investors": last_round_lead_investors,
        "total_number_of_funding_rounds": total_number,
    }


@task(
    name="compute_funding_metrics",
    retries=3,
    retry_delay_seconds=exponential_backoff(backoff_factor=4),
)
def compute_funding_metrics(domains: list[str]):
    logger = get_logger()
    client = get_supabase_client()

    logger.info(f"Starting compute_funding_metrics for {len(domains)} domains")

    # Step 1 – Fetch companies data (fields needed for fallback logic)
    company_rows = fetch_in_batches(
        client,
        "companies",
        "domain",
        domains,
        select="id, domain, vc_current_stage, last_funding_date, last_funding_amount, all_investors",
    )
    company_map: dict[str, dict] = {}
    id_to_domain: dict[str, str] = {}
    for row in company_rows:
        company_map[row["domain"]] = row
        id_to_domain[row["id"]] = row["domain"]

    logger.info(f"Fetched {len(company_map)} companies")

    # Step 2 – Fetch all funding rounds for these companies
    company_ids = list(id_to_domain.keys())
    funding_rows = fetch_in_batches(
        client,
        "funding_rounds",
        "company_id",
        company_ids,
        select="company_id, date, stage, amount, lead_investors, all_investors, source",
    )
    logger.info(f"Fetched {len(funding_rows)} funding rounds")

    # Step 3 – Group funding rounds by domain
    rounds_by_domain: dict[str, list[dict]] = {}
    for fr in funding_rows:
        domain = id_to_domain.get(fr["company_id"])
        if domain:
            rounds_by_domain.setdefault(domain, []).append(fr)

    # Step 4 – Compute metrics per company
    records = []
    for domain, company in company_map.items():
        rounds = rounds_by_domain.get(domain, [])
        records.append(_compute_for_company(company, rounds))

    # Step 5 – Upsert to business_computed_values
    if records:
        upsert_in_batches(
            client,
            "business_computed_values",
            records,
            on_conflict="domain",
            logger=logger,
        )
        logger.info(f"Upserted {len(records)} funding metric records")
    else:
        logger.info("No records to upsert")

    logger.info("compute_funding_metrics complete")
