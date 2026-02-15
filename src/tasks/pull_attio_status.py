import time

import requests
from prefect import task
from tqdm import tqdm

from src.config.clients import get_supabase_client
from src.config.settings import get_settings
from src.utils.db import upsert_in_batches
from src.utils.logger import get_logger


def get_dealflow_details(api_token, domains_list):
    """
    1. Queries 'dealflow' List Entries filtering by root_domain using specific path syntax.
    2. Queries Company records to map those entries back to the specific domains.
    3. Returns { domain: { status: ..., stage: ... } or None }
    """

    # Initialize results with None
    results = {domain: None for domain in domains_list}

    if not domains_list:
        return results

    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json",
    }

    # ======================================================
    # STEP 1: Query List Entries (Filtering by Root Domain)
    # ======================================================
    entries_url = "https://api.attio.com/v2/lists/dealflow/entries/query"

    # Construct the $or array dynamically based on input domains
    or_filter = []
    for domain in domains_list:
        or_filter.append(
            {
                "path": [["dealflow", "parent_record"], ["companies", "domains"]],
                "constraints": {"root_domain": domain},
            }
        )

    entries_payload = {
        "filter": {"$or": or_filter},
    }

    found_entries_map = {}  # Maps record_id -> {status, stage}
    parent_record_ids = set()

    try:
        resp_entries = requests.post(entries_url, headers=headers, json=entries_payload)
        resp_entries.raise_for_status()
        entries_data = resp_entries.json().get("data", [])
        for entry in entries_data:
            p_id = entry["parent_record_id"]
            parent_record_ids.add(p_id)

            # Extract Values using V2 structure (arrays of objects)
            entry_vals = entry.get("entry_values", {})

            # 1. Status
            status_val = (
                entry_vals.get("status", [{}])[0].get("status", {}).get("title")
            )

            # 2. Stage (dd_stage_1)
            stage_val = (
                entry_vals.get("dd_stage_1", [{}])[0].get("status", {}).get("title")
            )
            if status_val or stage_val:
                found_entries_map[p_id] = {"status": status_val, "stage": stage_val}

    except Exception as e:
        print(f"Error in Step 1 (List Entries): {e}")
        if "resp_entries" in locals():
            print(resp_entries.text)
        return results

    if not parent_record_ids:
        return results

    # ======================================================
    # STEP 2: Query Companies to Map Domains
    # ======================================================
    # We batch query the companies to find which domain belongs to which ID
    companies_url = "https://api.attio.com/v2/objects/companies/records/"

    try:
        companies_data = []
        for record_id in tqdm(
            parent_record_ids, desc="Querying companies of dealflow entries"
        ):
            resp_companies = requests.get(companies_url + record_id, headers=headers)
            resp_companies.raise_for_status()
            time.sleep(0.25)
            company_data = resp_companies.json().get("data")
            if company_data:
                companies_data.append(company_data)

        # ======================================================
        # STEP 3: Stitching it all together
        # ======================================================
        for company in companies_data:
            r_id = company["id"]["record_id"]

            # Get the domains this company actually has in Attio
            comp_domains = [d["domain"] for d in company["values"].get("domains", [])]

            # Retrieve the status/stage we found in Step 1
            entry_details = found_entries_map.get(r_id)

            if entry_details:
                # Check if any of this company's domains match the ones we asked for
                for d in comp_domains:
                    if d in results:
                        results[d] = entry_details

    except Exception as e:
        print(f"Error in Step 2 (Companies): {e}")

    return results


@task(name="pull_attio_status")
def pull_attio_status(domains: list[str]):
    """
    Pull dealflow status from both Attio workspaces (BY and CG),
    merge with CG taking priority, and upsert into business_computed_values.
    """
    logger = get_logger()
    settings = get_settings()

    domains = list(set(domains))
    logger.info(f"Pulling Attio status for {len(domains)} domains")

    # Pull from BY workspace
    logger.info("Pulling from Attio BY workspace")
    by_results = get_dealflow_details(
        settings.attio_by_token.get_secret_value(), domains
    )

    # Pull from CG workspace
    logger.info("Pulling from Attio CG workspace")
    cg_results = get_dealflow_details(
        settings.attio_cg_token.get_secret_value(), domains
    )

    # Merge: start with BY, then override with CG (CG takes priority)
    merged = {}
    for domain in domains:
        by_entry = by_results.get(domain)
        cg_entry = cg_results.get(domain)

        if cg_entry:
            merged[domain] = cg_entry
        elif by_entry:
            merged[domain] = by_entry

    # Build upsert records
    records_to_upsert = []
    for domain, details in merged.items():
        records_to_upsert.append(
            {
                "domain": domain,
                "in_attio": True,
                "attio_status": details["status"],
                "attio_stage": details["stage"],
            }
        )

    # Also mark domains not found in either workspace
    domains_not_in_attio = [d for d in domains if d not in merged]
    for domain in domains_not_in_attio:
        records_to_upsert.append(
            {
                "domain": domain,
                "in_attio": False,
                "attio_status": None,
                "attio_stage": None,
            }
        )

    logger.info(
        f"Upserting {len(records_to_upsert)} records "
        f"({len(merged)} in Attio, {len(domains_not_in_attio)} not in Attio)"
    )

    upsert_in_batches(
        get_supabase_client(),
        "business_computed_values",
        records_to_upsert,
        on_conflict="domain",
        logger=logger,
    )
