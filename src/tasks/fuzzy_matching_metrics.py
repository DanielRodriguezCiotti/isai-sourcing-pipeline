"""
This task computes the three fuzzy matching metrics "global_2000_clients", "competitors_cg", "competitors_by", "key_platforms_cg", "key_platforms_by"
"""

from prefect import task

from src.config.clients import get_supabase_client
from src.utils.db import fetch_in_batches, keep_latest_per_domain, upsert_in_batches
from src.utils.fuzzy_matcher import CompanyFuzzyMatcher
from src.utils.logger import get_logger

FINAL_COLUMN_MAPPING = {
    "by_competitors": "competitors_by",
    "by_platforms": "platforms_by",
    "cg_competitors": "competitors_cg",
    "cg_sw_platforms": "platforms_cg",
    "global_2000": "global_2000_clients",
}


def load_references():
    """
    Load the references values from tables by_competitors, by_platforms, cg_competitors, cg_sw_platforms, global_2000
    """
    client = get_supabase_client()
    by_competitors = [
        record["name"]
        for record in client.table("by_competitors").select("name").execute().data
    ]
    by_platforms = [
        record["name"]
        for record in client.table("by_platforms").select("name").execute().data
    ]
    cg_competitors = [
        record["name"]
        for record in client.table("cap_competitors").select("name").execute().data
    ]
    cg_sw_platforms = [
        record["name"]
        for record in client.table("cap_sw_partners").select("name").execute().data
    ]
    global_2000 = [
        record["name"]
        for record in client.table("global_2000").select("name").execute().data
    ]
    return {
        "by_competitors": by_competitors,
        "by_platforms": by_platforms,
        "cg_competitors": cg_competitors,
        "cg_sw_platforms": cg_sw_platforms,
        "global_2000": global_2000,
    }


@task(name="fuzzy_matching_metrics")
def fuzzy_matching_metrics(domains: list[str]):
    """
    This task computes the three fuzzy matching metrics "global_2000_clients", "competitors_cg", "competitors_by_name"
    """
    logger = get_logger()
    # 1. Loads the identified clients and partners from the web_scraping_enrichment table
    clients_and_partners = fetch_in_batches(
        get_supabase_client(),
        "web_scraping_enrichment",
        "domain",
        domains,
        select="domain, key_clients, key_partners, updated_at",
    )
    clients_and_partners = keep_latest_per_domain(clients_and_partners)
    all_clients_and_partners = []
    for record in clients_and_partners:
        if record["key_clients"] is not None:
            all_clients_and_partners.extend(record["key_clients"])
        if record["key_partners"] is not None:
            all_clients_and_partners.extend(record["key_partners"])
    all_clients_and_partners = list(set(all_clients_and_partners))
    # 2. Load the references values from tables by_competitors, by_platforms, cg_competitors, cg_sw_platforms, global_2000
    references = load_references()
    # 3. Compute the fuzzy matching metrics agains each reference for all the clients and partners
    maps_identified_references = {}
    for key_ref, list_refs in references.items():
        logger.info(f"Computing fuzzy matching metrics for {key_ref}")
        fuzzy_matcher = CompanyFuzzyMatcher(list_refs)
        results = fuzzy_matcher.match_batch(all_clients_and_partners)
        map_name_to_match = {result["input"]: result["match"] for result in results}
        maps_identified_references[key_ref] = map_name_to_match
    # 4. Save the results to the fuzzy_matching_metrics table
    records_to_upsert = []
    for source_record in clients_and_partners:
        record_client_and_partners = []
        if source_record["key_clients"] is not None:
            record_client_and_partners.extend(source_record["key_clients"])
        if source_record["key_partners"] is not None:
            record_client_and_partners.extend(source_record["key_partners"])
        record = {
            "domain": source_record["domain"],
        }
        for key_ref, column_name in FINAL_COLUMN_MAPPING.items():
            mapping = maps_identified_references[key_ref]
            identified_list = []
            for client_or_partner in list(set(record_client_and_partners)):
                identified_reference = mapping.get(client_or_partner)
                if identified_reference is not None:
                    identified_list.append(
                        f"{identified_reference} ({client_or_partner})"
                    )
            record[column_name] = identified_list

        records_to_upsert.append(record)
    upsert_in_batches(
        get_supabase_client(),
        "business_computed_values",
        records_to_upsert,
        on_conflict="domain",
        logger=logger,
    )
    logger.info(
        f"Upserted {len(records_to_upsert)} records to fuzzy_matching_metrics table"
    )
