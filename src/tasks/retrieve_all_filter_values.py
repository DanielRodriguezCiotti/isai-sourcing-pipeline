import json
from datetime import datetime, timezone

from prefect import task

from src.config.clients import get_supabase_client
from src.config.settings import get_settings
from src.utils.logger import get_logger

TAG_COLUMNS: list[str] = [
    "fund_prime_scope",
    "hq_country",
    "hq_city",
    "gtm_target_cg",
    "gtm_target_by",
    "vc_current_stage",
    "business_model",
    "primary_sector_served_cg",
    "primary_industry_served_cg",
    "primary_sector_served_by",
    "primary_industry_served_by",
    "business_mapping",
    "last_stage_in_attio",
    "last_status_in_attio",
]

MULTITAG_COLUMNS: list[str] = [
    "global_2000_clients",
    "cg_key_platforms",
    "by_key_platforms",
    "competitors_cg",
    "competitors_by",
    "affiliates_cg",
    "affiliates_by",
    "all_investors",
    "last_round_lead_investors",
    "all_industries_served",
    "tech_tags",
]

_VIEW_NAME = "sourcing_view"
_PAGE_SIZE = 1000
_MAX_VALUES_PER_COLUMN = 2000


@task(name="retrieve_all_filter_values")
def retrieve_all_filter_values() -> None:
    logger = get_logger()
    client = get_supabase_client()
    settings = get_settings()

    all_cols = TAG_COLUMNS + MULTITAG_COLUMNS
    select_str = ",".join(all_cols)

    tag_sets: dict[str, set[str]] = {col: set() for col in TAG_COLUMNS}
    multitag_sets: dict[str, set[str]] = {col: set() for col in MULTITAG_COLUMNS}

    offset = 0
    while True:
        resp = (
            client.from_(_VIEW_NAME)
            .select(select_str)
            .range(offset, offset + _PAGE_SIZE - 1)
            .execute()
        )
        page = resp.data
        logger.debug(
            f"retrieve_all_filter_values: fetched page offset={offset}, rows={len(page)}"
        )

        for row in page:
            for col in TAG_COLUMNS:
                if len(tag_sets[col]) >= _MAX_VALUES_PER_COLUMN:
                    continue
                v = row.get(col)
                if v is not None:
                    tag_sets[col].add(v)

            for col in MULTITAG_COLUMNS:
                if len(multitag_sets[col]) >= _MAX_VALUES_PER_COLUMN:
                    continue
                arr = row.get(col)
                if arr:
                    for item in arr:
                        multitag_sets[col].add(item)
                        if len(multitag_sets[col]) >= _MAX_VALUES_PER_COLUMN:
                            break

        if len(page) < _PAGE_SIZE:
            break
        offset += _PAGE_SIZE

    tag_columns = {col: sorted(tag_sets[col]) for col in TAG_COLUMNS}
    multitag_columns = {col: sorted(multitag_sets[col]) for col in MULTITAG_COLUMNS}

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "tag_columns": tag_columns,
        "multitag_columns": multitag_columns,
    }

    bytes_content = json.dumps(payload, ensure_ascii=False).encode("utf-8")

    client.storage.from_(settings.search_resources_bucket_name).upload(
        "search_tag_and_multitag_values.json",
        bytes_content,
        file_options={"upsert": "true"},
    )

    logger.info(
        f"retrieve_all_filter_values: uploaded search_tag_and_multitag_values.json "
        f"to bucket '{settings.search_resources_bucket_name}'"
    )
