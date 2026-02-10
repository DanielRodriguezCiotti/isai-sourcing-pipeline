import pandas as pd
from prefect import task
from prefect.logging import get_run_logger

from src.config.clients import get_supabase_client
from src.config.settings import get_settings

logger = get_run_logger()


def load_traxcn_export(supabase_file_path: str) -> bytes:
    client = get_supabase_client()
    file = client.storage.from_(get_settings().traxcn_exports_bucket_name).download(
        supabase_file_path
    )
    if file is None:
        raise FileNotFoundError(f"The file '{supabase_file_path}' was not found.")
    return file


def extract_domains_from_companies_sheet(file: bytes) -> list[str]:
    """
    Extracts domains from the Companies sheet of an Excel file.
    """
    all_sheets = pd.read_excel(file, sheet_name=None, header=5)
    prefix = "Companies"

    for sheet_name, df in all_sheets.items():
        # Check if sheet starts with any of the target prefixes
        if sheet_name.startswith(prefix):
            logger.info("Found Companies sheet")
            df.dropna(subset=["Domain Name"], inplace=True)
            df.drop_duplicates(subset=["Domain Name"], inplace=True)
            return df["Domain Name"].tolist()


def determine_db_modifications(domains: list[str]) -> dict[str, int | list[str]]:
    """
    Determines the modifications to be made to the database.
    """
    client = get_supabase_client()
    existing_domains = set(
        client.table("traxcn_companies").select("domain_name").in_("domains").execute()
    )
    nb_existing_domains = len(existing_domains)
    new_domains = set(domains) - existing_domains
    nb_new_domains = len(new_domains)
    return {
        "number_of_companies_to_add": nb_new_domains,
        "number_of_companies_to_update": nb_existing_domains,
        "new_domains": list(new_domains),
        "existing_domains": list(existing_domains),
    }


@task(name="assess_db_modifications_from_traxcn_export")
def assess_db_modifications_from_traxcn_export(
    supabase_file_path: str,
) -> dict[str, int | list[str]]:
    file = load_traxcn_export(supabase_file_path)
    domains = extract_domains_from_companies_sheet(file)
    return determine_db_modifications(domains)
