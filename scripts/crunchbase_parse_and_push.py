"""
Script to parse Crunchbase CSV files and push records to Supabase.
Processes: organizations.csv (companies), funding_rounds.csv, and people.csv (founders)
"""

import math
import os
from pathlib import Path
from typing import Any, Optional

import pandas as pd
from supabase import Client, create_client

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BATCH_SIZE = 1000


def get_supabase_client() -> Client:
    """Create and return a Supabase client using environment variables."""
    url = os.environ["SUPABASE_URL"]
    key = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
    return create_client(url, key)


def comma2list(value: Any) -> Optional[list[str]]:
    """
    Turns string containing commas into a list.
    Returns None if value is empty/nan.
    """
    if pd.isna(value) or value == "" or value is None:
        return None
    if isinstance(value, str):
        items = [item.strip() for item in value.split(",") if item.strip()]
        return items if items else None
    return None


def parsedate(value: Any) -> Optional[str]:
    """
    Parse ISO dates (YYYY-MM-DD) and return as-is for Postgres.
    Returns None for empty/invalid values.
    """
    if pd.isna(value) or value == "" or value is None:
        return None
    value_str = str(value).strip()
    # Crunchbase dates are already in YYYY-MM-DD format
    if len(value_str) >= 10:
        return value_str[:10]
    return None


# ---------------------------------------------------------------------------
# Per-file parsers
# ---------------------------------------------------------------------------


def parse_organizations_csv(input_path: Path) -> tuple[pd.DataFrame, set[str]]:
    """Parse organizations.csv and map to crunchbase_companies schema.

    Returns a tuple of (cleaned DataFrame, set of excluded crunchbase_ids)
    where rows with duplicate domains have been removed entirely.
    """
    df = pd.read_csv(input_path, low_memory=False)

    # Rename uuid -> crunchbase_id
    df = df.rename(columns={"uuid": "crunchbase_id"})

    # Keep only columns that map to the DB schema
    keep = [
        "crunchbase_id",
        "name",
        "legal_name",
        "domain",
        "homepage_url",
        "country_code",
        "state_code",
        "region",
        "city",
        "address",
        "postal_code",
        "status",
        "short_description",
        "category_list",
        "category_groups_list",
        "num_funding_rounds",
        "total_funding_usd",
        "founded_on",
        "last_funding_on",
        "email",
        "phone",
        "facebook_url",
        "linkedin_url",
        "twitter_url",
        "logo_url",
    ]
    df = df[[col for col in keep if col in df.columns]]

    # Arrays
    for col in ["category_list", "category_groups_list"]:
        if col in df.columns:
            df[col] = df[col].apply(comma2list)

    # Numerics
    if "num_funding_rounds" in df.columns:
        df["num_funding_rounds"] = pd.to_numeric(
            df["num_funding_rounds"], errors="coerce"
        ).astype("Int64")

    if "total_funding_usd" in df.columns:
        df["total_funding_usd"] = pd.to_numeric(
            df["total_funding_usd"], errors="coerce"
        )

    # Dates
    for col in ["founded_on", "last_funding_on"]:
        if col in df.columns:
            df[col] = df[col].apply(parsedate)

    # Deduplicate: drop ALL rows that share a duplicate domain
    dup_mask = df["domain"].notna() & df.duplicated(subset="domain", keep=False)
    excluded_ids = set(df.loc[dup_mask, "crunchbase_id"].tolist())
    df = df[~dup_mask]

    return df, excluded_ids


def parse_funding_rounds_csv(input_path: Path) -> pd.DataFrame:
    """Parse funding_rounds.csv and map to crunchbase_funding_rounds schema."""
    df = pd.read_csv(input_path, low_memory=False)

    # Rename org_uuid -> crunchbase_company_uuid, lead_investor_names -> lead_investors
    df = df.rename(
        columns={
            "org_uuid": "crunchbase_company_uuid",
            "lead_investor_names": "lead_investors",
        }
    )

    keep = [
        "crunchbase_company_uuid",
        "name",
        "investment_type",
        "announced_on",
        "raised_amount_usd",
        "post_money_valuation_usd",
        "investor_count",
        "lead_investors",
    ]
    df = df[[col for col in keep if col in df.columns]]

    # Dates
    if "announced_on" in df.columns:
        df["announced_on"] = df["announced_on"].apply(parsedate)

    # Numerics
    for col in ["raised_amount_usd", "post_money_valuation_usd"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "investor_count" in df.columns:
        df["investor_count"] = pd.to_numeric(
            df["investor_count"], errors="coerce"
        ).astype("Int64")

    # Arrays
    if "lead_investors" in df.columns:
        # turn "['123', '456']" into ["123", "456"]
        df["lead_investors"] = df["lead_investors"].apply(
            lambda x: x.strip("[]").split(",") if isinstance(x, str) else None
        )

    return df


def parse_people_csv(input_path: Path) -> pd.DataFrame:
    """Parse people.csv and map to crunchbase_founders schema."""
    df = pd.read_csv(input_path, low_memory=False)

    # Rename featured_job_organization_uuid -> crunchbase_company_uuid
    df = df.rename(
        columns={
            "featured_job_organization_uuid": "crunchbase_company_uuid",
            "featured_job_title": "job_title",
        }
    )

    keep = [
        "crunchbase_company_uuid",
        "name",
        "first_name",
        "last_name",
        "gender",
        "job_title",
        "facebook_url",
        "linkedin_url",
        "twitter_url",
        "description",
    ]
    df = df[[col for col in keep if col in df.columns]]

    # Drop rows with no company link
    if "crunchbase_company_uuid" in df.columns:
        df = df[df["crunchbase_company_uuid"].notna()]

    return df


# ---------------------------------------------------------------------------
# Supabase push helpers
# ---------------------------------------------------------------------------


def clean_row(row: dict) -> dict:
    """Clean a row dict for Supabase: convert NaN/NaT/pd.NA to None."""
    cleaned = {}
    for key, value in row.items():
        if isinstance(value, float) and math.isnan(value):
            cleaned[key] = None
        elif isinstance(value, list):
            cleaned[key] = [item for item in value if item is not None]
        elif pd.isna(value):
            cleaned[key] = None
        else:
            cleaned[key] = value
    return cleaned


def push_to_supabase(
    client: Client,
    table_name: str,
    df: pd.DataFrame,
    batch_size: int = BATCH_SIZE,
    upsert_on_conflict: Optional[str] = None,
) -> None:
    """Push DataFrame records to a Supabase table in batches."""
    records = [clean_row(row) for row in df.to_dict(orient="records")]
    total = len(records)
    total_batches = math.ceil(total / batch_size)

    print(f"  Pushing {total} records to '{table_name}' (batch size: {batch_size})...")

    for i in range(0, total, batch_size):
        batch = records[i : i + batch_size]
        batch_num = i // batch_size + 1

        if upsert_on_conflict:
            client.table(table_name).upsert(
                batch, on_conflict=upsert_on_conflict
            ).execute()
        else:
            client.table(table_name).insert(batch).execute()

        print(f"    Batch {batch_num}/{total_batches} ({len(batch)} records)")

    print(f"  Done: {total} records pushed to '{table_name}'")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    """Parse all Crunchbase CSV files and push to Supabase."""
    base_path = Path(__file__).parent.parent
    input_dir = base_path / "data" / "crunchbase_scope"

    client = get_supabase_client()

    print("Starting Crunchbase CSV parse and push...\n")

    # --- Companies (must go first due to FK constraints) ---
    # If domain already exists, skip
    valid_crunchbase_ids: set[str] = set()
    orgs_input = input_dir / "organizations.csv"
    if orgs_input.exists():
        print(f"Parsing {orgs_input.name}...")
        orgs_df, excluded_ids = parse_organizations_csv(orgs_input)

        # Log duplicate-domain summary
        if excluded_ids:
            # Re-read just to get the duplicate domain values for logging
            raw_df = pd.read_csv(
                orgs_input, low_memory=False, usecols=["uuid", "domain"]
            )
            raw_df = raw_df.rename(columns={"uuid": "crunchbase_id"})
            dup_domains = sorted(
                raw_df.loc[
                    raw_df["crunchbase_id"].isin(excluded_ids)
                    & raw_df["domain"].notna(),
                    "domain",
                ].unique()
            )
            print(f"  Duplicate domains found: {len(dup_domains)}")
            print(f"  Rows dropped (all copies): {len(excluded_ids)}")
            display = dup_domains[:50]
            print(f"  Domains: {display}")
            if len(dup_domains) > 50:
                print(f"  ... and {len(dup_domains) - 50} more")

        valid_crunchbase_ids = set(orgs_df["crunchbase_id"].tolist())
        print(f"  Rows: {len(orgs_df)}, Columns: {len(orgs_df.columns)}")
        push_to_supabase(
            client,
            "crunchbase_companies",
            orgs_df,
            upsert_on_conflict="crunchbase_id",
        )
    else:
        print(f"  {orgs_input.name} not found, skipping")

    # --- Funding rounds ---
    funding_input = input_dir / "funding_rounds.csv"
    if funding_input.exists():
        print(f"\nParsing {funding_input.name}...")
        funding_df = parse_funding_rounds_csv(funding_input)
        if valid_crunchbase_ids:
            before = len(funding_df)
            funding_df = funding_df[
                funding_df["crunchbase_company_uuid"].isin(valid_crunchbase_ids)
            ]
            skipped = before - len(funding_df)
            if skipped:
                print(
                    f"  Filtered out {skipped} funding rounds linked to excluded companies"
                )
            # drop duplicates on (announced_on, crunchbase_company_uuid, investment_type)
            funding_df_after_duplicates = funding_df.drop_duplicates(
                subset=["announced_on", "crunchbase_company_uuid", "investment_type"]
            )
            if len(funding_df_after_duplicates) != len(funding_df):
                print(
                    f"  Dropped {len(funding_df) - len(funding_df_after_duplicates)} duplicates"
                )
            funding_df = funding_df_after_duplicates
        print(f"  Rows: {len(funding_df)}, Columns: {len(funding_df.columns)}")
        push_to_supabase(client, "crunchbase_funding_rounds", funding_df)
    else:
        print(f"  {funding_input.name} not found, skipping")

    # --- People / Founders ---
    people_input = input_dir / "people.csv"
    if people_input.exists():
        print(f"\nParsing {people_input.name}...")
        people_df = parse_people_csv(people_input)
        if valid_crunchbase_ids:
            before = len(people_df)
            people_df = people_df[
                people_df["crunchbase_company_uuid"].isin(valid_crunchbase_ids)
            ]
            skipped = before - len(people_df)
            if skipped:
                print(f"  Filtered out {skipped} founders linked to excluded companies")

            # drop duplicates on (crunchbase_company_uuid, name, job_title)
            people_df_after_duplicates = people_df.drop_duplicates(
                subset=["crunchbase_company_uuid", "name", "job_title"]
            )
            if len(people_df_after_duplicates) != len(people_df):
                print(
                    f"  Dropped {len(people_df) - len(people_df_after_duplicates)} duplicates"
                )
            people_df = people_df_after_duplicates
        print(f"  Rows: {len(people_df)}, Columns: {len(people_df.columns)}")
        push_to_supabase(client, "crunchbase_founders", people_df)
    else:
        print(f"  {people_input.name} not found, skipping")

    print("\nAll done!")


if __name__ == "__main__":
    main()
