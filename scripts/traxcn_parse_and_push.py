"""
Script to parse TraxCN CSV files and push records to Supabase.
Processes: companies.csv, funding.csv, and people.csv
"""

import math
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import pandas as pd
from supabase import Client, create_client

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BATCH_SIZE = 10


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
    Parse dates in various formats and return YYYY-MM-DD (ISO) format for Postgres.
    Handles:
    - Oct 16, 2019 -> 2019-10-16
    - 2015 -> 2015-01-01
    - Jan 2023 -> 2023-01-01
    - 2019-10-16 -> 2019-10-16
    """
    if pd.isna(value) or value == "" or value is None:
        return None

    value_str = str(value).strip()

    # Handle year only (e.g., "2015")
    if re.match(r"^\d{4}$", value_str):
        return f"{value_str}-01-01"

    # Handle "Month Year" format (e.g., "Jan 2023")
    month_year_match = re.match(r"^([A-Za-z]{3})\s+(\d{4})$", value_str)
    if month_year_match:
        month_str, year = month_year_match.groups()
        try:
            date_obj = datetime.strptime(f"{month_str} {year}", "%b %Y")
            return f"{year}-{date_obj.month:02d}-01"
        except ValueError:
            return None

    # Handle "Month Day, Year" format (e.g., "Oct 16, 2019")
    try:
        date_obj = datetime.strptime(value_str, "%b %d, %Y")
        return date_obj.strftime("%Y-%m-%d")
    except ValueError:
        pass

    # Handle ISO format (e.g., "2019-10-16")
    try:
        date_obj = datetime.strptime(value_str, "%Y-%m-%d")
        return date_obj.strftime("%Y-%m-%d")
    except ValueError:
        pass

    return None


def parse_column_names(columns: list[str], csv_type: str) -> list[str]:
    """
    Parse column names according to rules:
    - To lower
    - Replace spaces by _
    - Remove things between () except for (USD) that becomes 'in_usd'
    - Remove all '
    - For companies: remove ': TRUE' in Special Flags col
    """
    parsed_columns = []

    for col in columns:
        parsed_col = col.lower()
        parsed_col = parsed_col.replace("'", "")
        parsed_col = parsed_col.replace("-", "_")

        if "(usd)" in parsed_col:
            parsed_col = parsed_col.replace("(usd)", "in_usd")
        parsed_col = re.sub(r"\([^)]*\)", "", parsed_col)

        if csv_type == "companies" and ": true" in parsed_col:
            parsed_col = parsed_col.replace(": true", "")

        parsed_col = parsed_col.replace(" ", "_")
        parsed_col = re.sub(r"_+", "_", parsed_col)
        parsed_col = parsed_col.strip("_")

        parsed_columns.append(parsed_col)

    return parsed_columns


def parse_people_csv(input_path: Path) -> pd.DataFrame:
    """Parse people.csv according to specified rules."""
    df = pd.read_csv(input_path)
    df.columns = parse_column_names(df.columns.tolist(), "people")

    if "sno." in df.columns:
        df = df.drop(columns=["sno."])

    if "emails" in df.columns:
        df["emails"] = df["emails"].apply(
            lambda x: (
                [e.strip() for e in str(x).split()] if pd.notna(x) and x != "" else None
            )
        )

    return df


def parse_companies_csv(input_path: Path) -> pd.DataFrame:
    """Parse companies.csv according to specified rules."""
    df = pd.read_csv(input_path)
    df.columns = parse_column_names(df.columns.tolist(), "companies")

    cols_to_drop = ["sno.", "soonicorn_club_status", "soonicorn_club_event_date"]
    df = df.drop(columns=[col for col in cols_to_drop if col in df.columns])

    if "founded_year" in df.columns:
        df["founded_year"] = pd.to_numeric(df["founded_year"], errors="coerce").astype(
            "Int64"
        )

    comma_list_cols = [
        "sector",
        "business_models",
        "waves",
        "trending_themes",
        "special_flags",
        "institutional_investors",
        "angel_investors",
        "key_people_email_ids",
        "links_to_key_people_profiles",
        "acquisition_list",
        "company_emails",
    ]

    for col in comma_list_cols:
        if col in df.columns:
            df[col] = df[col].apply(comma2list)

    bool_cols = ["is_funded", "is_deadpooled", "is_acquired", "is_ipo"]
    for col in bool_cols:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda x: (
                    True
                    if str(x).lower() in ["yes", "true", "1"]
                    else False
                    if str(x).lower() in ["no", "false", "0"]
                    else None
                )
            )

    date_cols = [
        "latest_funded_date",
        "editors_rated_date",
        "website_status_last_updated",
        "date_added",
    ]

    for col in date_cols:
        if col in df.columns:
            df[col] = df[col].apply(parsedate)

    return df


def parse_funding_csv(input_path: Path) -> pd.DataFrame:
    """Parse funding.csv according to specified rules."""
    df = pd.read_csv(input_path)
    df.columns = parse_column_names(df.columns.tolist(), "funding")

    if "sno." in df.columns:
        df = df.drop(columns=["sno."])

    if "round_date" in df.columns:
        df["round_date"] = df["round_date"].apply(parsedate)

    if "round_amount_in_usd" in df.columns:

        def parse_amount(value):
            if pd.isna(value) or value == "" or value is None:
                return None
            try:
                value_str = str(value).split(".")[0].replace(",", "")
                return int(value_str)
            except (ValueError, AttributeError):
                return None

        df["round_amount_in_usd"] = df["round_amount_in_usd"].apply(parse_amount)

    if "founded_year" in df.columns:
        df["founded_year"] = pd.to_numeric(df["founded_year"], errors="coerce").astype(
            "Int64"
        )

    comma_list_cols = [
        "institutional_investors",
        "angel_investors",
        "lead_investor",
        "facilitators",
        "practice_areas",
        "feed_name",
        "business_models",
    ]

    for col in comma_list_cols:
        if col in df.columns:
            df[col] = df[col].apply(comma2list)

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
    """Parse all TraxCN CSV files and push to Supabase."""
    base_path = Path(__file__).parent.parent
    input_dir = base_path / "data" / "traxcn_csvs"

    client = get_supabase_client()

    print("Starting TraxCN CSV parse and push...\n")

    # --- Companies (must go first due to FK constraints) ---
    companies_input = input_dir / "companies.csv"
    if companies_input.exists():
        print(f"Parsing {companies_input.name}...")
        companies_df = parse_companies_csv(companies_input)
        print(f"  Rows: {len(companies_df)}, Columns: {len(companies_df.columns)}")
        push_to_supabase(
            client,
            "traxcn_companies",
            companies_df,
            upsert_on_conflict="domain_name",
        )
    else:
        print(f"  {companies_input.name} not found, skipping")

    # --- Funding rounds ---
    funding_input = input_dir / "funding.csv"
    if funding_input.exists():
        print(f"\nParsing {funding_input.name}...")
        funding_df = parse_funding_csv(funding_input)
        print(f"  Rows: {len(funding_df)}, Columns: {len(funding_df.columns)}")
        push_to_supabase(client, "traxcn_funding_rounds", funding_df)
    else:
        print(f"  {funding_input.name} not found, skipping")

    # --- People / Founders ---
    people_input = input_dir / "people.csv"
    if people_input.exists():
        print(f"\nParsing {people_input.name}...")
        people_df = parse_people_csv(people_input)
        print(f"  Rows: {len(people_df)}, Columns: {len(people_df.columns)}")
        push_to_supabase(client, "traxcn_founders", people_df)
    else:
        print(f"  {people_input.name} not found, skipping")

    print("\nAll done!")


if __name__ == "__main__":
    main()
