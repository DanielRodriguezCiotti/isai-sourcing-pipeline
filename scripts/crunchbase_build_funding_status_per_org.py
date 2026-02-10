import json
import os
from collections import defaultdict

import pandas as pd
from tqdm import tqdm


def build_funding_status_per_org(
    input_path: str,
    output_path: str,
    chunk_size: int = 100_000,
):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    org_investment_types: dict[str, dict[str, str]] = defaultdict(dict)

    for chunk in tqdm(
        pd.read_csv(
            input_path,
            chunksize=chunk_size,
            dtype=str,
            usecols=["org_uuid", "announced_on", "investment_type"],
            on_bad_lines="skip",
        ),
        desc="Processing chunks",
    ):
        chunk.columns = chunk.columns.str.strip()
        chunk = chunk.dropna(subset=["org_uuid", "announced_on", "investment_type"])

        for org_uuid, announced_on, inv_type in zip(
            chunk["org_uuid"], chunk["announced_on"], chunk["investment_type"]
        ):
            org_investment_types[org_uuid.strip()][announced_on.strip()] = (
                inv_type.strip()
            )

    # Keep the latest investment type for each org
    def retrieve_latest_investment_type(inv_types: dict[str, str]) -> str:
        dates = inv_types.keys()
        latest_date = max(dates)
        print(f"latest date of {dates} is {latest_date}")
        return inv_types[latest_date]

    result = {
        org_id: retrieve_latest_investment_type(inv_types)
        for org_id, inv_types in org_investment_types.items()
    }

    # Save to JSON
    with open(output_path, "w") as f:
        json.dump(result, f, indent=2)

    print(f"\nDone. {len(result)} orgs written to {output_path}")


if __name__ == "__main__":
    # input_csv = "/Users/danielrodriguez/Desktop/projects/ISAI/crunchbase_bulk_export/funding_rounds.csv"
    output_json = "data/crunchbase_scope/funding_status_per_org.json"
    # build_funding_status_per_org(input_csv, output_json)
    json_data = json.load(open(output_json))
    unique_investment_types = set(json_data.values())
    print(f"unique investment types: {unique_investment_types}")
