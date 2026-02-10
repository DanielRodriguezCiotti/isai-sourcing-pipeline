import json
import os

import pandas as pd
from tqdm import tqdm

ACCEPTED_INVESTMENT_TYPES = {
    "convertible_note",
    "corporate_round",
    "post_ipo_debt",
    "post_ipo_equity",
    "post_ipo_secondary",
    "pre_seed",
    "private_equity",
    "seed",
    "series_a",
    "series_b",
    "series_c",
    "series_d",
    "series_e",
    "series_f",
    "series_g",
    "series_h",
    "series_i",
    "series_j",
    "series_unknown",
    "undisclosed",
}

with open("data/crunchbase_scope/funding_status_per_org.json") as f:
    funding_status_per_org: dict[str, list[str]] = json.load(f)


def filter_organizations(
    input_path: str,
    output_path: str,
    chunk_size: int = 100_000,
):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    total_kept = 0
    header_written = False

    for chunk in tqdm(
        pd.read_csv(
            input_path,
            chunksize=chunk_size,
            dtype=str,
            on_bad_lines="skip",
        ),
        desc="Processing chunks",
    ):
        chunk["funding_status"] = chunk["uuid"].map(funding_status_per_org)
        total_kept += len(
            chunk[chunk["funding_status"].isin(ACCEPTED_INVESTMENT_TYPES)]
        )

        # Append to CSV continuously
        chunk[chunk["funding_status"].isin(ACCEPTED_INVESTMENT_TYPES)].to_csv(
            output_path,
            mode="a",
            header=not header_written,
            index=False,
        )
        header_written = True

    print(f"\nFiltering complete. Total organizations kept: {total_kept}")


if __name__ == "__main__":
    input_csv = "data/crunchbase_scope/organizations_partial_filter.csv"
    output_csv = "data/crunchbase_scope/organizations.csv"
    # Remove the file if it exists
    if os.path.exists(output_csv):
        os.remove(output_csv)
    filter_organizations(input_csv, output_csv)
    # filter_organizations(input_csv, output_csv)
