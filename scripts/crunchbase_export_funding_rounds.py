import os

import pandas as pd
from tqdm import tqdm

organizations_csv = "data/crunchbase_scope/organizations.csv"
org_uuids = pd.read_csv(organizations_csv)["uuid"].tolist()

inverstors_csv = (
    "/Users/danielrodriguez/Desktop/projects/ISAI/crunchbase_bulk_export/investors.csv"
)
investors = pd.read_csv(inverstors_csv)
investors_map = dict(zip(investors["uuid"], investors["name"]))


def assign_investor_name(investor_uuid: str) -> str:
    if pd.isna(investor_uuid):
        return None
    list_of_investors = investor_uuid.strip().split(",")
    list_of_investor_names = [investors_map[investor] for investor in list_of_investors]
    return list_of_investor_names


def filter_funding_rounds(
    input_path: str,
    output_path: str,
    chunk_size: int = 100_000,
):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    total_kept = 0
    total = 0
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
        total += len(chunk)
        mask = chunk["org_uuid"].isin(org_uuids)
        filtered = chunk[mask]
        total_kept += len(filtered)
        filtered["lead_investor_names"] = filtered["lead_investor_uuids"].apply(
            assign_investor_name
        )
        # Append to CSV continuously
        filtered.to_csv(
            output_path,
            mode="a",
            header=not header_written,
            index=False,
        )
        header_written = True

    print(
        f"\nFiltering complete. Total funding rounds kept: {total_kept} out of {total}"
    )


if __name__ == "__main__":
    input_csv = "/Users/danielrodriguez/Desktop/projects/ISAI/crunchbase_bulk_export/funding_rounds.csv"
    output_csv = "data/crunchbase_scope/funding_rounds.csv"
    # Remove the file if it exists
    if os.path.exists(output_csv):
        os.remove(output_csv)
    filter_funding_rounds(input_csv, output_csv)
    # filter_organizations(input_csv, output_csv)
