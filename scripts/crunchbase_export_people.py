import os

import pandas as pd
from tqdm import tqdm

organizations_csv = "data/crunchbase_scope/organizations.csv"
org_uuids = pd.read_csv(organizations_csv)["uuid"].tolist()


def filter_people(
    input_path: str,
    output_path: str,
    chunk_size: int = 100_000,
):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    total_kept = 0
    total = 0
    header_written = False
    people_uuids = []

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
        mask = chunk["featured_job_organization_uuid"].isin(org_uuids)
        filtered = chunk[mask]
        total_kept += len(filtered)
        people_uuids.extend(filtered["uuid"].tolist())
        # Append to CSV continuously
        filtered.to_csv(
            output_path,
            mode="a",
            header=not header_written,
            index=False,
        )
        header_written = True

    print(f"\nFiltering complete. Total people kept: {total_kept} out of {total}")
    return people_uuids


def retrieve_people_descriptions(
    people_uuids: list[str], people_description_csv: str, chunk_size: int = 100_000
) -> list[str]:

    people_descriptions = {}

    for chunk in tqdm(
        pd.read_csv(
            people_description_csv,
            chunksize=chunk_size,
            dtype=str,
            usecols=["uuid", "description"],
            on_bad_lines="skip",
        ),
        desc="Processing chunks",
    ):
        mask = chunk["uuid"].isin(people_uuids)
        filtered = chunk[mask]
        people_descriptions.update(dict(zip(filtered["uuid"], filtered["description"])))

    return people_descriptions


def add_people_descriptions(
    people_csv: str, people_descriptions: dict[str, str], output_path: str
) -> pd.DataFrame:
    people = pd.read_csv(people_csv)
    people["description"] = people["uuid"].map(people_descriptions)
    people.to_csv(output_path, index=False)


if __name__ == "__main__":
    input_csv = (
        "/Users/danielrodriguez/Desktop/projects/ISAI/crunchbase_bulk_export/people.csv"
    )
    people_description_csv = "/Users/danielrodriguez/Desktop/projects/ISAI/crunchbase_bulk_export/people_descriptions.csv"
    intermediate_csv = "data/crunchbase_scope/people_intermediate.csv"
    output_csv = "data/crunchbase_scope/people.csv"
    # Remove the file if it exists
    if os.path.exists(output_csv):
        os.remove(output_csv)
    if os.path.exists(intermediate_csv):
        os.remove(intermediate_csv)
    people_uuids = filter_people(input_csv, intermediate_csv)
    people_descriptions = retrieve_people_descriptions(
        people_uuids, people_description_csv
    )
    add_people_descriptions(intermediate_csv, people_descriptions, output_csv)
    # filter_organizations(input_csv, output_csv)
