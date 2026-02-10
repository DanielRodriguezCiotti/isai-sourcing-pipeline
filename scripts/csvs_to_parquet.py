import os

import pandas as pd
from tqdm import tqdm


def csvs_to_parquet(input_files: list[str], output_files: list[str]):
    assert len(input_files) == len(output_files), (
        "Input and output files must have the same length"
    )
    for path in output_files:
        os.makedirs(os.path.dirname(path), exist_ok=True)

    for input_file, output_file in tqdm(
        zip(input_files, output_files), desc="Converting CSV to Parquet"
    ):
        df = pd.read_csv(input_file)
        df.to_parquet(output_file)


if __name__ == "__main__":
    input_files = [
        "data/crunchbase_scope/organizations.csv",
        "data/crunchbase_scope/funding_rounds.csv",
        "data/crunchbase_scope/people.csv",
        "data/traxcn_csvs/companies.csv",
        "data/traxcn_csvs/funding.csv",
        "data/traxcn_csvs/people.csv",
    ]
    output_files = [
        "data/crunchbase_scope_parquet/organizations.parquet",
        "data/crunchbase_scope_parquet/funding_rounds.parquet",
        "data/crunchbase_scope_parquet/people.parquet",
        "data/traxcn_csvs_parquet/companies.parquet",
        "data/traxcn_csvs_parquet/funding.parquet",
        "data/traxcn_csvs_parquet/people.parquet",
    ]
    csvs_to_parquet(input_files, output_files)
