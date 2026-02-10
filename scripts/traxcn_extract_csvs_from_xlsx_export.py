import os

import pandas as pd


def load_and_clean_excel(file_path, output_dir="data/traxcn_parsed"):
    """
    Loads an Excel file, filters sheets starting with 'Companies', 'Funding', or 'People',
    uses row 6 as column names, and saves them as CSV files.

    Args:
        file_path (str): Path to the .xlsx file.
        output_dir (str): Directory to save the CSV files.

    Returns:
        dict: Keys are output names (companies, funding, people), values are cleaned DataFrames.
    """

    # Check if file exists
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"The file '{file_path}' was not found.")

    print(f"Loading {file_path}...")

    try:
        # sheet_name=None tells pandas to read ALL sheets into a dictionary
        # header=5 uses row 6 (0-based index) as column names and skips rows 0-5
        all_sheets = pd.read_excel(file_path, sheet_name=None, header=5)
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        return {}

    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    cleaned_sheets = {}
    sheet_prefixes = {
        "Companies": "companies",
        "Funding": "funding",
        "People": "people",
    }

    for sheet_name, df in all_sheets.items():
        # Check if sheet starts with any of the target prefixes
        matching_prefix = None
        for prefix, output_name in sheet_prefixes.items():
            if sheet_name.startswith(prefix):
                matching_prefix = output_name
                break

        # Skip sheets that don't match
        if matching_prefix is None:
            print(f"Skipping sheet: '{sheet_name}'")
            continue

        print(f"Processing sheet: '{sheet_name}'")

        # --- CLEANING STEPS ---

        # 1. Drop rows that are completely empty
        df = df.dropna(how="all", axis=0)

        # 2. Drop columns that are completely empty
        df = df.dropna(how="all", axis=1)

        # 3. Replace newlines within cells with a delimiter
        df = df.map(
            lambda x: (
                x.replace("\n", " ").replace("\r", "") if isinstance(x, str) else x
            )
        )

        # Save to CSV
        output_path = os.path.join(output_dir, f"{matching_prefix}.csv")
        df.to_csv(output_path, index=False)
        print(f"Saved '{matching_prefix}.csv' - Shape: {df.shape}")

        # Add to result dictionary
        cleaned_sheets[matching_prefix] = df

    return cleaned_sheets


# --- USAGE EXAMPLE ---
if __name__ == "__main__":
    # Replace with your actual file path
    input_file = "/Users/danielrodriguez/Downloads/CVC_ALL_SCOPE_TRACXNDB_DEDUP.xlsx"

    try:
        # Process the Excel file and save CSVs
        data_frames = load_and_clean_excel(input_file, output_dir="data/traxcn_csvs")

        print("\n✓ Processing complete!")
        print(f"✓ Processed {len(data_frames)} sheets: {', '.join(data_frames.keys())}")
        print("✓ CSV files saved to: data/traxcn_csvs/")

    except FileNotFoundError as e:
        print(e)
