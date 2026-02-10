import os

import pandas as pd
from tqdm import tqdm

DISCARDED_COUNTRIES = {
    "AGO",
    "BDI",
    "BEN",
    "BFA",
    "BWA",
    "CAF",
    "CHN",
    "CIV",
    "CMR",
    "COD",
    "COG",
    "COM",
    "CPV",
    "DJI",
    "DZA",
    "EGY",
    "ERI",
    "ESH",
    "ETH",
    "GAB",
    "GHA",
    "GIN",
    "GMB",
    "GNB",
    "GNQ",
    "KEN",
    "LBR",
    "LBY",
    "LSO",
    "MAR",
    "MDG",
    "MLI",
    "MOZ",
    "MRT",
    "MUS",
    "MWI",
    "MYT",
    "NAM",
    "NER",
    "NGA",
    "REU",
    "RWA",
    "SDN",
    "SEN",
    "SLE",
    "SOM",
    "STP",
    "SWZ",
    "SYC",
    "TAN",
    "TCD",
    "TGO",
    "TUN",
    "UGA",
    "ZAF",
    "ZMB",
    "ZWE",
}

ACCEPTED_INDUSTRY_GROUPS = {
    "Software",
    "Real Estate",
    "Data and Analytics",
    "Energy",
    "Platforms",
    "Blockchain and Cryptocurrency",
    "Financial Services",
}

ACCEPTED_CATEGORIES = {
    "Geospatial",
    "GPS",
    "Indoor Positioning",
    "Location Based Services",
    "Mapping Services",
    "Navigation",
    "Water",
    "Timber",
    "Natural Resources",
    "Mining Technology",
    "Mining",
    "Mineral",
    "3D Printing",
    "Advanced Materials",
    "Foundries",
    "Industrial",
    "Industrial Automation",
    "Industrial Engineering",
    "Industrial Manufacturing",
    "Machinery Manufacturing",
    "Manufacturing",
    "Wood Processing",
    "Visual Search",
    "Vertical Search",
    "Semantic Search",
    "Search Engine",
    "Product Search",
    "Internet of Things",
    "Cloud Infrastructure",
    "Business Information Systems",
    "Cloud Data Services",
    "Cloud Management",
    "Cloud Security",
    "CMS",
    "Contact Management",
    "CRM",
    "Cyber Security",
    "Data Center",
    "Data Center Automation",
    "Data Integration",
    "Data Mining",
    "Data Visualization",
    "DevOps",
    "Document Management",
    "GovTech",
    "Identity Management",
    "Information and Communications Technology (ICT)",
    "Information Services",
    "Information Technology",
    "Intrusion Detection",
    "IT Infrastructure",
    "IT Management",
    "Management Information Systems",
    "Military",
    "Network Security",
    "Penetration Testing",
    "Private Cloud",
    "Reputation",
    "Sales Automation",
    "Scheduling",
    "Social CRM",
    "Spam Filtering",
    "Technical Support",
    "Unified Communications",
    "Virtualization",
    "VoIP",
    "Assistive Technology",
    "Biopharma",
    "Clinical Trials",
    "Electronic Health Record (EHR)",
    "Medical",
    "Pharmaceutical",
    "Precision Medicine",
    "Telehealth",
    "Government",
    "Law Enforcement",
    "National Security",
    "Public Safety",
    "Asset Management",
    "Auto Insurance",
    "Banking",
    "Bitcoin",
    "Bookkeeping and Payroll",
    "Commercial Insurance",
    "Consumer Lending",
    "Credit",
    "Credit Bureau",
    "Cryptocurrency",
    "Debt Collections",
    "Finance",
    "Financial Exchanges",
    "Financial Services",
    "FinTech",
    "Foreign Exchange Trading",
    "Fraud Detection",
    "Health Insurance",
    "Impact Investing",
    "Insurance",
    "InsurTech",
    "Leasing",
    "Lending",
    "Life Insurance",
    "Mobile Payments",
    "Payments",
    "Prediction Markets",
    "Property Insurance",
    "Real Estate Investment",
    "Stock Exchanges",
    "Transaction Processing",
    "Virtual Currency",
    "Wealth Management",
    "Biomass Energy",
    "Clean Energy",
    "Electrical Distribution",
    "Energy",
    "Energy Efficiency",
    "Energy Management",
    "Energy Storage",
    "Geothermal Energy",
    "Hydroelectric",
    "Power Grid",
    "Renewable Energy",
    "Solar",
    "Wind Energy",
    "Web Design",
    "UX Design",
    "Usability Testing",
    "Product Research",
    "Product Design",
    "Mechanical Design",
    "Market Research",
    "Interior Design",
    "Industrial Design",
    "Human Computer Interaction",
    "Graphic Design",
    "Consumer Research",
    "Drones",
    "Intelligent Systems",
    "Blockchain",
    "E-Commerce",
    "Foundational AI",
    "Agentic AI",
    "Smart Contracts",
    "Personalization",
    "Machine Learning",
    "Natural Language Processing",
    "Generative AI",
    "Retail Technology",
    "Robotic Process Automation (RPA)",
    "AI Infrastructure",
    "E-Commerce Platforms",
    "Artificial Intelligence (AI)",
    "Decentralized Finance (DeFi)",
    "Non-Fungible Token (NFT)",
    "Predictive Analytics",
    "Web3",
}


def has_overlap(cell_value: str, accepted: set) -> bool:
    """Check if a comma-separated cell has at least one value in the accepted set."""
    if not isinstance(cell_value, str):
        return False
    items = {item.strip() for item in cell_value.split(",")}
    return bool(items & accepted)


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
        # Strip whitespace from column names
        chunk.columns = chunk.columns.str.strip()

        # 1. primary_role == "company"
        mask = chunk["primary_role"].str.strip() == "company"

        # 2. status == "operating"
        mask &= chunk["status"].str.strip().isin(["operating", "acquired", "ipo"])

        # 3. country_code NOT in discarded list
        mask &= ~chunk["country_code"].str.strip().isin(DISCARDED_COUNTRIES)

        # 3. founded_on not null and after 2012
        founded = pd.to_datetime(chunk["founded_on"], errors="coerce")
        mask &= founded.notna() & (founded.dt.year > 2012)

        # 4. category_groups_list has at least one accepted industry group
        mask &= chunk["category_groups_list"].apply(
            lambda v: has_overlap(v, ACCEPTED_INDUSTRY_GROUPS)
        )

        # 5. category_list has at least one accepted category
        mask &= chunk["category_list"].apply(
            lambda v: has_overlap(v, ACCEPTED_CATEGORIES)
        )

        # 6. domain is not null and unique
        mask &= chunk["domain"].notna()

        filtered = chunk[mask]
        total_kept += len(filtered)

        # Append to CSV continuously
        filtered.to_csv(
            output_path,
            mode="a",
            header=not header_written,
            index=False,
        )
        header_written = True

    print(f"\nFiltering complete. Total organizations kept: {total_kept}")


if __name__ == "__main__":
    input_csv = "/Users/danielrodriguez/Desktop/projects/ISAI/crunchbase_bulk_export/organizations.csv"
    output_csv = "data/crunchbase_scope/organizations_partial_filter.csv"
    # Remove the file if it exists
    if os.path.exists(output_csv):
        os.remove(output_csv)
    filter_organizations(input_csv, output_csv)
