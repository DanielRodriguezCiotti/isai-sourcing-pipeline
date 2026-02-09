from prefect import flow

from src.tasks.get_companies import get_companies_from_export
from src.tasks.dealroom_enrichment import dealroom_enrich
from src.tasks.save_to_db import save_to_db


@flow(name="etl-pipeline")
def etl_pipeline():
    companies = get_companies_from_export()
    enriched = dealroom_enrich(companies)
    save_to_db(enriched)


if __name__ == "__main__":
    etl_pipeline()
