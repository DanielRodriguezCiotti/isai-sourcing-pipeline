from prefect import flow
from prefect.logging import get_run_logger

from src.tasks import ingest_traxcn_export


@flow(name="traxcn-ingestion-flow")
def traxcn_ingestion_flow(supabase_file_path: str):
    logger = get_run_logger()
    ingest_traxcn_export(supabase_file_path)
    logger.info("Ingestion completed successfully.")


if __name__ == "__main__":
    traxcn_ingestion_flow(
        supabase_file_path="traxcn_export_sample.xlsx",
    )
