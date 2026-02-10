from typing import Literal

from prefect import flow
from prefect.flow_runs import pause_flow_run
from prefect.input import RunInput
from pydantic import Field

from src.tasks import assess_db_modifications_from_traxcn_export, ingest_traxcn_export


class IngestionConfirmation(RunInput):
    mode: Literal["insert", "upsert"] = Field(
        ..., description="The mode of the ingestion"
    )
    response: Literal["yes", "no"] = Field(
        ..., description="Whether to proceed with the ingestion"
    )


@flow(name="traxcn-ingestion-flow")
async def traxcn_ingestion_flow(supabase_file_path: str):

    db_modifications = assess_db_modifications_from_traxcn_export(supabase_file_path)
    md_message = f"Among the provided export there are {db_modifications['number_of_companies_to_add']} new companies and {db_modifications['number_of_companies_to_update']} companies to update."
    confirmation = await pause_flow_run(
        wait_for_input=IngestionConfirmation.with_initial_data(md_message)
    )
    if confirmation.response == "yes":
        if confirmation.mode == "insert":
            ingest_traxcn_export(supabase_file_path, db_modifications["new_domains"])
        elif confirmation.mode == "upsert":
            ingest_traxcn_export(
                supabase_file_path,
                db_modifications["existing_domains"] + db_modifications["new_domains"],
            )
        else:
            raise ValueError("Invalid mode.")
    else:
        raise ValueError("Ingestion cancelled.")


if __name__ == "__main__":
    import asyncio

    asyncio.run(
        traxcn_ingestion_flow(supabase_file_path="TracxnExport-Feb-05-2026.xlsx")
    )
