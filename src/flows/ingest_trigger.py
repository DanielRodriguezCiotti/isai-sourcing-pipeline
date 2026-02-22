from prefect import flow
from prefect.context import get_run_context

from src.tasks import ingest_traxcn_export, schedule_pipeline_runs


@flow(name="ingest-trigger-flow", timeout_seconds=1800)  # 30 minutes
def ingest_trigger_flow(supabase_file_path: str):

    domains = ingest_traxcn_export(supabase_file_path)
    flow_run_name = get_run_context().flow_run.name
    schedule_pipeline_runs(domains, flow_run_name)
