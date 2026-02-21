from datetime import datetime, timedelta, timezone

from prefect import flow
from prefect.deployments import run_deployment
from prefect.logging import get_run_logger

from src.config.settings import get_settings
from src.tasks import ingest_traxcn_export


@flow(name="ingest-trigger-flow")
def ingest_trigger_flow(supabase_file_path: str):
    logger = get_run_logger()
    settings = get_settings()
    domains = ingest_traxcn_export(supabase_file_path)

    # Split into batches
    batches = [
        domains[i : i + settings.batch_size]
        for i in range(0, len(domains), settings.batch_size)
    ]

    logger.info(
        f"Ingested {len(domains)} domains → {len(batches)} batches "
        f"(batch_size={settings.batch_size}, parallel_batches={settings.parallel_batches})"
    )

    now = datetime.now(tz=timezone.utc)

    for batch_idx, batch in enumerate(batches):
        wave = batch_idx // settings.parallel_batches
        position_in_wave = batch_idx % settings.parallel_batches
        delay_minutes = (
            wave * settings.estimated_time_per_batch_minutes
            + position_in_wave * settings.offset_between_parallel_batches_minutes
        )
        scheduled_time = now + timedelta(minutes=delay_minutes)

        run_deployment(
            name=settings.full_pipeline_deployment_name,
            parameters={"domains": batch},
            scheduled_time=scheduled_time,
            timeout=0,        # fire-and-forget: don't wait for each run
            as_subflow=False, # independent runs, no state propagation back to trigger
        )

        logger.info(
            f"Batch {batch_idx + 1}/{len(batches)} scheduled for "
            f"{scheduled_time.isoformat()} "
            f"(wave={wave}, position={position_in_wave}, {len(batch)} domains)"
        )
