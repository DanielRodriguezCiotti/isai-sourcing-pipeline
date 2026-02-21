from datetime import datetime, timedelta, timezone

from prefect import task
from prefect.deployments import run_deployment

from src.config.settings import get_settings
from src.utils.logger import get_logger


@task(name="schedule_pipeline_runs")
def schedule_pipeline_runs(domains: list[str], prefix: str = "full-pipeline-run"):
    settings = get_settings()
    logger = get_logger()

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
            flow_run_name=f"{prefix}-#{str(batch_idx + 1).zfill(len(str(len(batches))))}",
            parameters={"domains": batch},
            scheduled_time=scheduled_time,
            timeout=0,  # fire-and-forget: don't wait for each run
            as_subflow=False,  # independent runs, no state propagation back to trigger
        )

        logger.info(
            f"Batch {batch_idx + 1}/{len(batches)} scheduled for "
            f"{scheduled_time.isoformat()} "
            f"(wave={wave}, position={position_in_wave}, {len(batch)} domains)"
        )
