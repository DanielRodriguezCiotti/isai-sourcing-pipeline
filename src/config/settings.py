from functools import lru_cache

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore")

    supabase_url: str = Field(alias="SUPABASE_URL")
    supabase_service_role_key: SecretStr = Field(alias="SUPABASE_SERVICE_ROLE_KEY")
    google_api_key: SecretStr = Field(alias="GOOGLE_API_KEY")
    attio_cg_token: SecretStr = Field(alias="ATTIO_CG_TOKEN")
    attio_by_token: SecretStr = Field(alias="ATTIO_BY_TOKEN")
    traxcn_exports_bucket_name: str = Field(
        alias="TRAXCN_EXPORTS_BUCKET_NAME", default="traxcn_exports"
    )
    websites_bucket_name: str = Field(alias="WEBSITES_BUCKET_NAME", default="websites")
    search_resources_bucket_name: str = Field(
        alias="SEARCH_RESOURCES_BUCKET_NAME", default="search_ressources"
    )
    dealroom_bucket_name: str = Field(alias="DEALROOM_BUCKET_NAME", default="dealroom")
    dealroom_api_key: SecretStr = Field(alias="DEALROOM_API_KEY")
    batch_size: int = Field(alias="BATCH_SIZE", default=200)
    parallel_batches: int = Field(alias="PARALLEL_BATCHES", default=4)
    estimated_time_per_batch_minutes: int = Field(
        alias="ESTIMATED_TIME_PER_BATCH", default=120
    )
    offset_between_parallel_batches_minutes: int = Field(
        alias="OFFSET_BETWEEN_PARALLEL_BATCHES", default=3
    )
    full_pipeline_deployment_name: str = Field(
        alias="FULL_PIPELINE_DEPLOYMENT_NAME",
        default="full-pipeline-flow/full-pipeline-deployment",
    )
    website_enrichment_batch_size: int = Field(
        alias="WEBSITE_ENRICHMENT_BATCH_SIZE", default=20
    )
    compute_business_metric_batch_size: int = Field(
        alias="COMPUTE_BUSINESS_METRIC_BATCH_SIZE", default=200
    )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
