import os
from functools import lru_cache

from prefect.blocks.system import Secret as PrefectSecret
from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore")

    supabase_url: str = Field(alias="SUPABASE_URL")
    supabase_service_role_key: SecretStr = Field(alias="SUPABASE_SERVICE_ROLE_KEY")
    google_api_key: SecretStr = Field(alias="GOOGLE_API_KEY")
    traxcn_exports_bucket_name: str = Field(
        alias="TRAXCN_EXPORTS_BUCKET_NAME", default="traxcn_exports"
    )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    # Iterate over the list of aliases and if they are not in the environment, try to pull them from prefect secrets, else raise an error
    for field in Settings.model_fields.values():
        if os.environ.get(field.alias) is None:
            prefect_secret = PrefectSecret(field.alias).load()
            secret_str = prefect_secret.get()
            if secret_str is None:
                raise ValueError(f"Secret {field.alias} is not set")
            os.environ[field.alias] = secret_str
    return Settings()
