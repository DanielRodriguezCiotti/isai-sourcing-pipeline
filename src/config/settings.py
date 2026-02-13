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
    attio_cg_token: SecretStr = Field(alias="ATTIO_CG_TOKEN")
    attio_by_token: SecretStr = Field(alias="ATTIO_BY_TOKEN")
    traxcn_exports_bucket_name: str = Field(
        alias="TRAXCN_EXPORTS_BUCKET_NAME", default="traxcn_exports"
    )
    websites_bucket_name: str = Field(alias="WEBSITES_BUCKET_NAME", default="websites")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    # Iterate over the list of aliases and if they are not in the environment, try to pull them from prefect secrets, else raise an error
    loaded_secrets = {}
    for field in Settings.model_fields.values():
        # If the field has no default and is not in the env vars, load from prefect secrets
        if not isinstance(field.default, str) and os.environ.get(field.alias) is None:
            secret_name = field.alias.lower().replace("_", "-")
            prefect_secret = PrefectSecret.load(secret_name)
            secret_str = prefect_secret.get()
            if secret_str is None:
                raise ValueError(f"Secret {field.alias} is not set")
            loaded_secrets[field.alias] = secret_str
    return Settings(**loaded_secrets)
