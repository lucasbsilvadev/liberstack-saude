from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field

class Settings(BaseSettings):
    ENV: str = Field(default="production")

    DB_HOST: str
    DB_PORT: int = Field(default=5432)
    DB_NAME: str
    DB_USER: str
    DB_PASSWORD: str

    MERCADO_LIVRE_API_KEY: str | None = Field(default=None)

    model_config = SettingsConfigDict(env_file="../.env", env_file_encoding="utf-8", extra="ignore")

    settings = Settings()