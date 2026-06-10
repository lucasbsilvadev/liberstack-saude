from pydantic_settings import BaseSettings
from pydantic import Field

class Settings(BaseSettings):
    ENV: str = Field(default="production")
    
    # mapeamento de credenciais
    DB_HOST: str
    DB_PORT: int = Field(default=5432)
    DB_NAME: str
    DB_USER: str
    DB_PASSWORD: str
    
    INFOSAUDE_API_URL: str

    model_config = {
        "extra": "ignore"
    }

settings = Settings()