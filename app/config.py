from functools import lru_cache
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    mongo_uri: str = Field(default="mongodb://localhost:27017/?directConnection=true&replicaSet=rs0")
    mongo_db: str = Field(default="orderstream")

    app_host: str = "0.0.0.0"
    app_port: int = 8000
    log_level: str = "INFO"

    cohesion_window_status: float = 0.050
    cohesion_window_price: float = 0.200
    cohesion_window_default: float = 0.100

    max_client_queue: int = 200
    heartbeat_interval: int = 15
    health_event_interval: int = 10

    audit_genesis_hash: str = "0" * 64


@lru_cache
def get_settings() -> Settings:
    return Settings()
