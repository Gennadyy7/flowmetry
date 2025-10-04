import uuid

from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    model_config = {
        'extra': 'ignore',
        'env_file': '.env',
        'env_file_encoding': 'utf-8',
    }

    REDIS_HOST: str
    REDIS_PORT: int
    REDIS_PASSWORD: str | None = None
    REDIS_DB: int = 0
    REDIS_STREAM_NAME: str
    REDIS_CONSUMER_GROUP: str
    REDIS_CONSUMER_NAME: str = Field(
        default_factory=lambda: f'agg-{uuid.uuid4().hex[:8]}'
    )
    REDIS_BLOCK_MS: int
    REDIS_BATCH_SIZE: int
    REDIS_PENDING_IDLE_MS: int

    SERVICE_NAME: str
    SERVICE_VERSION: str
    LOG_LEVEL: str
    LOG_FORMAT: str

    WORKER_SHUTDOWN_TIMEOUT: float = 10.0

    DB_HOST: str
    DB_PORT: int
    DB_USER: str
    DB_PASSWORD: str
    DB_NAME: str


settings = Settings()  # type: ignore[call-arg]
