from typing import Literal
import uuid

from pydantic import Field
from pydantic_settings import BaseSettings

SSLMode = Literal[
    'disable',
    'allow',
    'prefer',
    'require',
    'verify-ca',
    'verify-full',
]


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
    POSTGRES_DB: str
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str

    HEALTH_SERVER_HOST: str = '0.0.0.0'
    HEALTH_SERVER_PORT: int = 8080

    DB_MIN_POOL_SIZE: int = 1
    DB_MAX_POOL_SIZE: int = 10
    DB_COMMAND_TIMEOUT: float = 60.0
    DB_SSL_MODE: SSLMode | None = None


settings = Settings()  # type: ignore[call-arg]
