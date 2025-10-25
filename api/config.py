from typing import Literal

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

    API_HOST: str
    API_PORT: int
    API_RELOAD: bool

    SERVICE_NAME: str
    SERVICE_VERSION: str
    LOG_LEVEL: str
    LOG_FORMAT: str

    DB_HOST: str
    DB_PORT: int
    POSTGRES_DB: str
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str

    DB_MIN_POOL_SIZE: int = 1
    DB_MAX_POOL_SIZE: int = 5
    DB_COMMAND_TIMEOUT: float = 30.0
    DB_SSL_MODE: SSLMode | None = None


settings = Settings()  # type: ignore[call-arg]
