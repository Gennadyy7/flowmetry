from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    model_config = {
        'extra': 'ignore',
        'env_file': '.env',
        'env_file_encoding': 'utf-8',
        'frozen': True,
    }

    API_HOST: str
    API_PORT: int
    API_RELOAD: bool

    REDIS_HOST: str
    REDIS_PORT: int
    REDIS_PASSWORD: str | None = None
    REDIS_DB: int = 0
    REDIS_STREAM_NAME: str

    SERVICE_NAME: str
    SERVICE_VERSION: str
    LOG_LEVEL: str
    LOG_FORMAT: str


settings = Settings()  # type: ignore[call-arg]
