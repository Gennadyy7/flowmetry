from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    model_config = {
        'extra': 'ignore',
        'env_file': '.env',
        'env_file_encoding': 'utf-8',
    }

    REDIS_HOST: str
    REDIS_PORT: int
    REDIS_DB: str


settings = Settings()  # type: ignore[call-arg]
