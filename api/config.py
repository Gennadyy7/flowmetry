from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    model_config = {
        'extra': 'ignore',
        'env_file': '.env',
        'env_file_encoding': 'utf-8',
    }

    API_HOST: str
    API_PORT: int
    API_RELOAD: bool


settings = Settings()  # type: ignore[call-arg]
