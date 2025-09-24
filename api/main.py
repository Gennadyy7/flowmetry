from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI
import uvicorn

from api.config import settings


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    try:
        yield
    finally:
        pass


app = FastAPI(lifespan=lifespan)


def main() -> None:
    uvicorn.run(
        'api.main:app',
        host=settings.API_HOST,
        port=settings.API_PORT,
        reload=settings.API_RELOAD,
    )


if __name__ == '__main__':
    main()
