import logging

from aggregator.schemas import MetricPoint

logger = logging.getLogger(__name__)


class TimescaleDB:
    def __init__(self) -> None:
        logger.info('The database is initialized')

    @staticmethod
    async def connect() -> None:
        logger.info('Connection to the database is successful')

    @staticmethod
    async def insert_metric(point: MetricPoint) -> None:
        logger.info(
            'A metric has been inserted into the database',
            extra={'metric_point': point},
        )

    @staticmethod
    async def close() -> None:
        logger.info('The database connection is closed')


timescale_db = TimescaleDB()
