import os
import sys
import logging.config

from classes.config import Config
from logging.handlers import RotatingFileHandler
from classes.user_events_db_handler import UserEventsHandler
from classes.parquet_reader import ParquetReader
from classes.transformations import Transformations


APP_CONFIG_PATH = os.path.join(os.path.dirname((os.path.dirname(os.path.abspath(__file__)))), 'config')
LOGGING_PATH = os.path.join((os.path.dirname(os.path.abspath(__file__))), 'logs')
LOGGING_CONFIG = os.path.join(os.path.dirname((os.path.dirname(os.path.abspath(__file__)))), 'config', 'logging.conf')
LOGGER = "ep-etl"


def main(logger_obj):
    """
    Runs the ETL to load a Parquet file into Postgres
    :param logger_obj: The main logger which is initialized in this module
    """
    config = Config(logger_obj.name, APP_CONFIG_PATH)

    # Extract
    pr = ParquetReader(logger_obj.name, config)
    df_table = pr.read()

    # Transform
    tr = Transformations(logger_obj.name, config)
    user_events = tr.process_records(df_table)

    # Load
    ue_handler = UserEventsHandler(logger_obj.name, config)
    ue_handler.bulk_load(user_events)

    sys.exit(0)


if __name__ == "__main__":
    logging.config.fileConfig(LOGGING_CONFIG)
    logger = logging.getLogger(LOGGER)

    # Rotate the logs for every run
    for handler in logger.handlers:
        if isinstance(handler, RotatingFileHandler):
            handler.doRollover()
    logger.info("Starting Data Transformations")
    
    main(logger)
