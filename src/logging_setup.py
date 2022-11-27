import logging.config
from logging_config import LOGGING_CONFIG


def load_logger_config(name="default"):
    logging.config.dictConfig(LOGGING_CONFIG[name])
