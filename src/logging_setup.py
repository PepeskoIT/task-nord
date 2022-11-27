import logging.config

from logging_config import LOGGING_CONFIG


def load_logger_config(name="default"):
    """Loads logger settings. Should be called once.

    Args:
        name (str, optional): name of logger settings to load. 
            Defaults to "default".
    """
    logging.config.dictConfig(LOGGING_CONFIG[name])
