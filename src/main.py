
from jobs.meta import process_all

import logging

from logging_setup import load_logger_config

load_logger_config()

logger = logging.getLogger()


if __name__ == '__main__':
    process_all(1)
