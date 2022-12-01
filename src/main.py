# Copyright 2022, Lukasz Przybyl , All rights reserved.

import datetime
import logging

from jobs.collector import process_all
from logging_setup import load_logger_config

load_logger_config()

logger = logging.getLogger()

N_NUMBER = 2000  # task input value


def main() -> None:
    """Main program starting function.
    """
    logger.info("Main starts")
    start_time = datetime.datetime.utcnow()
    logger.info("Start time")
    process_all(N_NUMBER)
    end_time = datetime.datetime.utcnow()
    logger.info(f"Main ends. Execution took {str(end_time-start_time)}")


if __name__ == '__main__':
    main()
