# Copyright 2022, Lukasz Przybyl , All rights reserved.

import asyncio
import logging

from jobs.collector import process_all
from logging_setup import load_logger_config

load_logger_config()

logger = logging.getLogger()

N_NUMBER = 10  # task input value


async def main() -> None:
    """Main program starting function.
    """
    logger.info("Main starts")
    await process_all(N_NUMBER)
    logger.info("Main exits")

if __name__ == '__main__':
    asyncio.run(main())
