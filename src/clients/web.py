import logging

import aiofiles
from aiohttp.client import ClientSession

logger = logging.getLogger()


async def download_file(src: str, dest: str) -> None:
    """Downloads file.

    Args:
        src (str): source url to download file from
        dest (str): local target path to download file to
    """
    logger.debug(f"Downloading from {src} to {dest}")
    async with ClientSession() as session:
        async with session.get(src) as resp:
            if resp.status == 200:
                f = await aiofiles.open(dest, mode='wb')
                await f.write(await resp.read())
                await f.close()
                logger.debug(f"File {dest} saved")
