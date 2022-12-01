import logging

import requests

logger = logging.getLogger()


def download_file(src: str, dest: str) -> None:
    """Downloads file via GET.

    Args:
        src (str): source url to download file from
        dest (str): local target path to download file to
    """
    logger.debug(f"Downloading from {src} to {dest}")
    # Consider stream
    r = requests.get(src)
    if r.status_code == 200:
        with open(dest, mode='wb') as f:
            f.write(r.content)
    logger.debug(f"File {dest} saved")
