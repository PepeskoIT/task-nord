import logging
from itertools import chain
from typing import Tuple

from clients.aws import S3Scrapper
from definitions import BOTO3_CLIENT, BUCKET
from envs import S3_STORAGE_URL
from processors.s3_to_mysql import S3MysqlProcessor

logger = logging.getLogger()


def calculate_cnt_div(n: int) -> Tuple[int, int]:
    """Caluculates how many malicious and clean files to process based on
        input.

    Args:
        n (int): used to calucalte number of malicious and clean files to 
            process

    Returns:
        Tuple[int, int]: malicious files to proc, clean files to proc
    """
    return n//2, n//2


def process_all(n: int) -> None:
    """Process number of malicious and clean files.

    Args:
        n (int): number to calculate clean and malicious files to process
    """

    s3scrapper = S3Scrapper(
        bucket=BUCKET, boto3_client=BOTO3_CLIENT, root_url=S3_STORAGE_URL
        )

    processor = S3MysqlProcessor()

    malicious_cnt, clean_cnt = calculate_cnt_div(n)
    urls_to_process = (
            chain(
                s3scrapper.list_malicious_files_urls(clean_cnt),
                s3scrapper.list_clean_files_urls(malicious_cnt)
                )
            )
    processor.process_files(urls_to_process)
