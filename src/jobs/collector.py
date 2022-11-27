from typing import Tuple

from envs import S3_STORAGE_URL
from processors.s3 import S3Processor

S3 = S3Processor(S3_STORAGE_URL)


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


async def process_all(n: int) -> None:
    """Process number of malicious and clean files.

    Args:
        n (int): number to calculate clean and malicious files to process
    """
    malicious_cnt, clean_cnt = calculate_cnt_div(n)
    await S3.process_data(malicious_cnt, clean_cnt)
