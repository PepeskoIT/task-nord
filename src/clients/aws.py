import logging
from typing import Sequence, Tuple

import boto3
from botocore import UNSIGNED
from botocore.client import Config
from processors.base import sempahore

from clients.web import download_file

logger = logging.getLogger()

DELIMITER = "/"


def get_client_data_from_s3_url(url: str) -> Tuple[str, str]:
    """Extract bucket and region information from s3 url.

    Args:
        url (str): s3 url

    Returns:
        Tuple[str, str]: return bucket, region information
    """
    clean_url = url.split("//")[-1]
    decomposed_url = clean_url.split(".")
    return decomposed_url[0], decomposed_url[2]


class S3Client:
    """Wrapper class for boto3 interface.
    """
    def __init__(self, bucket: str, region: str, url: str) -> None:
        self.bucket = bucket
        self.region = region
        self.url = url
        self._client = boto3.client(
            's3', region_name=region, config=Config(signature_version=UNSIGNED)
        )

    @classmethod
    def from_url(cls, url: str):
        """Instantinate class from url.

        Args:
            url (str): s3 storage url

        Returns:
            S3Client: instance
        """

        bucket, region = get_client_data_from_s3_url(url)
        return cls(bucket, region, url)

    # TODO: async maybe?
    def list_objects_content(
            self, max_cnt: int, prefix: str, delimiter: str = DELIMITER,
            max_keys: int = 1000
    ) -> Sequence:
        """Lists meta data of task related files in the bucket.

        Args:
            max_cnt (int): total number of maximum files to return
            prefix (str): prefix to filter s3 bucket files
            delimiter (str, optional): folder delimiter in file key.
                Defaults to "/".
            max_keys (int, optional): number of files to return in single
                response. Defaults to 1000.

        Returns:
            Sequence: metadata from files of intrest

        Yields:
            Iterator[Sequence]: file metadata
        """
        cnt_left = max_cnt
        rsp = None
        data = None
        list_objects_kwargs = {}
        is_trucated = True
        iter = 0

        while is_trucated and cnt_left:
            if rsp:
                cnt_returned = len(data) if data else 0
                logger.debug(f"Aquired {cnt_returned}")
                cnt_left -= cnt_returned
                is_trucated = rsp["IsTruncated"]
                if is_trucated:
                    continuation_token = rsp["NextContinuationToken"]
                    list_objects_kwargs = {
                        "ContinuationToken": continuation_token
                        }
            logger.debug(f"Left to be aquired: {cnt_left}")
            if cnt_left and cnt_left < max_keys:
                logger.debug(
                    f"max keys ({max_keys}) < left to aquire ({cnt_left}."
                    f"Set new value max_keys={cnt_left}"
                    )
                # don't fetch more then needed
                max_keys = cnt_left
            elif cnt_left <= 0:
                logger.debug(
                    f"Already reached desired limit {max_cnt}. "
                    f"{cnt_left} left to be listed."
                )
                break
            logger.debug(f"Iteration: {iter}")
            rsp = self._client.list_objects_v2(
                Bucket=self.bucket, Prefix=prefix, Delimiter=delimiter,
                MaxKeys=max_keys, **list_objects_kwargs
                )
            logger.debug(
                f"Prefix {prefix}, delimiter {delimiter}, response {rsp}"
                )
            iter += 1
            # filter out files that are not "interesting"
            data = [
                item
                for item in rsp["Contents"] if "00Tree.html" not in item['Key']
                ]
            yield from data

    # @sempahore(50)
    async def download_file(self, key: str, target_path: str) -> None:
        """Download file from 'Key' value of s3 metadata.

        Args:
            key (str): 'Key' value from s3
            target_path (str): local path to save file to
        """
        http_url = f"{self.url}/{key}"
        await download_file(http_url, target_path)
