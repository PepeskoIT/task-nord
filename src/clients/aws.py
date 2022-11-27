import boto3

from botocore import UNSIGNED
from botocore.client import Config
from os.path import join

import logging

logger = logging.getLogger()

DELIMITER = "/"


def get_client_data_from_s3_url(url: str):
    """
    return bucket, region
    """
    clean_url = url.split("//")[-1]
    decomposed_url = clean_url.split(".")
    return decomposed_url[0], decomposed_url[2]


class S3Client:
    def __init__(self, bucket: str, region: str):
        self.bucket = bucket
        self.region = region
        self._client = boto3.client(
            's3', region_name=region, config=Config(signature_version=UNSIGNED)
        )

    @classmethod
    def from_url(cls, url: str):
        bucket, region = get_client_data_from_s3_url(url)
        return cls(bucket, region)

    def list_objects_content(
            self, max_cnt: int, prefix: str, delimiter: str = "/",
            max_keys: int = 1000
    ):
        cnt_left = max_cnt
        rsp = None
        list_objects_kwargs = {}
        is_trucated = True  # run at least ones
        iter = 0

        while is_trucated and cnt_left:
            if rsp:
                cnt_returned = len(rsp["Contents"])
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
            iter += 1
            yield from rsp["Contents"]

    def download_file(self, key: str, target_path: str):
        self._client.download_file(self.bucket, key, target_path)
