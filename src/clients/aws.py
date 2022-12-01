import logging
from abc import abstractmethod
from typing import Sequence, Tuple

import boto3
from botocore import UNSIGNED
from botocore.client import Config

logger = logging.getLogger()

DELIMITER = "/"
S3_MALICIOUS_PREFIX = "0/"
S3_CLEAN_PREFIX = "1/"


def get_bucket_and_boto3_from_url(url):
    bucket, region = get_client_data_from_s3_url(url)
    return (
        bucket,
        boto3.client(
            's3', region_name=region,
            config=Config(signature_version=UNSIGNED)
            )
        )


class FileUrl:
    __slots__ = ('root_url', 'path')

    def __init__(self, root_url, path) -> None:
        self.root_url = root_url
        self.path = path

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"root_url={repr(self.root_url)}, "
            f"path={repr(self.path)}"
            ")"
        )

    def __str__(self) -> str:
        return f"{self.root_url}/{self.path}"


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


class URLScrapper:
    """Interface class - provides interface to list
    malicious and clean files urls for further processing
    """
    @abstractmethod
    def list_malicious_files(self, n: int) -> Sequence[FileUrl]:
        """Returns urls of malicious files.

        Args:
            n (int): max entries to return
        Returns:
            Sequence: urls of 'malicious' files
        """
        pass

    @abstractmethod
    def list_clean_files(self, n: int) -> Sequence[FileUrl]:
        """Returns urls of clean files.

        Args:
            n (int): max entries to return

        Returns:
            Sequence: urls of 'clean' files
        """
        pass


class S3Scrapper(URLScrapper):
    """Class that scraps urls for malicious and clean files from aws s3.
    """
    def __init__(self, bucket, boto3_client, root_url) -> None:
        self.bucket = bucket
        self.boto3_client = boto3_client
        self.root_url = root_url

    @classmethod
    def from_root_url(cls, root_url: str):
        """Instantinate class from url.

        Args:
            root_url (str): s3 storage url

        Returns:
            S3Client: instance
        """

        bucket, region = get_client_data_from_s3_url(root_url)
        return cls(bucket, region, root_url)

    def get_keys(
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
            Sequence: keys of files

        Yields:
            Iterator[Sequence]: file key
        """

        # TODO: simplify

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
            rsp = self.boto3_client.list_objects_v2(
                Bucket=self.bucket, Prefix=prefix, Delimiter=delimiter,
                MaxKeys=max_keys, **list_objects_kwargs
                )
            logger.debug(
                f"Prefix {prefix}, delimiter {delimiter}, response {rsp}"
                )
            iter += 1
            # filter out files and data that are not "interesting"
            data = [
                item["Key"]
                for item in rsp["Contents"] if "00Tree.html" not in item['Key']
                ]
            yield from data

    def list_keys(self, n: int, prefix: str) -> Sequence:
        """Returns keys of S3 files.

        Args:
            n (int): max entries to return
            prefix (str): s3 prefix to filter s3 keys

        Returns:
            Sequence: keys of S3 files
        """
        return self.get_keys(n, prefix)

    def urls_from_keys(self, n: int, prefix: str) -> Sequence:
        return (
            FileUrl(self.root_url, key)
            for key in self.list_keys(n, prefix)
            )

    def list_malicious_files_urls(self, n: int) -> Sequence:
        """Returns urls of malicious files.

        Args:
            n (int): max urls to return
        Returns:
            Sequence: urls of malicious files
        """
        return self.urls_from_keys(n, S3_MALICIOUS_PREFIX)

    def list_clean_files_urls(self, n: int) -> Sequence:
        """Returns urls of clean files.

        Args:
            n (int): max entries to return

        Returns:
            Sequence: urls of clean files
        """
        return self.urls_from_keys(n, S3_CLEAN_PREFIX)
