import logging
from typing import Mapping, Sequence

from clients.aws import S3Client
from processors.base import MetaProcessor

logger = logging.getLogger()

S3_MALICIOUS_PREFIX = "0/"
S3_CLEAN_PREFIX = "1/"


class S3Processor(MetaProcessor):
    """Class that processess metadata located at S3 AWS storage.
    """
    # TODO: ETag of S3 file is sometimes MD5. Could speed-up processing.
    def __init__(self, url) -> None:
        super().__init__(S3Client.from_url(url))

    def list_files(self, n: int, prefix: str) -> Sequence:
        """Returns informations about S3 files.

        Args:
            n (int): max entries to return
            prefix (str): s3 prefix to filter results

        Returns:
            Sequence: info about found malicious files
        """
        return self.data_client.list_objects_content(n, prefix)

    def list_malicious_files(self, n: int) -> Sequence:
        """Returns informations about files from S3 'malicious' folder.

        Args:
            n (int): max entries to return
        Returns:
            Sequence: meta info about found 'malicious' files
        """
        return self.list_files(n, S3_MALICIOUS_PREFIX)

    def list_clean_files(self, n: int) -> Sequence:
        """Returns informations about files from S3 'clean' folder.

        Args:
            n (int): max entries to return

        Returns:
            Sequence: meta info about found 'clean' files
        """
        return self.list_files(n, S3_CLEAN_PREFIX)

    @staticmethod
    def get_path(data: Mapping) -> str:
        """Finds file path information from S3 metadata entry.

        Args:
            data (Mapping): mapping of file properties info from S3

        Returns:
            str: remote file path
        """
        return data["Key"]

    @staticmethod
    def get_size(data: Mapping) -> str:
        """Finds file size information from S3 metadata entry.

        Args:
            data (Mapping): mapping of file properties info from S3

        Returns:
            str: size in bytes
        """
        return data["Size"]

    async def download_file(self, src: str, dest: str) -> None:
        """Downloads file from S3 store.

        Args:
            src (str): remote file path
            dest (str): destination file path
        """
        await self.data_client.download_file(src, dest)
