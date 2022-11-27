import hashlib
import logging
import pathlib
from abc import abstractmethod
from typing import Sequence, Mapping, Any
from itertools import chain
from os import makedirs
from os.path import join

from clients.aws import S3Client
from clients.db import get_db_session
from db_models.meta import Meta, Base

logger = logging.getLogger()

S3_MALICIOUS_PREFIX = "0/"
S3_CLEAN_PREFIX = "1/"
LOCAL_DL_DIR = "/tmp"


def calc_md5(file_path: str) -> str:
    """Calculates MD5 of given file.

    Args:
        file_path (str): path to the local file

    Returns:
        str: calculated MD5 hash
    """
    md5_hash = hashlib.md5()
    with open(file_path, "rb") as f:
        for block in iter(lambda: f.read(4096), b""):
            md5_hash.update(block)
        return md5_hash.hexdigest()


class MetaAgregator:
    """Interface base class for meta data agregattion and processing.
    Should be interited from when introducing new metadata processing inputs
    and outputs.
    """

    def __init__(self, data_client, ) -> None:
        self.data_client = data_client
        pass

    @abstractmethod
    def list_malicious_files(self, n: int) -> Sequence:
        pass

    @abstractmethod
    def list_clean_files(self, n: int) -> Sequence:
        pass

    @abstractmethod
    def get_path(data: Any) -> str:
        pass

    @abstractmethod
    def get_size(data: Any) -> str:
        pass

    @staticmethod
    def get_extension(file_path: str) -> str:
        """Aquires extension from file path.

        Args:
            file_path (str): path of the file

        Returns:
            str: file extension
        """
        return pathlib.Path(file_path).suffix[1:]

    @staticmethod
    def get_hash(file_path: str) -> str:
        return calc_md5(file_path)

    @abstractmethod
    def download_file(self, src: str, dest: str) -> None:
        """Download file.

        Args:
            src (str): source path to download from
            dest (str): destination path to download to
        """
        pass

    @staticmethod
    def send_to_db(db_entry: Base) -> None:
        logger.debug(f"Entry process: {str(db_entry)}")
        with get_db_session() as session:
            is_already_in_db = session.query(Meta).filter(
                Meta.hash == db_entry.hash
            ).count()
            logger.debug(f"DB query result: {is_already_in_db}")
            if not is_already_in_db:
                session.add(db_entry)
                logger.debug(f"Added new row to db: {str(db_entry)}")

    def proces_item(self, item: Any) -> None:
        """Process aquired metadata of given file.

        Args:
            item (Any): object with file metadata
        """
        logger.debug(f"Processing item: {item}")

        path = self.get_path(item)
        size = self.get_size(item)
        # lets assume file ext names are correct for this dev iteration
        # TODO: check for more sophisticated ways of detections
        extension = self.get_extension(path)
        target_path = join(LOCAL_DL_DIR, path.split("/")[-1])

        # TODO: group processing of downloaded file - into one method maybe?
        self.download_file(path, target_path)
        hash = self.get_hash(target_path)
        # TODO: check arch
        # TODO: check imports?
        # TODO: check exports?

        self.send_to_db(
            Meta(
                    hash=str.encode(hash), size=size,
                    path=path, extension=extension,
                )
        )

    def process_data(self, n_malicious: int, n_clean: int) -> None:
        """Process metadata n of malicious and n of clean files.

        Args:
            n_malicious (int): number of malicious files to process
            n_clean (int): number of malicious files to process
        """
        # TODO: consider local dir as TemporaryDirectory - self cleanup
        # after processing completed
        makedirs(LOCAL_DL_DIR, exist_ok=True)

        all_files_to_process = (
            chain(
                self.list_clean_files(n_clean),
                self.list_clean_files(n_malicious)
                )
            )

        for item in all_files_to_process:
            self.proces_item(item)

# TODO: move below to separate folder


class S3Agregator(MetaAgregator):
    """Class that processess metadata located at S3 AWS storage.
    """
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

    def download_file(self, src: str, dest: str) -> None:
        """Downloads file from S3 store.

        Args:
            src (str): remote file path
            dest (str): destination file path
        """
        self.data_client.download_file(src, dest)
