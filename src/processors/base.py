import datetime
import hashlib
import logging
import os
import pathlib
from abc import abstractmethod
from os.path import join
from typing import Tuple
import subprocess

from pyspark.sql import SparkSession

from clients.aws import FileUrl
from db_models.meta import Meta

logger = logging.getLogger()

S3_MALICIOUS_PREFIX = "0/"
S3_CLEAN_PREFIX = "1/"
LOCAL_DL_DIR = "/tmp"

COUNT_QUERY = (
    """select count(1) as count
    from meta where hash = :hash"""
)

ARCH_START_IDX = "\narchitecture: "
ARCH_END_IDX = ", flags"

def calc_md5(file_path: str) -> str:
    """Calculates MD5 of given file.

    Args:
        file_path (str): path to the local file

    Returns:
        str: calculated MD5 hash
    """
    logger.debug(f"MD5 calc of {file_path} begging")
    start_time = datetime.datetime.utcnow()
    with open(file_path, "rb") as f:
        hash = hashlib.md5(f.read()).hexdigest()
        end_time = datetime.datetime.utcnow()
        logger.debug(
            f"MD5 calc of {file_path} end. Took {end_time - start_time}"
            )
        return hash


def get_arch(file_path):
    objdump_r = subprocess.run(
        f"objdump -f {file_path}".split(),
        capture_output=True
    )
    err = objdump_r.stderr.decode('utf-8').strip()
    if err:
        logger.warning(f"Error during objdump: {err}")
        return None
    objdump_r = objdump_r.stdout.decode('utf-8').strip()
    start_idx = objdump_r.find(ARCH_START_IDX) + len(ARCH_START_IDX)
    end_idx = objdump_r.find(ARCH_END_IDX)
    arch = objdump_r[start_idx:end_idx]
    logger.debug(f"detected architecture of {file_path} is {arch}")
    return arch


def get_extension(file_path: str) -> str:
    """Aquires extension from file path.

    Args:
        file_path (str): path of the file

    Returns:
        str: file extension
    """
    # lets assume file ext names are correct for this dev iteration
    # TODO: check for more sophisticated ways of detections
    return pathlib.Path(file_path).suffix[1:]


class MetaProcessor:
    """Interface base class for meta urls processing.
    Should be interited from when introducing new metadata processing inputs
    and outputs.
    """

    @abstractmethod
    def download_file(self, src: str, dest: str) -> None:
        pass

    def io_file_process(self, src: str, dest: str) -> Tuple[bytes, int]:
        """Groups io file related actions.

        Args:
            src (str): source url to download file from
            dest (str): local target path to download file to
        Returns:
            bytes: metadata aquired through sequentional i/o actions
        """
        self.download_file(src, dest)
        # TODO: check arch
        # TODO: check imports?
        # TODO: check exports?
        return calc_md5(dest), os.path.getsize(dest), get_arch(dest)

    def process_item(self, url: FileUrl) -> None:
        """Process given file url.

        Args:
            url (FileUrl): url of file to analyse
        """
        logger.debug(f"Processing item: {url}")

        path = url.path

        extension = get_extension(path)

        target_path = join(LOCAL_DL_DIR, path.split("/")[-1])

        hash, size, arch = self.io_file_process(url, target_path)

        self.send_to_db(
            Meta(
                    hash=str.encode(hash), size=size,
                    path=path, extension=extension.lower(), arch=arch
                )
        )

    def spark_processor(self, items_to_process):
        spark = SparkSession.builder.appName('backend').getOrCreate()
        sc = spark.sparkContext

        rdd = sc.parallelize(items_to_process)
        rdd.foreach(lambda item: self.process_item(item))

    def process_files(self, urls) -> None:
        """Process metadata n of malicious and n of clean files.

        Args:
            n_malicious (int): number of malicious files to process
            n_clean (int): number of malicious files to process
        """
        # TODO: consider local dir as TemporaryDirectory - self cleanup
        # after processing completed
        os.makedirs(LOCAL_DL_DIR, exist_ok=True)

        self.spark_processor(urls)
