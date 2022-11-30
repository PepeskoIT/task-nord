import asyncio
import datetime
import hashlib
import logging
import pathlib
from abc import abstractmethod
from itertools import chain
from os.path import join
from typing import Any, Sequence

from aiofiles import open as a_open
from aiofiles import os

from clients.db import get_db_session
from common import sempahore
from db_models.meta import Base, Meta

logger = logging.getLogger()

S3_MALICIOUS_PREFIX = "0/"
S3_CLEAN_PREFIX = "1/"
LOCAL_DL_DIR = "/tmp"

COUNT_QUERY = (
    """select count(1) as count
    from meta where hash = :hash"""
)


async def cal_md5_in_chunks(file_path: str) -> str:
    """Calculates MD5 of given file in async chunks.

    Args:
        file_path (str): path to the local file

    Returns:
        str: calculated MD5 hash
    """
    logger.debug(f"MD5 calc of {file_path} begging")
    start_time = datetime.datetime.utcnow()

    md5_hash = hashlib.md5()
    async with a_open(file_path, "rb") as f:
        async for chunk in f:
            md5_hash.update(chunk)
        end_time = datetime.datetime.utcnow()
        logger.debug(
            f"MD5 calc of {file_path} end. Took {end_time - start_time}"
            )
        return md5_hash.hexdigest()


async def calc_md5(file_path: str) -> str:
    """Calculates MD5 of given file.

    Args:
        file_path (str): path to the local file

    Returns:
        str: calculated MD5 hash
    """
    logger.debug(f"MD5 calc of {file_path} begging")
    start_time = datetime.datetime.utcnow()
    async with a_open(file_path, "rb") as f:
        hash = hashlib.md5(await f.read()).hexdigest()
        end_time = datetime.datetime.utcnow()
        logger.debug(
            f"MD5 calc of {file_path} end. Took {end_time - start_time}"
            )
        return hash


class MetaProcessor:
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
    async def get_hash(file_path: str) -> str:
        # return await calc_md5(file_path)
        return await cal_md5_in_chunks(file_path)

    @abstractmethod
    async def download_file(self, src: str, dest: str) -> None:
        """Download file.

        Args:
            src (str): source path to download from
            dest (str): destination path to download to
        """
        pass

    @staticmethod
    async def send_to_db(db_entry: Base) -> None:
        """Sends data object to database

        Args:
            db_entry (Base): data object
        """
        logger.debug(f"Entry process: {str(db_entry)}")
        async with get_db_session() as session:
            is_already_in_db = (await session.execute(
                COUNT_QUERY, {"hash": db_entry.hash}

            )).first()[0]
            logger.debug(f"DB query result: {is_already_in_db}")
            if not is_already_in_db:
                session.add(db_entry)
                logger.debug(f"Added new row to db: {str(db_entry)}")

    @sempahore(50)
    async def io_file_process(self, src: str, dest: str) -> bytes:
        """Groups io file related actions.

        Args:
            src (str): source url to download file from
            dest (str): local target path to download file to
        Returns:
            bytes: metadata aquired through sequentional i/o actions
        """
        await self.download_file(src, dest)
        # TODO: check arch
        # TODO: check imports?
        # TODO: check exports?

        return await self.get_hash(dest)

    async def proces_item(self, item: Any) -> None:
        # async with PROCESS_FILE_SEM as sem:
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

        hash = await self.io_file_process(path, target_path)

        await self.send_to_db(
            Meta(
                    hash=str.encode(hash), size=size,
                    path=path, extension=extension.lower(),
                )
        )

    async def process_data(self, n_malicious: int, n_clean: int) -> None:
        """Process metadata n of malicious and n of clean files.

        Args:
            n_malicious (int): number of malicious files to process
            n_clean (int): number of malicious files to process
        """
        # TODO: consider local dir as TemporaryDirectory - self cleanup
        # after processing completed
        await os.makedirs(LOCAL_DL_DIR, exist_ok=True)

        all_files_to_process = (
            chain(
                self.list_clean_files(n_clean),
                self.list_malicious_files(n_malicious)
                )
            )

        coroutines = [self.proces_item(item) for item in all_files_to_process]
        await asyncio.gather(*coroutines)
