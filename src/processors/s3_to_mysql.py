import logging

from clients.db import get_db_session
from db_models.meta import Meta
from definitions import BOTO3_CLIENT, BUCKET
from processors.base import MetaProcessor

logger = logging.getLogger()

COUNT_QUERY = (
    """select count(1) as count
    from meta where hash = :hash"""
)


class MySQLMixin:
    """Mixin that adds MySQL db session support.
    """
    def send_to_db(self, db_entry: Meta) -> None:
        """Sends data object to database.

        Args:
            db_entry (Base): data object
        """
        logger.debug(f"Entry process: {str(db_entry)}")
        with get_db_session() as session:
            is_already_in_db = (session.execute(
                COUNT_QUERY, {"hash": db_entry.hash}

            )).first()[0]
            logger.debug(f"DB query result: {is_already_in_db}")
            if not is_already_in_db:
                session.add(db_entry)
                logger.debug(f"Added new row to db: {str(db_entry)}")


class S3MysqlProcessor(MetaProcessor, MySQLMixin):
    # Download via 'requests' lib implementation seems slower than boto3.
    # boto3 dl is fastest when instance is reused for same bucket/region
    # operations. Current implementation of boto3 cannot be pickeled though!
    # What if files are downloaded from different buckets or regions?
    # TODO: can't pass boto3 to instance attributes due to not pickable.
    # Consider some configurable singleton client/session cache obj that allows
    # dynamic assigment instead hardcoded.
    bucket = BUCKET
    boto3_client = BOTO3_CLIENT

    def download_file(self, src: str, dest: str) -> None:
        """Downloads file from S3 store.

        Args:
            src (str): remote file path
            dest (str): destination file path
        """
        logger.debug(f"src: {repr(src)}, dest: {dest}")
        self.boto3_client.download_file(self.bucket, src.path, dest)
