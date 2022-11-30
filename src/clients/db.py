import logging
import socket
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from envs import DB_HOST, DB_NAME, DB_PASSWORD, DB_PORT, DB_USER

logger = logging.getLogger()
DB_ASYNC_DRIVER = "mysql+asyncmy"
DB_SYNC_DRIVER = "mysql+mysqldb"

DB_URL_TEMPLATE = (
    f"{{db_driver}}://{DB_USER}:{DB_PASSWORD}"
    f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )

DB_ASYNC_URL = DB_URL_TEMPLATE.format(db_driver=DB_ASYNC_DRIVER)
DB_SYNC_URL = DB_URL_TEMPLATE.format(db_driver=DB_SYNC_DRIVER)

ENGINE = create_async_engine(
    DB_ASYNC_URL, future=True, echo=True,
    pool_size=200, max_overflow=100, pool_recycle=3600
    )


ASYNC_SESSION = sessionmaker(
    ENGINE, expire_on_commit=False, class_=AsyncSession
    )


class DbClientError(Exception):
    """Generic db client related error
    """
    pass


@asynccontextmanager
async def get_db_session() -> AsyncSession:
    """Common session context to operate async db sessions.

    Raises:
        DbClientError: generic db related exception

    Yields:
        AsynSession: asyn db session object
    """

    async with ASYNC_SESSION.begin() as session:
        try:
            logger.debug("session ready")
            yield session
        except (socket.error, OSError) as e:
            logger.exception(
                f"Exception during handling session to DB {session}. "
                f"{e}"
                )
            raise DbClientError(e)
        finally:
            logger.debug("session closed")
