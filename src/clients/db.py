from contextlib import contextmanager
import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import os
import socket
from envs import DB_PASSWORD, DB_NAME, DB_HOST, DB_PORT, DB_USER

logger = logging.getLogger()

DB_URL = (
    f"mysql+mysqldb://{DB_USER}:{DB_PASSWORD}"
    f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

ENGINE = create_engine(
    DB_URL, future=True, echo=True,
    pool_size=20, max_overflow=0
)

SESSION = sessionmaker(ENGINE, expire_on_commit=False)


class DbClientError(Exception):
    pass


@contextmanager
def get_db_session(session=SESSION):
    with session.begin() as session:
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
