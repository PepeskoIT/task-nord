from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import (
    Column, String, TIMESTAMP, text, JSON, Sequence, DateTime,
    BIGINT, VARBINARY, VARCHAR
)
# from sqlalchemy.dialects.mysql import BIGINT, VARBINARY, VARCHAR
import datetime


Base = declarative_base()
metadata = Base.metadata


class Meta(Base):
    __tablename__ = 'meta'

    id = Column(
        BIGINT, Sequence('meta_id_seq', cycle=True), primary_key=True
    )
    created = Column(
        DateTime(), default=datetime.datetime.utcnow, nullable=False
    )
    hash = Column(VARBINARY(32), nullable=False)
    path = Column(VARCHAR(260), nullable=False)
    size = Column(BIGINT(), nullable=False)
    extension = Column(VARCHAR(6), nullable=False)
