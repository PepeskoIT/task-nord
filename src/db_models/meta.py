import datetime

from sqlalchemy import (BIGINT, INT, VARBINARY, VARCHAR, Column, DateTime,
                        Sequence)
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class Meta(Base):
    __tablename__ = 'meta'

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}("
            f"hash={repr(self.hash)}, "
            f"path={repr(self.path)}, "
            f"size={repr(self.size)}, "
            f"extension={repr(self.extension)}, "
            f"arch={repr(self.arch)}, "
            f"imports={repr(self.imports)}, "
            f"exports={repr(self.exports)}"
            ")"
            )

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
    arch = Column(VARCHAR(16), nullable=True)
    imports = Column(INT(), nullable=True)
    exports = Column(INT(), nullable=True)
