from sqlalchemy import create_engine, Column, String, Text, DateTime, Integer
from sqlalchemy.engine.url import make_url
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.sql import func
from typing import Optional, List, Dict, Any

import logging
import threading
import uuid


Base = declarative_base()
logger = logging.getLogger(__name__)


class _NoOpLock:
    def __enter__(self):
        pass

    def __exit__(self, *args):
        pass


class History(Base):
    __tablename__ = "history"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    memory_id = Column(Text)
    old_memory = Column(Text)
    new_memory = Column(Text)
    event = Column(Text)

    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    is_deleted = Column(Integer)
    actor_id = Column(Text)
    role = Column(Text)


class SQLiteManager:
    def __init__(self, db_url: str = "sqlite:///:memory:"):
        self.db_url = db_url
        self._is_sqlite = make_url(self.db_url).get_backend_name() == "sqlite"
        self.engine = create_engine(
            self.db_url,
            connect_args={"check_same_thread": False} if self._is_sqlite else {},
        )
        self.Session = scoped_session(sessionmaker(bind=self.engine))
        self._lock = threading.Lock() if self._is_sqlite else _NoOpLock()

        self._create_history_table()

    def _create_history_table(self):
        with self._lock:
            try:
                Base.metadata.create_all(self.engine)
            except Exception as e:
                logger.error(f"Failed to create history table: {e}")
                raise

    def add_history(
        self,
        memory_id: str,
        old_memory: Optional[str],
        new_memory: Optional[str],
        event: str,
        *,
        created_at: Optional[str] = None,
        updated_at: Optional[str] = None,
        is_deleted: int = 0,
        actor_id: Optional[str] = None,
        role: Optional[str] = None,
    ) -> None:
        with self._lock:
            session = self.Session()
            try:
                history = History(
                    memory_id=memory_id,
                    old_memory=old_memory,
                    new_memory=new_memory,
                    event=event,
                    is_deleted=is_deleted,
                    actor_id=actor_id,
                    role=role,
                )

                session.add(history)
                session.commit()
            except Exception as e:
                session.rollback()
                logger.error(f"Failed to add history record: {e}")
                raise
            finally:
                session.close()

    def get_history(self, memory_id: str) -> List[Dict[str, Any]]:
        with self._lock:
            session = self.Session()
            try:
                rows = (
                    session.query(History)
                    .filter(History.memory_id == memory_id)
                    .order_by(History.created_at.asc(), History.updated_at.asc())
                    .all()
                )

                return [
                    {
                        "id": r.id,
                        "memory_id": r.memory_id,
                        "old_memory": r.old_memory,
                        "new_memory": r.new_memory,
                        "event": r.event,
                        "created_at": r.created_at,
                        "updated_at": r.updated_at,
                        "is_deleted": bool(r.is_deleted),
                        "actor_id": r.actor_id,
                        "role": r.role,
                    }
                    for r in rows
                ]
            except Exception as e:
                logger.error(f"Failed to fetch history for memory_id={memory_id}: {e}")
                raise
            finally:
                session.close()

    def reset(self) -> None:
        """Drop and recreate the history table."""
        with self._lock:
            try:
                Base.metadata.drop_all(self.engine, tables=[History.__table__])
                Base.metadata.create_all(self.engine, tables=[History.__table__])
            except Exception as e:
                logger.error(f"Failed to reset history table: {e}")
                raise

    def close(self) -> None:
        self.Session.remove()
        self.engine.dispose()

    def __del__(self):
        self.close()
