from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from potatocore.core.config import get_settings
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker


def get_warehouse_engine() -> Engine:
    settings = get_settings()
    return create_engine(settings.warehouse_sqlalchemy_url, future=True, pool_pre_ping=True)


def get_api_engine() -> Engine:
    settings = get_settings()
    return create_engine(settings.api_sqlalchemy_url, future=True, pool_pre_ping=True)


def build_session_factory(engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)


@contextmanager
def session_scope(engine: Engine) -> Iterator[Session]:
    session_factory = build_session_factory(engine)
    session = session_factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
