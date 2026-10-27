from collections.abc import Generator
from contextlib import contextmanager
from logging import getLogger

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

log = getLogger(__name__)


@contextmanager
def get_db_session(engine_url: str) -> Generator[Session]:
    """Provides a transactional database session scope.
    Rolls back on exception, and always closes the session.
    Does not flush by default."""

    engine = create_engine(url=engine_url, echo=False)
    log.info("Created database engine ...")

    SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)

    session = SessionLocal()
    log.info("Created new database session ...")

    try:
        yield session
    except Exception:
        log.exception(
            "Encountered an exception while interacting with the database. Rolling back ..."
        )
        session.rollback()
        raise
    finally:
        session.close()
        log.info("Closed database session ...")
        engine.dispose()
        log.info("Disposed database engine ...")
