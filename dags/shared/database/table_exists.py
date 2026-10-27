from logging import getLogger
from typing import cast

from schemas.base import Base
from sqlalchemy import Table, create_engine, inspect

log = getLogger(__name__)


def ensure_table_exists(engine_url: str, schema: type[Base]) -> None:
    """Check if a table exists with the specified schema."""
    table_name = schema.__tablename__

    log.info(f"Inspecting if '{table_name}' exists ...")

    engine = create_engine(url=engine_url)

    try:
        inspector = inspect(engine)
        if not inspector.has_table(table_name=table_name):
            Base.metadata.create_all(
                bind=engine, tables=[cast(Table, schema.__table__)]
            )
            log.info(f"Table '{table_name}' did not exist, created ...")
        else:
            log.info(f"Table '{table_name}' already exists, skipping creation ...")
    except Exception as e:
        log.error(f"Error ensuring table '{table_name}' exists: {e} ...")
        raise
    finally:
        engine.dispose()
        log.info("Disposed database engine")
