from logging import getLogger
from typing import Any

from schemas.base import Base
from sqlalchemy import func
from sqlalchemy.orm import Session

log = getLogger(__name__)


def get_max_value_from_schema_by_column(
    session: Session, schema: type[Base], column_name: str
) -> Any:
    """Will find and return the maximum value within a specified schema for a specified column."""

    column = getattr(schema, column_name)
    max_value = session.query(func.max(column)).scalar()
    if not max_value:
        log.info(
            f"No max value found for colum name '{column_name}' in schema '{schema.__tablename__}'. Returning None ..."
        )
    else:
        log.info(
            f"Max value found for column name '{column_name}' in schema '{schema.__tablename__}'. Returning '{max_value}' ..."
        )
    return max_value
