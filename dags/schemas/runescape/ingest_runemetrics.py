import uuid
from datetime import datetime

from schemas.base import Base
from sqlalchemy import JSON, TIMESTAMP, UUID, Integer, String, text
from sqlalchemy.orm import Mapped, mapped_column


class IngestRuneMetricsT(Base):
    __tablename__ = "ingest_runemetrics"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID, primary_key=True, server_default=text("gen_random_uuid()")
    )

    user_id: Mapped[uuid.UUID] = mapped_column(UUID, nullable=False)

    status_code: Mapped[int] = mapped_column(Integer, nullable=True)

    data: Mapped[dict] = mapped_column(JSON, nullable=True)

    inserted_date: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=text("now()")
    )

    inserted_by: Mapped[str] = mapped_column(
        String(255), nullable=False, server_default=text("current_user")
    )
