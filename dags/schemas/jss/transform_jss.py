import uuid
from datetime import datetime

from sqlalchemy import JSON, TIMESTAMP, UUID, Boolean, Float, String, text
from sqlalchemy.orm import Mapped, mapped_column

from schemas.base import Base


class TransformDataJSS(Base):
    __tablename__ = "transform_data"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID, primary_key=True, server_default=text("gen_random_uuid()")
    )

    job_id: Mapped[uuid.UUID] = mapped_column(UUID, nullable=False)

    ingest_id: Mapped[uuid.UUID] = mapped_column(UUID, nullable=False)

    stage_id: Mapped[uuid.UUID] = mapped_column(UUID, nullable=False)

    j_data: Mapped[dict] = mapped_column(JSON, nullable=False)

    j_score: Mapped[float] = mapped_column(Float, nullable=False)

    processed_by_user: Mapped[bool] = mapped_column(Boolean, nullable=False)

    inserted_date: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=text("now()")
    )

    inserted_by: Mapped[str] = mapped_column(
        String(255), nullable=False, server_default=text("current_user")
    )
