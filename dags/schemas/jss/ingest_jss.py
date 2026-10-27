import uuid
from datetime import datetime

from sqlalchemy import JSON, TIMESTAMP, UUID, String, text
from sqlalchemy.orm import Mapped, mapped_column

from schemas.base import Base


class IngestJobsJSS(Base):
    __tablename__ = "ingest_jobs"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID, primary_key=True, server_default=text("gen_random_uuid()")
    )

    username: Mapped[str] = mapped_column(String(255), nullable=False)

    filters: Mapped[dict] = mapped_column(JSON, nullable=True)

    api_key: Mapped[str] = mapped_column(String(255), nullable=False)

    url: Mapped[str] = mapped_column(String(255), nullable=False)

    inserted_date: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=text("now()")
    )

    inserted_by: Mapped[str] = mapped_column(
        String(255), nullable=False, server_default=text("current_user")
    )


class IngestDataJSS(Base):
    __tablename__ = "ingest_data"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID, primary_key=True, server_default=text("gen_random_uuid()")
    )

    job_id: Mapped[uuid.UUID] = mapped_column(
        UUID, primary_key=True, server_default=text("gen_random_uuid()")
    )

    data: Mapped[dict] = mapped_column(JSON, nullable=False)

    inserted_date: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=text("now()")
    )

    inserted_by: Mapped[str] = mapped_column(
        String(255), nullable=False, server_default=text("current_user")
    )
