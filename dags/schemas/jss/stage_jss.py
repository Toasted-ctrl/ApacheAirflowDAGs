import uuid
from datetime import datetime

from sqlalchemy import TIMESTAMP, UUID, BigInteger, String, Text, text
from sqlalchemy.orm import Mapped, mapped_column

from schemas.base import Base


class StageDataJSS(Base):
    __tablename__ = "stage_data"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID, primary_key=True, server_default=text("gen_random_uuid()")
    )

    ingest_id: Mapped[uuid.UUID] = mapped_column(UUID, nullable=False)

    job_id: Mapped[uuid.UUID] = mapped_column(UUID, nullable=False)

    j_id: Mapped[int] = mapped_column(BigInteger, nullable=False)

    j_description: Mapped[str] = mapped_column(Text, nullable=False)

    j_location: Mapped[str] = mapped_column(String(255), nullable=False)

    j_url: Mapped[str] = mapped_column(String(255), nullable=False)

    j_ctype: Mapped[str] = mapped_column(String(40), nullable=False)

    j_ptime: Mapped[str] = mapped_column(String(25), nullable=False)

    j_title: Mapped[str] = mapped_column(String(40), nullable=False)

    inserted_date: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=text("now()")
    )

    inserted_by: Mapped[str] = mapped_column(
        String(255), nullable=False, server_default=text("current_user")
    )
