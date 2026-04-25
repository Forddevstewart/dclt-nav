import uuid
import enum
from datetime import date, datetime

from sqlalchemy import (
    Date, DateTime, Enum, Float, ForeignKey,
    Integer, JSON, String,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class DocType(str, enum.Enum):
    agenda = "agenda"
    minutes = "minutes"
    packet = "packet"
    supplemental = "supplemental"
    unknown = "unknown"


class DocStatus(str, enum.Enum):
    pending = "pending"
    downloaded = "downloaded"
    error = "error"


class Document(Base):
    __tablename__ = "documents"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    url: Mapped[str] = mapped_column(String, nullable=False, unique=True)
    doc_type: Mapped[DocType] = mapped_column(Enum(DocType), nullable=False, default=DocType.unknown)
    committee_name: Mapped[str | None] = mapped_column(String, nullable=True)
    meeting_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    published_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    downloaded_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    local_path: Mapped[str | None] = mapped_column(String, nullable=True)
    checksum: Mapped[str | None] = mapped_column(String(64), nullable=True)
    file_size_bytes: Mapped[int | None] = mapped_column(Integer, nullable=True)
    status: Mapped[DocStatus] = mapped_column(Enum(DocStatus), nullable=False, default=DocStatus.pending)

    def __repr__(self) -> str:
        return (
            f"<Document id={self.id!r} committee={self.committee_name!r} "
            f"meeting_date={self.meeting_date} status={self.status}>"
        )


class PersonMention(Base):
    __tablename__ = "person_mentions"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    document_id: Mapped[str] = mapped_column(String(36), ForeignKey("documents.id"), nullable=False)
    raw_name: Mapped[str] = mapped_column(String, nullable=False)
    role: Mapped[str | None] = mapped_column(String, nullable=True)
    affiliation: Mapped[str | None] = mapped_column(String, nullable=True)
    pattern_source: Mapped[str] = mapped_column(String, nullable=False)
    confidence: Mapped[float] = mapped_column(Float, nullable=False)
    page_number: Mapped[int | None] = mapped_column(Integer, nullable=True)
    source_json_path: Mapped[str | None] = mapped_column(String, nullable=True)

    document: Mapped["Document"] = relationship("Document")


class PersonCandidate(Base):
    __tablename__ = "person_candidates"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    canonical_name: Mapped[str] = mapped_column(String, nullable=False, unique=True)
    aliases: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    mention_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    committees: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    roles: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    resolution_status: Mapped[str | None] = mapped_column(String, nullable=True)
