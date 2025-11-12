from sqlalchemy import Integer, String, DateTime, Boolean, ForeignKey
from sqlalchemy.orm import relationship, Mapped, mapped_column
from datetime import datetime
from .database import Base

class Technician(Base):
    __tablename__ = "technicians"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    name: Mapped[str] = mapped_column(String(120), nullable=False, index=True)
    team: Mapped[str] = mapped_column(String(80), nullable=True, index=True)

    work_orders: Mapped[list["WorkOrder"]] = relationship("WorkOrder", back_populates="technician", cascade="all, delete-orphan")

class WorkOrder(Base):
    __tablename__ = "work_orders"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    technician_id: Mapped[int] = mapped_column(ForeignKey("technicians.id", ondelete="CASCADE"), index=True)
    opened_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    closed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    duration_minutes: Mapped[int | None] = mapped_column(Integer, nullable=True)
    sla_met: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    category: Mapped[str | None] = mapped_column(String(80), nullable=True)

    technician: Mapped["Technician"] = relationship("Technician", back_populates="work_orders")
