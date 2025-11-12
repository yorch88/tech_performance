from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class TechnicianBase(BaseModel):
    name: str
    team: Optional[str] = None

class TechnicianCreate(TechnicianBase):
    pass

class Technician(TechnicianBase):
    id: int
    class Config:
        from_attributes = True

class WorkOrderBase(BaseModel):
    technician_id: int
    opened_at: Optional[datetime] = None
    closed_at: Optional[datetime] = None
    duration_minutes: Optional[int] = None
    sla_met: Optional[bool] = True
    category: Optional[str] = None

class WorkOrderCreate(WorkOrderBase):
    pass

class WorkOrder(WorkOrderBase):
    id: int
    class Config:
        from_attributes = True

class KPIResponse(BaseModel):
    technician_id: int
    period: str
    total_orders: int
    avg_duration_min: float | None
    sla_compliance: float | None
