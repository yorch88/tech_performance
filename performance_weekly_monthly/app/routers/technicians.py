from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func, select, case
from datetime import datetime
from ..database import get_session
from .. import models, schemas

router = APIRouter(prefix="/technicians", tags=["technicians"])

@router.post("/", response_model=schemas.Technician, status_code=201)
def create_technician(payload: schemas.TechnicianCreate, session: Session = Depends(get_session)):
    tech = models.Technician(**payload.model_dump())
    session.add(tech)
    session.flush()  # obtener id
    return tech

@router.get("/", response_model=list[schemas.Technician])
def list_technicians(session: Session = Depends(get_session)):
    return session.scalars(select(models.Technician).order_by(models.Technician.id)).all()

@router.post("/work-orders", response_model=schemas.WorkOrder, status_code=201)
def create_work_order(payload: schemas.WorkOrderCreate, session: Session = Depends(get_session)):
    tech = session.get(models.Technician, payload.technician_id)
    if not tech:
        raise HTTPException(status_code=404, detail="Technician not found")

    wo = models.WorkOrder(**payload.model_dump())

    if wo.closed_at and not wo.duration_minutes:
        wo.duration_minutes = int((wo.closed_at - (wo.opened_at or datetime.utcnow())).total_seconds() // 60)

    session.add(wo)
    session.flush()
    return wo

@router.get("/kpi", response_model=list[schemas.KPIResponse])
def kpi_by_technician(period: str = "daily", session: Session = Depends(get_session)):
    # Promedio de cumplimiento SLA usando CASE (compatible SQL standard; MySQL lo traduce correctamente)
    sla_case = case((models.WorkOrder.sla_met == True, 1), else_=0)
    stmt = (
        select(
            models.WorkOrder.technician_id.label("technician_id"),
            func.count(models.WorkOrder.id).label("total_orders"),
            func.avg(models.WorkOrder.duration_minutes).label("avg_duration_min"),
            func.avg(sla_case).label("sla_compliance"),
        )
        .group_by(models.WorkOrder.technician_id)
        .order_by(models.WorkOrder.technician_id)
    )
    rows = session.execute(stmt).all()

    return [
        schemas.KPIResponse(
            technician_id=r.technician_id,
            period=period,
            total_orders=r.total_orders,
            avg_duration_min=float(r.avg_duration_min) if r.avg_duration_min is not None else None,
            sla_compliance=float(r.sla_compliance) if r.sla_compliance is not None else None,
        )
        for r in rows
    ]
