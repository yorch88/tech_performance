from fastapi import FastAPI
from .database import engine, Base
from .routers import technicians

app = FastAPI(title="Performance Técnicos API", version="0.1.0")

# Crear tablas al inicio (usa Alembic para producción)
Base.metadata.create_all(bind=engine)

@app.get("/health", tags=["health"])
def health():
    return {"status": "ok"}

app.include_router(technicians.router)
