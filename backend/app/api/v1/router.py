from fastapi import APIRouter

from app.api.v1 import airflow, auth, automation, detection, health, incidents, users

api_router = APIRouter()
api_router.include_router(health.router)
api_router.include_router(auth.router)
api_router.include_router(users.router)
api_router.include_router(airflow.router)
api_router.include_router(incidents.router)
api_router.include_router(detection.router)
api_router.include_router(automation.router)
