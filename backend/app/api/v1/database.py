import uuid

from fastapi import APIRouter, status

from app.core.dependencies import (
    AdminUser,
    ClientIp,
    DbSession,
    OperatorUser,
    PageParams,
    ViewerUser,
)
from app.schemas.common import Page
from app.schemas.database import (
    DbConnCreate,
    DbConnRead,
    DbConnTestRequest,
    DbConnTestResultRead,
    DbConnUpdate,
)
from app.services import database_connection_service
from app.services.database_connection_service import DbProbeOutcome

router = APIRouter(prefix="/databases", tags=["databases"])


def _test_result(outcome: DbProbeOutcome) -> DbConnTestResultRead:
    r = outcome.result
    return DbConnTestResultRead(
        status=r.status,
        ok=r.ok,
        message=r.message,
        latency_ms=r.latency_ms,
        server_version=r.server_version,
        checked_at=outcome.checked_at,
    )


@router.get("/connections", response_model=Page[DbConnRead])
def list_connections(db: DbSession, _: ViewerUser, page: PageParams) -> Page[DbConnRead]:
    items, total = database_connection_service.list_connections(
        db, limit=page.limit, offset=page.offset
    )
    return Page[DbConnRead](
        items=[DbConnRead.model_validate(c) for c in items],
        total=total,
        limit=page.limit,
        offset=page.offset,
    )


@router.post("/connections", response_model=DbConnRead, status_code=status.HTTP_201_CREATED)
def create_connection(
    body: DbConnCreate, db: DbSession, admin: AdminUser, ip: ClientIp
) -> DbConnRead:
    conn = database_connection_service.create_connection(db, body, actor=admin, ip_address=ip)
    return DbConnRead.model_validate(conn)


@router.get("/connections/{connection_id}", response_model=DbConnRead)
def get_connection(connection_id: uuid.UUID, db: DbSession, _: ViewerUser) -> DbConnRead:
    return DbConnRead.model_validate(database_connection_service.get_connection(db, connection_id))


@router.patch("/connections/{connection_id}", response_model=DbConnRead)
def update_connection(
    connection_id: uuid.UUID, body: DbConnUpdate, db: DbSession, admin: AdminUser, ip: ClientIp
) -> DbConnRead:
    conn = database_connection_service.update_connection(
        db, connection_id, body, actor=admin, ip_address=ip
    )
    return DbConnRead.model_validate(conn)


@router.delete("/connections/{connection_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_connection(
    connection_id: uuid.UUID, db: DbSession, admin: AdminUser, ip: ClientIp
) -> None:
    database_connection_service.delete_connection(db, connection_id, actor=admin, ip_address=ip)


@router.post("/test-connection", response_model=DbConnTestResultRead)
def test_unsaved_connection(
    body: DbConnTestRequest, db: DbSession, operator: OperatorUser, ip: ClientIp
) -> DbConnTestResultRead:
    outcome = database_connection_service.check_unsaved_connection(
        db, body, actor=operator, ip_address=ip
    )
    return _test_result(outcome)


@router.post("/connections/{connection_id}/test", response_model=DbConnTestResultRead)
def test_saved_connection(
    connection_id: uuid.UUID, db: DbSession, operator: OperatorUser, ip: ClientIp
) -> DbConnTestResultRead:
    outcome = database_connection_service.check_saved_connection(
        db, connection_id, actor=operator, ip_address=ip
    )
    return _test_result(outcome)
