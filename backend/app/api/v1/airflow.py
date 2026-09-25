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
from app.schemas.airflow import (
    AirflowConnCreate,
    AirflowConnRead,
    AirflowConnTestRequest,
    AirflowConnUpdate,
    ConnectionTestResultRead,
    DagMonitorUpdate,
    DagRead,
    DagSyncResult,
)
from app.schemas.common import Page
from app.services import airflow_service
from app.services.airflow_service import ProbeOutcome

router = APIRouter(prefix="/airflow", tags=["airflow"])


def _test_result(outcome: ProbeOutcome) -> ConnectionTestResultRead:
    r = outcome.result
    return ConnectionTestResultRead(
        status=r.status,
        ok=r.ok,
        message=r.message,
        latency_ms=r.latency_ms,
        api_version=r.api_version,
        airflow_version=r.airflow_version,
        checked_at=outcome.checked_at,
    )


# ---------------------------------------------------------------------- connections


@router.get("/connections", response_model=Page[AirflowConnRead])
def list_connections(db: DbSession, _: ViewerUser, page: PageParams) -> Page[AirflowConnRead]:
    items, total = airflow_service.list_connections(db, limit=page.limit, offset=page.offset)
    return Page[AirflowConnRead](
        items=[AirflowConnRead.model_validate(c) for c in items],
        total=total,
        limit=page.limit,
        offset=page.offset,
    )


@router.post("/connections", response_model=AirflowConnRead, status_code=status.HTTP_201_CREATED)
def create_connection(
    body: AirflowConnCreate, db: DbSession, admin: AdminUser, ip: ClientIp
) -> AirflowConnRead:
    conn = airflow_service.create_connection(db, body, actor=admin, ip_address=ip)
    return AirflowConnRead.model_validate(conn)


@router.get("/connections/{connection_id}", response_model=AirflowConnRead)
def get_connection(connection_id: uuid.UUID, db: DbSession, _: ViewerUser) -> AirflowConnRead:
    return AirflowConnRead.model_validate(airflow_service.get_connection(db, connection_id))


@router.patch("/connections/{connection_id}", response_model=AirflowConnRead)
def update_connection(
    connection_id: uuid.UUID,
    body: AirflowConnUpdate,
    db: DbSession,
    admin: AdminUser,
    ip: ClientIp,
) -> AirflowConnRead:
    conn = airflow_service.update_connection(db, connection_id, body, actor=admin, ip_address=ip)
    return AirflowConnRead.model_validate(conn)


@router.delete("/connections/{connection_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_connection(
    connection_id: uuid.UUID, db: DbSession, admin: AdminUser, ip: ClientIp
) -> None:
    airflow_service.delete_connection(db, connection_id, actor=admin, ip_address=ip)


# ---------------------------------------------------------------------- testing


@router.post("/test-connection", response_model=ConnectionTestResultRead)
def test_unsaved_connection(
    body: AirflowConnTestRequest, db: DbSession, operator: OperatorUser, ip: ClientIp
) -> ConnectionTestResultRead:
    outcome = airflow_service.check_unsaved_connection(db, body, actor=operator, ip_address=ip)
    return _test_result(outcome)


@router.post("/connections/{connection_id}/test", response_model=ConnectionTestResultRead)
def test_saved_connection(
    connection_id: uuid.UUID, db: DbSession, operator: OperatorUser, ip: ClientIp
) -> ConnectionTestResultRead:
    outcome = airflow_service.check_saved_connection(
        db, connection_id, actor=operator, ip_address=ip
    )
    return _test_result(outcome)


# ---------------------------------------------------------------------- DAGs


@router.post("/connections/{connection_id}/sync-dags", response_model=DagSyncResult)
def sync_dags(
    connection_id: uuid.UUID, db: DbSession, operator: OperatorUser, ip: ClientIp
) -> DagSyncResult:
    outcome = airflow_service.sync_dags(db, connection_id, actor=operator, ip_address=ip)
    return DagSyncResult(
        total=outcome.total,
        created=outcome.created,
        updated=outcome.updated,
        missing=outcome.missing,
        synced_at=outcome.synced_at,
    )


@router.get("/connections/{connection_id}/dags", response_model=Page[DagRead])
def list_dags(
    connection_id: uuid.UUID,
    db: DbSession,
    _: ViewerUser,
    page: PageParams,
    monitored: bool | None = None,
    tag: str | None = None,
    search: str | None = None,
) -> Page[DagRead]:
    items, total = airflow_service.list_dags(
        db,
        connection_id,
        monitored=monitored,
        tag=tag,
        search=search,
        limit=page.limit,
        offset=page.offset,
    )
    return Page[DagRead](
        items=[DagRead.model_validate(d) for d in items],
        total=total,
        limit=page.limit,
        offset=page.offset,
    )


@router.patch("/dags/{dag_pk}", response_model=DagRead)
def set_dag_monitored(
    dag_pk: uuid.UUID,
    body: DagMonitorUpdate,
    db: DbSession,
    operator: OperatorUser,
    ip: ClientIp,
) -> DagRead:
    changes = body.model_dump(include=body.model_fields_set)
    dag = airflow_service.update_dag(db, dag_pk, changes, actor=operator, ip_address=ip)
    return DagRead.model_validate(dag)
