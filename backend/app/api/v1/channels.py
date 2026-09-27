import uuid

from fastapi import APIRouter, status

from app.core.dependencies import AdminUser, ClientIp, DbSession, OperatorUser, ViewerUser
from app.models.notification_channel import NotificationChannel
from app.schemas.channel import (
    ChannelCreate,
    ChannelRead,
    ChannelTestRequest,
    ChannelTestResult,
    ChannelUpdate,
)
from app.services import notification_channel_service as service

router = APIRouter(prefix="/channels", tags=["channels"])


def _read(channel: NotificationChannel) -> ChannelRead:
    return ChannelRead.model_validate(channel).model_copy(
        update={"target": service.target(channel)}
    )


@router.get("", response_model=list[ChannelRead])
def list_channels(db: DbSession, _: ViewerUser) -> list[ChannelRead]:
    return [_read(c) for c in service.list_channels(db)]


@router.post("", response_model=ChannelRead, status_code=status.HTTP_201_CREATED)
def create_channel(
    body: ChannelCreate, db: DbSession, admin: AdminUser, ip: ClientIp
) -> ChannelRead:
    return _read(service.create_channel(db, body, actor=admin, ip_address=ip))


@router.get("/{channel_id}", response_model=ChannelRead)
def get_channel(channel_id: uuid.UUID, db: DbSession, _: ViewerUser) -> ChannelRead:
    return _read(service.get_channel(db, channel_id))


@router.patch("/{channel_id}", response_model=ChannelRead)
def update_channel(
    channel_id: uuid.UUID, body: ChannelUpdate, db: DbSession, admin: AdminUser, ip: ClientIp
) -> ChannelRead:
    return _read(service.update_channel(db, channel_id, body, actor=admin, ip_address=ip))


@router.delete("/{channel_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_channel(channel_id: uuid.UUID, db: DbSession, admin: AdminUser, ip: ClientIp) -> None:
    service.delete_channel(db, channel_id, actor=admin, ip_address=ip)


@router.post("/{channel_id}/test", response_model=ChannelTestResult)
def test_channel(
    channel_id: uuid.UUID,
    db: DbSession,
    operator: OperatorUser,
    ip: ClientIp,
    body: ChannelTestRequest | None = None,
) -> ChannelTestResult:
    result = service.send_test(
        db, channel_id, to=list(body.to) if body else [], actor=operator, ip_address=ip
    )
    channel = service.get_channel(db, channel_id)
    return ChannelTestResult(ok=result.ok, message=result.detail, status=channel.last_status)
