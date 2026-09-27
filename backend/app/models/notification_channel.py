import uuid
from datetime import datetime
from enum import StrEnum
from typing import Any

from sqlalchemy import JSON, ForeignKey, String, Text, Uuid
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base, TimestampMixin, UTCDateTime
from app.models.airflow import _enum


class ChannelKind(StrEnum):
    EMAIL = "EMAIL"
    SLACK = "SLACK"
    TEAMS = "TEAMS"
    WEBHOOK = "WEBHOOK"


class ChannelStatus(StrEnum):
    UNKNOWN = "UNKNOWN"
    OK = "OK"
    FAILED = "FAILED"


class NotificationChannel(TimestampMixin, Base):
    """A way to reach people (email account, Slack/Teams channel, webhook) in the catalog.

    Not polled: sending test messages on a timer would spam people, so the status reflects the
    last test or real send.
    """

    __tablename__ = "notification_channels"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(100), unique=True)
    kind: Mapped[ChannelKind] = mapped_column(_enum(ChannelKind, "channel_kind"))
    # Non-secret settings, e.g. EMAIL: smtp_host, smtp_port, security, username, from_address,
    # default_recipients.
    settings: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    # SMTP password, or the webhook URL (for Slack/Teams/webhook the URL is the credential).
    encrypted_secret: Mapped[str | None] = mapped_column(Text)
    is_active: Mapped[bool] = mapped_column(default=True)
    last_status: Mapped[ChannelStatus] = mapped_column(
        _enum(ChannelStatus, "channel_status"), default=ChannelStatus.UNKNOWN
    )
    last_message: Mapped[str | None] = mapped_column(Text)
    last_used_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    created_by: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("users.id", ondelete="SET NULL")
    )

    @property
    def has_secret(self) -> bool:
        return bool(self.encrypted_secret)
