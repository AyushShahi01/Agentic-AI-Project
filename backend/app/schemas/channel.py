import uuid
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, EmailStr, Field

from app.models.notification_channel import ChannelKind, ChannelStatus

MAX_RECIPIENTS = 20


class EmailSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    smtp_host: str = Field(min_length=1, max_length=255)
    smtp_port: int | None = Field(default=None, ge=1, le=65535)
    security: Literal["starttls", "ssl", "none"] = "starttls"
    username: str | None = Field(default=None, max_length=255)
    from_address: EmailStr
    default_recipients: list[EmailStr] = Field(default_factory=list, max_length=MAX_RECIPIENTS)


class ChannelCreate(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    kind: ChannelKind
    # EMAIL only.
    email: EmailSettings | None = None
    # SMTP password, or the webhook URL for SLACK/TEAMS/WEBHOOK.
    secret: str | None = Field(default=None, max_length=4096)
    is_active: bool = True


class ChannelUpdate(BaseModel):
    """Partial update. `secret` present replaces it (null or "" clears it)."""

    name: str | None = Field(default=None, min_length=1, max_length=100)
    email: EmailSettings | None = None
    secret: str | None = Field(default=None, max_length=4096)
    is_active: bool | None = None


class ChannelRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    name: str
    kind: ChannelKind
    settings: dict
    target: str = ""  # masked, filled by the API: "hooks.slack.com/…" or "smtp.gmail.com:587"
    has_secret: bool
    is_active: bool
    last_status: ChannelStatus
    last_message: str | None
    last_used_at: datetime | None
    created_at: datetime
    updated_at: datetime


class ChannelTestRequest(BaseModel):
    # EMAIL: where to send the test (default: the channel's default recipients).
    to: list[EmailStr] = Field(default_factory=list, max_length=MAX_RECIPIENTS)


class ChannelTestResult(BaseModel):
    ok: bool
    message: str
    status: ChannelStatus
