"""Deliver messages to people: email (SMTP), Slack, Microsoft Teams and generic webhooks.

Each sender either returns normally or raises NotifyError with a message fit for the UI.
No database access here; callers pass decrypted secrets.
"""

import smtplib
import ssl
from dataclasses import dataclass, field
from email.message import EmailMessage
from typing import Any
from urllib.parse import urlsplit

import httpx

# Tests swap these for fakes.
http_transport: httpx.BaseTransport | None = None
smtp_factory: Any = None  # callable(host, port, *, timeout, ssl) -> smtplib.SMTP-like


class NotifyError(Exception):
    """The message could not be delivered."""


@dataclass(frozen=True)
class Message:
    title: str
    text: str = ""
    link: str | None = None  # "Open in Agentic Ops"
    link_label: str = "Open in Agentic Ops"
    level: str = "INFO"
    extra: dict[str, Any] = field(default_factory=dict)  # generic webhook payload fields


# ---------------------------------------------------------------------- webhooks


def check_webhook_url(url: str, *, kind: str, allowed_hosts: list[str]) -> str:
    """Validate a Slack/Teams/webhook URL; returns it. Raises NotifyError."""
    parts = urlsplit(url or "")
    if parts.scheme not in ("http", "https") or not parts.hostname:
        raise NotifyError("The webhook link must start with https://")
    host = parts.hostname.lower()
    if kind == "SLACK" and (parts.scheme != "https" or host != "hooks.slack.com"):
        raise NotifyError("A Slack link must be an incoming webhook: https://hooks.slack.com/…")
    if kind == "TEAMS" and parts.scheme != "https":
        raise NotifyError("A Teams link must start with https://")
    allowed = {h.lower() for h in allowed_hosts}
    if allowed and host not in allowed:
        raise NotifyError(f"Host {host} is not in AUTOMATION_WEBHOOK_ALLOWED_HOSTS")
    return url


def masked_url(url: str) -> str:
    """hooks.slack.com/… : enough to recognise it, without the secret path."""
    parts = urlsplit(url or "")
    return f"{parts.hostname}/…" if parts.hostname else ""


def _post(url: str, payload: dict[str, Any], timeout: float) -> None:
    try:
        with httpx.Client(timeout=timeout, follow_redirects=False, transport=http_transport) as c:
            response = c.post(url, json=payload)
    except httpx.HTTPError as exc:
        raise NotifyError(
            f"Could not reach {urlsplit(url).hostname}: {type(exc).__name__}"
        ) from exc
    if not response.is_success:
        detail = response.text.strip()[:200]
        raise NotifyError(f"HTTP {response.status_code}" + (f": {detail}" if detail else ""))


_LEVEL_EMOJI = {
    "INFO": ":information_source:",
    "WARNING": ":warning:",
    "CRITICAL": ":rotating_light:",
}


def slack_payload(message: Message) -> dict[str, Any]:
    blocks: list[dict[str, Any]] = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"{_LEVEL_EMOJI.get(message.level, '')} *{message.title}*".strip(),
            },
        }
    ]
    if message.text:
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": message.text[:2900]}})
    if message.link:
        blocks.append(
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": message.link_label},
                        "url": message.link,
                    }
                ],
            }
        )
    fallback = message.title + (f"\n{message.text}" if message.text else "")
    return {"text": fallback[:3000], "blocks": blocks}


def teams_payload(message: Message) -> dict[str, Any]:
    """An Adaptive Card, the format Teams "Workflows" webhooks accept."""
    body: list[dict[str, Any]] = [
        {
            "type": "TextBlock",
            "text": message.title,
            "weight": "Bolder",
            "size": "Medium",
            "wrap": True,
            **({"color": "Attention"} if message.level == "CRITICAL" else {}),
        }
    ]
    if message.text:
        body.append({"type": "TextBlock", "text": message.text, "wrap": True})
    card: dict[str, Any] = {
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "type": "AdaptiveCard",
        "version": "1.4",
        "body": body,
    }
    if message.link:
        card["actions"] = [
            {"type": "Action.OpenUrl", "title": message.link_label, "url": message.link}
        ]
    return {
        "type": "message",
        "attachments": [
            {"contentType": "application/vnd.microsoft.card.adaptive", "content": card}
        ],
    }


def webhook_payload(message: Message) -> dict[str, Any]:
    return {
        "title": message.title,
        "message": message.text,
        "level": message.level,
        **({"link": message.link} if message.link else {}),
        **message.extra,
    }


def send_http(
    kind: str, url: str, message: Message, *, allowed_hosts: list[str], timeout: float
) -> None:
    check_webhook_url(url, kind=kind, allowed_hosts=allowed_hosts)
    payload = {
        "SLACK": slack_payload,
        "TEAMS": teams_payload,
        "WEBHOOK": webhook_payload,
    }[kind](message)
    _post(url, payload, timeout)


# ---------------------------------------------------------------------- email


def _smtp(host: str, port: int, *, timeout: float, use_ssl: bool) -> Any:
    if smtp_factory is not None:
        return smtp_factory(host, port, timeout=timeout, ssl=use_ssl)
    if use_ssl:
        return smtplib.SMTP_SSL(host, port, timeout=timeout, context=ssl.create_default_context())
    return smtplib.SMTP(host, port, timeout=timeout)


def send_email(
    settings: dict[str, Any],
    password: str | None,
    to: list[str],
    message: Message,
    *,
    timeout: float,
) -> None:
    if not to:
        raise NotifyError("No recipients: add email addresses to the block or the channel")
    security = settings.get("security", "starttls")
    email = EmailMessage()
    email["Subject"] = message.title[:250]
    email["From"] = settings.get("from_address") or settings.get("username")
    email["To"] = ", ".join(to)
    text = message.text or message.title
    if message.link:
        text += f"\n\n{message.link_label}: {message.link}"
    email.set_content(text)
    try:
        with _smtp(
            settings["smtp_host"],
            int(settings.get("smtp_port") or (465 if security == "ssl" else 587)),
            timeout=timeout,
            use_ssl=security == "ssl",
        ) as smtp:
            if security == "starttls":
                smtp.starttls(context=ssl.create_default_context())
            if settings.get("username") and password:
                smtp.login(settings["username"], password)
            smtp.send_message(email)
    except smtplib.SMTPAuthenticationError as exc:
        raise NotifyError("Email login failed: check the username and password") from exc
    except smtplib.SMTPRecipientsRefused as exc:
        raise NotifyError(f"The mail server refused: {', '.join(exc.recipients)}") from exc
    except (smtplib.SMTPException, OSError) as exc:
        raise NotifyError(f"Could not send email: {exc}".strip()[:300]) from exc
