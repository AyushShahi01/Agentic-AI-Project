"""Deterministic failure classification from task log text.

Pure: no database, HTTP, or framework imports. Rules are checked in order and the first match
wins, so specific causes (bad data, auth) are listed before generic ones (code errors).
"""

import re
from dataclasses import asdict, dataclass
from enum import StrEnum


class FailureCategory(StrEnum):
    AUTH = "AUTH"
    DATA_INTEGRITY = "DATA_INTEGRITY"
    SCHEMA = "SCHEMA"
    CODE_BUG = "CODE_BUG"
    RESOURCE = "RESOURCE"
    TIMEOUT = "TIMEOUT"
    TRANSIENT_NETWORK = "TRANSIENT_NETWORK"
    UPSTREAM_MISSING = "UPSTREAM_MISSING"
    UNKNOWN = "UNKNOWN"


CATEGORY_LABELS: dict[FailureCategory, str] = {
    FailureCategory.AUTH: "Login or permission problem",
    FailureCategory.DATA_INTEGRITY: "Bad data (constraint violation)",
    FailureCategory.SCHEMA: "Table or column changed",
    FailureCategory.CODE_BUG: "Code error",
    FailureCategory.RESOURCE: "Out of memory or disk",
    FailureCategory.TIMEOUT: "Timeout",
    FailureCategory.TRANSIENT_NETWORK: "Network glitch",
    FailureCategory.UPSTREAM_MISSING: "Missing input",
    FailureCategory.UNKNOWN: "Unknown",
}

RETRYABLE: frozenset[FailureCategory] = frozenset(
    {
        FailureCategory.RESOURCE,
        FailureCategory.TIMEOUT,
        FailureCategory.TRANSIENT_NETWORK,
        FailureCategory.UPSTREAM_MISSING,
        FailureCategory.UNKNOWN,
    }
)


@dataclass(frozen=True)
class _Rule:
    name: str
    category: FailureCategory
    pattern: re.Pattern[str]
    confidence: float


def _rule(name: str, category: FailureCategory, regex: str, confidence: float = 0.9) -> _Rule:
    return _Rule(name, category, re.compile(regex, re.IGNORECASE), confidence)


_C = FailureCategory
RULES: tuple[_Rule, ...] = (
    _rule(
        "auth_failure",
        _C.AUTH,
        r"authentication failed|invalid credentials|permission denied|access denied|"
        r"\b401\b.*unauthori[sz]ed|unauthori[sz]ed.*\b401\b|\b403\b.*forbidden|forbidden.*\b403\b|"
        r"(?:token|credentials?) (?:has |have )?expired|InvalidClientTokenId|"
        r"password authentication failed",
    ),
    _rule(
        "constraint_violation",
        _C.DATA_INTEGRITY,
        r"UniqueViolation|duplicate key|violates (?:unique|foreign key|not-null|check) constraint|"
        r"IntegrityError|ForeignKeyViolation|NotNullViolation|CheckViolation",
        0.95,
    ),
    _rule(
        "schema_change",
        _C.SCHEMA,
        r"UndefinedColumn|UndefinedTable|column \S+ (?:of relation \S+ )?does not exist|"
        r"relation \S+ does not exist|no such (?:table|column)|schema mismatch|"
        r"invalid identifier|Unknown column",
    ),
    _rule(
        "out_of_resources",
        _C.RESOURCE,
        r"MemoryError|OOMKilled|out of memory|No space left on device|"
        r"exit code (?:-9|137)|return code -9|\bSIGKILL\b",
    ),
    _rule(
        "timeout",
        _C.TIMEOUT,
        r"AirflowTaskTimeout|AirflowSensorTimeout|TimeoutError|timed out|deadline exceeded|"
        r"ReadTimeout|ConnectTimeout|statement timeout|execution timeout",
    ),
    _rule(
        "network_error",
        _C.TRANSIENT_NETWORK,
        r"ConnectionError|Connection (?:refused|reset|aborted)|ConnectionResetError|"
        r"Temporary failure in name resolution|Name or service not known|"
        r"\b(?:502|503|504)\b|Bad Gateway|Service Unavailable|Gateway Time-?out|"
        r"Too Many Requests|\b429\b|could not connect to server|server closed the connection|"
        r"BrokenPipeError|SSL: UNEXPECTED_EOF",
    ),
    _rule(
        "missing_input",
        _C.UPSTREAM_MISSING,
        r"FileNotFoundError|NoSuchKey|No such file or directory|NoSuchBucket|"
        r"partition \S+ not found|upstream \S* ?not ready|path does not exist|"
        r"object does not exist",
    ),
    _rule(
        "code_error",
        _C.CODE_BUG,
        r"SyntaxError|NameError|ImportError|ModuleNotFoundError|AttributeError|TypeError|"
        r"KeyError|IndexError|ZeroDivisionError|ValueError|AssertionError|NotImplementedError",
        0.7,
    ),
)


@dataclass(frozen=True)
class Diagnosis:
    category: FailureCategory
    label: str
    retryable: bool
    confidence: float
    rule: str | None = None
    matched_line: str | None = None

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


def _unknown(reason: str) -> Diagnosis:
    return Diagnosis(
        category=FailureCategory.UNKNOWN,
        label=CATEGORY_LABELS[FailureCategory.UNKNOWN],
        retryable=True,
        confidence=0.0,
        rule=reason,
    )


def _line_at(text: str, index: int) -> str:
    start = text.rfind("\n", 0, index) + 1
    end = text.find("\n", index)
    line = text[start : end if end != -1 else len(text)].strip()
    return line[:300]


def classify(log_text: str | None) -> Diagnosis:
    """Classify a failure from its log text (first matching rule wins)."""
    if not log_text or not log_text.strip():
        return _unknown("no_log")
    for rule in RULES:
        match = rule.pattern.search(log_text)
        if match:
            return Diagnosis(
                category=rule.category,
                label=CATEGORY_LABELS[rule.category],
                retryable=rule.category in RETRYABLE,
                confidence=rule.confidence,
                rule=rule.name,
                matched_line=_line_at(log_text, match.start()),
            )
    return _unknown("no_rule_matched")


def classify_logs(logs: list[str | None]) -> Diagnosis:
    """Classify the first log (newest first) that yields a known category."""
    fallback = _unknown("no_log")
    for text in logs:
        diagnosis = classify(text)
        if diagnosis.category != FailureCategory.UNKNOWN:
            return diagnosis
        if text:
            fallback = diagnosis
    return fallback
