"""Log preprocessing shared by training and inference.

Pure (standard library only) so the training scripts, the service and the tests all produce the
same window for the same log:

1. strip ANSI codes and Airflow line prefixes (``[timestamp] {file.py:NN} LEVEL -``);
2. mask volatile tokens (UUIDs, hex ids, IPs, long numbers, directory paths) with placeholders;
3. select a window that fits the model: the last traceback block, error lines and the last 30
   lines, deduplicated and packed from the tail by priority until the token budget is full.
"""

import re
from collections.abc import Callable
from dataclasses import dataclass

MAX_TOKENS = 510  # 512 minus [CLS] and [SEP]
TAIL_LINES = 30
MAX_LINE_CHARS = 400
SIGNAL_LINE_CHARS = 300

_ANSI = re.compile(r"\x1b\[[0-9;?]*[ -/]*[@-~]")
# Airflow 2: "[2026-09-27T00:34:40.930+0000] {taskinstance.py:3313} ERROR - msg"
# Airflow 3: "[2026-09-27 00:34:40] ERROR - msg" / "2026-09-27T00:34:40Z [error] msg"
_PREFIX = re.compile(
    r"^\s*(?:\[\d{4}-\d{2}-\d{2}[T ][^\]]*\]|\d{4}-\d{2}-\d{2}[T ][\d:.,]+(?:Z|[+-]\d{2}:?\d{2})?)"
    r"\s*(?:\{[^}]*\}\s*)?"
)
_LEVEL = re.compile(r"^\[?(?P<level>[A-Za-z]+)\]?(?:\s*-\s+|:\s+|\s+|$)")
_LEVELS = {"DEBUG", "INFO", "WARNING", "WARN", "ERROR", "CRITICAL", "FATAL", "EXCEPTION"}

_MASKS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b", re.I), "<uuid>"),
    (re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}(?::\d+)?\b"), "<ip>"),
    # directories of absolute / relative paths; the last two segments carry signal
    # (".../requests/adapters.py"), so they are kept.
    (
        re.compile(r"(?:[A-Za-z]:\\|/|\.{1,2}/)(?:[\w.\-~@=+]+[/\\])+(?=[\w.\-~@=+]+[/\\][\w.\-]+)"),
        "<path>/",
    ),
    (re.compile(r"\b0x[0-9a-f]+\b", re.I), "<hex>"),
    (re.compile(r"\b(?=[0-9a-f]*\d)(?=[0-9a-f]*[a-f])[0-9a-f]{8,}\b", re.I), "<hex>"),
    (re.compile(r"\b\d{4,}\b"), "<num>"),
]

_TRACEBACK = re.compile(r"^Traceback \(most recent call last\):")
_CHAIN = re.compile(
    r"^(?:During handling of the above exception|The above exception was the direct cause)"
)
# "ValueError: x", "psycopg2.errors.UniqueViolation: y", "botocore.exceptions.ClientError: z"
_EXCEPTION_LINE = re.compile(
    r"^(?:[A-Za-z_]\w*\.)*(?=[A-Za-z_])\w*(?:Error|Exception|Exit|Timeout|Interrupt|Violation|"
    r"Failure|Fault|Denied|Killed|NotFound|Missing)\b(?::|$)"
)
_ERROR_HINT = re.compile(r"ERROR|CRITICAL|FATAL|Exception|Error:|Traceback", re.I)
_NOISE = re.compile(r"^[\s^~\-=*_|]*$")
_FRAME = re.compile(r'^\s*File "[^"]*", line \d+')
_TOKEN = re.compile(r"\w+|[^\w\s]")


@dataclass(frozen=True)
class Line:
    text: str  # prefix-stripped, unmasked (used for display)
    level: str | None
    prefixed: bool  # True if this line started a new log record


@dataclass(frozen=True)
class Window:
    text: str  # masked lines joined with newlines; what the model sees
    signal_line: str | None  # unmasked highest-signal line (for display/evidence)
    lines: int
    tokens: int


def approx_tokens(text: str) -> int:
    """Cheap WordPiece estimate: words + punctuation, long words cost extra pieces."""
    return sum(1 + len(t) // 8 for t in _TOKEN.findall(text))


def strip_ansi(text: str) -> str:
    return _ANSI.sub("", text)


def mask(text: str) -> str:
    for pattern, replacement in _MASKS:
        text = pattern.sub(replacement, text)
    return text


def split_lines(log: str) -> list[Line]:
    out: list[Line] = []
    for raw in strip_ansi(log).replace("\r\n", "\n").replace("\r", "\n").split("\n"):
        match = _PREFIX.match(raw)
        if not match:
            out.append(Line(raw.rstrip()[:MAX_LINE_CHARS], None, False))
            continue
        body, level = raw[match.end() :], None
        level_match = _LEVEL.match(body)
        if level_match and level_match.group("level").upper() in _LEVELS:
            level = level_match.group("level").upper()
            body = body[level_match.end() :]
        out.append(Line(body.rstrip()[:MAX_LINE_CHARS], level, True))
    return out


def _is_exception(text: str) -> bool:
    return bool(_EXCEPTION_LINE.match(text.strip())) and not _FRAME.match(text)


def _last_traceback_block(lines: list[Line]) -> range | None:
    """Indices of the last (possibly chained) traceback, through its final exception line."""
    start = next((i for i in range(len(lines) - 1, -1, -1) if _TRACEBACK.match(
        lines[i].text.strip())), None)
    if start is None:
        return None
    # Walk back over chained tracebacks ("During handling of the above exception...").
    i = start - 1
    while i >= 0:
        stripped = lines[i].text.strip()
        if _TRACEBACK.match(stripped) or _CHAIN.match(stripped) or (
            lines[i].level is None and not lines[i].prefixed and stripped
        ):
            if _TRACEBACK.match(stripped):
                start = i
            i -= 1
            continue
        if not stripped:
            i -= 1
            continue
        break
    end = start
    for j in range(start + 1, len(lines)):
        if lines[j].prefixed:
            break
        end = j
        if j - start > 200:
            break
    return range(start, end + 1)


def signal_line(lines: list[Line]) -> str | None:
    block = _last_traceback_block(lines)
    if block is not None:
        for i in reversed(block):
            if _is_exception(lines[i].text):
                return lines[i].text.strip()[:SIGNAL_LINE_CHARS]
    for line in reversed(lines):
        if _is_exception(line.text):
            return line.text.strip()[:SIGNAL_LINE_CHARS]
    for line in reversed(lines):
        if line.level in {"ERROR", "CRITICAL", "FATAL", "EXCEPTION"} and line.text.strip():
            return line.text.strip()[:SIGNAL_LINE_CHARS]
    for line in reversed(lines):
        if line.text.strip() and not _NOISE.match(line.text):
            return line.text.strip()[:SIGNAL_LINE_CHARS]
    return None


def window(
    log: str | None,
    *,
    max_tokens: int = MAX_TOKENS,
    count_tokens: Callable[[str], int] = approx_tokens,
) -> Window:
    """Select the most informative lines of `log` that fit in `max_tokens`."""
    if not log or not log.strip():
        return Window(text="", signal_line=None, lines=0, tokens=0)
    lines = split_lines(log)
    n = len(lines)

    # priority: 0 = exception lines, 1 = rest of traceback + error lines, 2 = tail context
    priority: dict[int, int] = {}
    block = _last_traceback_block(lines)
    if block is not None:
        for i in block:
            priority[i] = 0 if _is_exception(lines[i].text) else 1
    for i, line in enumerate(lines):
        if _is_exception(line.text):
            priority[i] = 0
        elif i not in priority and (
            line.level in {"ERROR", "CRITICAL", "FATAL", "EXCEPTION"}
            or _ERROR_HINT.search(line.text)
        ):
            priority[i] = 1
    for i in range(max(0, n - TAIL_LINES), n):
        priority.setdefault(i, 2)

    chosen: set[int] = set()
    seen: set[str] = set()
    masked: dict[int, str] = {}
    used = 0
    for level in (0, 1, 2):
        for i in sorted((i for i, p in priority.items() if p == level), reverse=True):
            text = lines[i].text.strip()
            if not text or _NOISE.match(text):
                continue
            m = mask(text)
            if m in seen:
                continue
            cost = count_tokens(m) + 1
            if used + cost > max_tokens:
                continue  # a shorter, lower-priority line may still fit
            seen.add(m)
            masked[i] = m
            chosen.add(i)
            used += cost
    ordered = sorted(chosen)
    return Window(
        text="\n".join(masked[i] for i in ordered),
        signal_line=signal_line(lines),
        lines=len(ordered),
        tokens=used,
    )
