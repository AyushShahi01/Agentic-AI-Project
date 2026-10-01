import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

SERVICE_DIR = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class Settings:
    token: str
    weights_dir: Path
    weights_version: str | None  # None = newest directory under weights_dir
    device: str | None  # None = cuda if available else cpu
    max_body_bytes: int
    host: str
    port: int


@lru_cache
def get_settings() -> Settings:
    return Settings(
        token=os.getenv("ML_SERVICE_TOKEN", ""),
        weights_dir=Path(os.getenv("ML_WEIGHTS_DIR", str(SERVICE_DIR / "weights"))),
        weights_version=os.getenv("ML_WEIGHTS_VERSION") or None,
        device=os.getenv("ML_DEVICE") or None,
        max_body_bytes=int(os.getenv("ML_MAX_BODY_BYTES", str(256 * 1024))),
        # Logs can contain secrets: loopback only unless explicitly overridden.
        host=os.getenv("ML_HOST", "127.0.0.1"),
        port=int(os.getenv("ML_PORT", "8001")),
    )
