from functools import lru_cache

from cryptography.fernet import Fernet, InvalidToken, MultiFernet

from app.core.config import get_settings


class SecretDecryptionError(Exception):
    """Stored secret cannot be decrypted with any configured ENCRYPTION_KEY."""


@lru_cache
def _fernet() -> MultiFernet:
    keys = get_settings().encryption_keys
    if not keys:
        raise RuntimeError("ENCRYPTION_KEY is empty")
    return MultiFernet([Fernet(key) for key in keys])


def encrypt_secret(plaintext: str) -> str:
    return _fernet().encrypt(plaintext.encode()).decode()


def decrypt_secret(ciphertext: str) -> str:
    try:
        return _fernet().decrypt(ciphertext.encode()).decode()
    except InvalidToken as exc:
        raise SecretDecryptionError("Stored secret could not be decrypted") from exc


def rotate_secret(ciphertext: str) -> str:
    """Re-encrypt with the primary (first) key."""
    return _fernet().rotate(ciphertext.encode()).decode()
