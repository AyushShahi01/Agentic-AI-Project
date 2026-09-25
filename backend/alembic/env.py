from logging.config import fileConfig

from alembic import context

from app import models  # noqa: F401  (register all models on Base.metadata)
from app.core.config import get_settings
from app.db.base import Base
from app.db.session import create_db_engine

config = context.config

if config.config_file_name is not None and config.attributes.get("configure_logger", True):
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def _database_url() -> str:
    return config.get_main_option("sqlalchemy.url") or get_settings().DATABASE_URL


def _configure_kwargs() -> dict:
    return {
        "target_metadata": target_metadata,
        "render_as_batch": True,  # SQLite needs batch mode for ALTER TABLE
        "compare_type": True,
    }


def run_migrations_offline() -> None:
    context.configure(
        url=_database_url(),
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        **_configure_kwargs(),
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connection = config.attributes.get("connection")
    if connection is not None:
        context.configure(connection=connection, **_configure_kwargs())
        with context.begin_transaction():
            context.run_migrations()
        return

    engine = create_db_engine(_database_url())
    with engine.connect() as connection:
        context.configure(connection=connection, **_configure_kwargs())
        with context.begin_transaction():
            context.run_migrations()
    engine.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
