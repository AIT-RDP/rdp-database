"""
Provides the common SQL test fixtures to specifically assess the database layout

The logic was originally developed in the Edge4Energy activities. Make sure that the .env file is not populated by the
test runner itself, as the runner implementation does no variables replacement and the runner environment would take
precedence.
"""
import os
import urllib.parse

import alembic.config
import pytest
import sqlalchemy as sql
import dotenv

dotenv.load_dotenv(dotenv_path=".env")


@pytest.fixture()
def sql_engine_postgres() -> sql.engine.Engine:
    """Returns the engine that connects with the postgres admin user"""

    return get_user_engine(os.environ["POSTGRES_USER"], os.environ["POSTGRES_PASSWORD"])


@pytest.fixture()
def sql_engine_data_source() -> sql.engine.Engine:
    """Returns the engine that connects with the postgres admin user"""

    return get_user_engine(os.environ["POSTGRES_DATA_SOURCE_USER"], os.environ["POSTGRES_DATA_SOURCE_PASSWORD"])


@pytest.fixture()
def sql_engine_private_vis() -> sql.engine.Engine:
    """Returns the engine that connects with the postgres admin user"""

    return get_user_engine(os.environ["POSTGRES_DATA_VIS_USER"], os.environ["POSTGRES_DATA_VIS_PASSWORD"])


@pytest.fixture()
def sql_engine_public_vis() -> sql.engine.Engine:
    """Returns the engine that connects with the postgres admin user"""

    return get_user_engine(os.environ["POSTGRES_DATA_PUB_VIS_USER"], os.environ["POSTGRES_DATA_PUB_VIS_PASSWORD"])


def get_user_engine(username, password) -> sql.Engine:
    """Creates the DB connection from the given credentials and tests it."""

    engine_url = get_sql_url(username, password)
    engine = sql.create_engine(engine_url, pool_size=2, max_overflow=2, pool_timeout=2)
    engine.connect()
    return engine


def get_sql_url(username: str, password: str):
    """Resolves the engine url based on the postgres environment variables and the externally supplied credentials"""

    host = os.environ["RDP_POSTGRES_HOST"]
    port = os.environ.get("RDP_POSTGRES_PORT", 5432)
    db = os.environ["POSTGRES_DB"]

    username = urllib.parse.quote(username)
    password = urllib.parse.quote(password)

    return f"postgresql://{username}:{password}@{host}:{port}/{db}"


@pytest.fixture()
def clean_db():
    """Provides a clean DB environment by resetting and building the entire DB via alembic"""

    alembic_args = [
        '--raiseerr',
        'downgrade', 'base',
    ]
    alembic.config.main(argv=alembic_args)

    alembic_args = [
        '--raiseerr',
        'upgrade', 'head',
    ]
    alembic.config.main(argv=alembic_args)

    return ""

