import logging
import os
import re

from logging.config import fileConfig

from alembic import context

import dotenv
import sqlalchemy
import tenacity

import rdp_db.utils.db_version as db_version

# Populate the local environment variables
dotenv.load_dotenv(dotenv_path=".env")

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
fileConfig(config.config_file_name)

# add your model's MetaData object here
# for 'autogenerate' support
# from myapp import mymodel
# target_metadata = mymodel.Base.metadata
target_metadata = None

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.

logger = logging.getLogger(__name__)


def run_migrations_offline():
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    if "RDP_POSTGRES_URL" not in os.environ:
        raise KeyError("Expect the RDP_POSTGRES_URL environment variable to be available")

    url = config.get_main_option(os.environ["RDP_POSTGRES_URL"])
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        transaction_per_migration=True  # Avoid deadlocks, by the dedicated compression policy process
    )

    with context.begin_transaction():
        context.run_migrations()


def auto_create_db():
    """Uses the environment variables to automatically create the database"""

    if "RDP_POSTGRES_URL_INIT" not in os.environ:
        raise KeyError("Expect the RDP_POSTGRES_URL_INIT environment variable to be available")

    db_name = os.environ.get('POSTGRES_DB', 'rdp_db')
    if not re.match(r"^[a-zA-Z0-9_]+$", db_name):
        raise ValueError(f"The database name '{db_name}' is invalid. Only [a-zA-Z0-9_]+ is allowed")

    engine = _connect_to_db(os.environ["RDP_POSTGRES_URL_INIT"])
    with engine.connect() as conn:
        logger.info(f"Ensure that the database '{db_name}' exists")
        # See: https://stackoverflow.com/questions/18389124/simulate-create-database-if-not-exists-for-postgresql
        conn.execute(sqlalchemy.text(f"""
            DO
            $do$
            BEGIN
                IF EXISTS (SELECT FROM pg_database WHERE datname = '{db_name}') THEN
                    RAISE NOTICE 'Database already exists';
                ELSE
                    CREATE EXTENSION IF NOT EXISTS dblink;
                    PERFORM dblink_exec('dbname=' || current_database(), 'CREATE DATABASE {db_name}');
                END IF;
            END
            $do$;
        """))


@tenacity.retry(wait=tenacity.wait_exponential(multiplier=1.2, min=1, max=10),
                stop=tenacity.stop_after_delay(90),
                after=tenacity.after_log(logger, logging.WARNING))
def _connect_to_db(db_url: str) -> sqlalchemy.engine.Engine:
    """Creates the DB engine and tests it."""
    engine = sqlalchemy.create_engine(db_url)

    logger.debug(f"Test the DB URL {db_url}")
    with engine.connect() as conn:
        conn.execute(sqlalchemy.text("SELECT FROM pg_database;"))
    logger.debug(f"Successfully connected to {db_url}")

    return engine


def run_migrations_online():
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    if "RDP_POSTGRES_URL" not in os.environ:
        raise KeyError("Expect the RDP_POSTGRES_URL environment variable to be available")

    auto_create_db()
    connectable = _connect_to_db(os.environ["RDP_POSTGRES_URL"])

    with connectable.connect() as connection:
        context.configure(
            connection=connection, target_metadata=target_metadata,
            transaction_per_migration=True  # Avoid deadlocks, by the dedicated compression policy process
        )
        db_version.init_version(connection)

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
