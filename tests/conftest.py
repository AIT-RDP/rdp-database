"""
Provides the common SQL test fixtures to specifically assess the database layout

The logic was originally developed in the Edge4Energy activities. Make sure that the .env file is not populated by the
test runner itself, as the runner implementation does no variables replacement and the runner environment would take
precedence.
"""
import contextlib
import os
import urllib.parse

import alembic.config
import dotenv
import pytest
import sqlalchemy as sql
import sqlalchemy.exc
import tenacity

import tests.db_helpers as hlp

dotenv.load_dotenv(dotenv_path=".env")


@pytest.fixture()
def sql_engine_postgres() -> sql.engine.Engine:
    """Returns the engine that connects with the postgres admin user"""

    with get_user_engine(os.environ["POSTGRES_USER"], os.environ["POSTGRES_PASSWORD"]) as e:
        yield e


@pytest.fixture()
def sql_engine_data_source() -> sql.engine.Engine:
    """Returns the engine that connects with the postgres admin user"""

    with get_user_engine(os.environ["POSTGRES_DATA_SOURCE_USER"], os.environ["POSTGRES_DATA_SOURCE_PASSWORD"]) as e:
        yield e


@pytest.fixture()
def sql_engine_private_vis() -> sql.engine.Engine:
    """Returns the engine that connects with the postgres admin user"""

    with get_user_engine(os.environ["POSTGRES_DATA_VIS_USER"], os.environ["POSTGRES_DATA_VIS_PASSWORD"]) as e:
        yield e


@pytest.fixture()
def sql_engine_public_vis() -> sql.engine.Engine:
    """Returns the engine that connects with the postgres admin user"""

    with get_user_engine(os.environ["POSTGRES_DATA_PUB_VIS_USER"], os.environ["POSTGRES_DATA_PUB_VIS_PASSWORD"]) as e:
        yield e


@contextlib.contextmanager
def get_user_engine(username, password) -> sql.Engine:
    """Creates the DB connection from the given credentials and tests it."""

    engine_url = get_sql_url(username, password)
    engine = sql.create_engine(engine_url, pool_size=2, max_overflow=2, pool_timeout=2)

    yield engine
    engine.dispose(close=True)


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

    do_redeployment_cycle()
    return ""


@tenacity.retry(
    wait=tenacity.wait_exponential(multiplier=1.2, min=1, max=10),
    stop=tenacity.stop_after_delay(30),
    retry=tenacity.retry_if_exception_type(sqlalchemy.exc.OperationalError)
)
def do_redeployment_cycle():
    """
    Performs a full redeployment cycle erasing all data

    Since downgrading the timescale tables often runs into a deadlock, the operation is retried, if such a deadlock
    is detected.
    """

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


@pytest.fixture()
def basic_dp_test_set(clean_db, sql_engine_data_source) -> dict[str, int]:
    """Defines a basic set of datapoints and returns their IDs."""
    return {
        "loc0-dev0-pub-0": hlp.create_dp(
            sql_engine_data_source, "name_0", "device_0", "location_0", "provider_0",
            view_role="view_public", unit="ISO football fields", metadata={"note": "with 10mm grass only"}
        ),
        "loc0-dev0-pub-1": hlp.create_dp(
            sql_engine_data_source, "name_1", "device_0", "location_0", "provider_0",
            view_role="view_public", unit="DIN bathtubs", metadata={"note": "don't trust the units"}
        ),
        "loc1-dev1-pub-0": hlp.create_dp(
            sql_engine_data_source, "name_0", "device_1", "location_1", "provider_0",
            view_role="view_public"
        ),
        "loc0-dev0-pr-2": hlp.create_dp(
            sql_engine_data_source, "name_2", "device_0", "location_0", "provider_0",
            view_role="view_internal"
        ),

        # Data points with a defined datatype and temporality
        "loc2-dev0-pr-0-uni-dbl-0": hlp.create_dp(
            sql_engine_data_source, "name_0", "device_0", "location_2", "provider_1",
            view_role="view_internal", data_type='double', temporality='unitemporal'
        ),
        "loc2-dev0-pub-0-uni-dbl-1": hlp.create_dp(
            sql_engine_data_source, "name_1", "device_0", "location_2", "provider_1",
            view_role="view_public", data_type='double', temporality='unitemporal'
        ),
        "loc2-dev0-pr-0-bi-dbl-0": hlp.create_dp(
            sql_engine_data_source, "name_0", "device_0", "location_2", "provider_2",
            view_role="view_internal", data_type='double', temporality='bitemporal'
        ),
        "loc2-dev0-pub-0-bi-dbl-1": hlp.create_dp(
            sql_engine_data_source, "name_1", "device_0", "location_2", "provider_2",
            view_role="view_public", data_type='double', temporality='bitemporal'
        ),

        "loc2-dev0-pr-0-uni-int-0": hlp.create_dp(
            sql_engine_data_source, "name_0", "device_0", "location_3", "provider_1",
            view_role="view_internal", data_type='bigint', temporality='unitemporal'
        ),
        "loc2-dev0-pub-0-uni-int-1": hlp.create_dp(
            sql_engine_data_source, "name_1", "device_0", "location_3", "provider_1",
            view_role="view_public", data_type='bigint', temporality='unitemporal'
        ),
        "loc2-dev0-pr-0-bi-int-0": hlp.create_dp(
            sql_engine_data_source, "name_0", "device_0", "location_3", "provider_2",
            view_role="view_internal", data_type='bigint', temporality='bitemporal'
        ),
        "loc2-dev0-pub-0-bi-int-1": hlp.create_dp(
            sql_engine_data_source, "name_1", "device_0", "location_3", "provider_2",
            view_role="view_public", data_type='bigint', temporality='bitemporal'
        ),

        "loc2-dev0-pr-0-uni-bool-0": hlp.create_dp(
            sql_engine_data_source, "name_0", "device_0", "location_4", "provider_1",
            view_role="view_internal", data_type='boolean', temporality='unitemporal'
        ),
        "loc2-dev0-pub-0-uni-bool-1": hlp.create_dp(
            sql_engine_data_source, "name_1", "device_0", "location_4", "provider_1",
            view_role="view_public", data_type='boolean', temporality='unitemporal'
        ),
        "loc2-dev0-pr-0-bi-bool-0": hlp.create_dp(
            sql_engine_data_source, "name_0", "device_0", "location_4", "provider_2",
            view_role="view_internal", data_type='boolean', temporality='bitemporal'
        ),
        "loc2-dev0-pub-0-bi-bool-1": hlp.create_dp(
            sql_engine_data_source, "name_1", "device_0", "location_4", "provider_2",
            view_role="view_public", data_type='boolean', temporality='bitemporal'
        ),

        "loc2-dev0-pr-0-uni-json-0": hlp.create_dp(
            sql_engine_data_source, "name_0", "device_0", "location_5", "provider_1",
            view_role="view_internal", data_type='jsonb', temporality='unitemporal'
        ),
        "loc2-dev0-pub-0-uni-json-1": hlp.create_dp(
            sql_engine_data_source, "name_1", "device_0", "location_5", "provider_1",
            view_role="view_public", data_type='jsonb', temporality='unitemporal'
        ),
        "loc2-dev0-pr-0-bi-json-0": hlp.create_dp(
            sql_engine_data_source, "name_0", "device_0", "location_5", "provider_2",
            view_role="view_internal", data_type='jsonb', temporality='bitemporal'
        ),
        "loc2-dev0-pub-0-bi-json-1": hlp.create_dp(
            sql_engine_data_source, "name_1", "device_0", "location_5", "provider_2",
            view_role="view_public", data_type='jsonb', temporality='bitemporal'
        ),

    }


@pytest.fixture()
def mixed_dataset(basic_dp_test_set, sql_engine_data_source):
    """Defines a basic mixed dataset"""

    dp_mea_a = basic_dp_test_set["loc0-dev0-pub-0"]
    dp_mea_b = basic_dp_test_set["loc0-dev0-pr-2"]

    dp_fc_a = basic_dp_test_set["loc0-dev0-pub-1"]
    dp_fc_b = basic_dp_test_set["loc0-dev0-pr-2"]  # Intentionally the same datapoint for measurements and forecasts

    with sql_engine_data_source.begin() as con:
        con.execute(sql.text("""
            INSERT INTO measurements(dp_id, obs_time, value) VALUES
                (:dp_mea_a, '2024-12-24T00:00:00Z', 1.0),
                (:dp_mea_a, '2024-12-24T06:00:00Z', 2.0),
                (:dp_mea_a, '2024-12-24T12:00:00Z', 3.0),
                (:dp_mea_a, '2024-12-24T18:00:00Z', 4.0),

                (:dp_mea_b, '2024-12-24T00:00:00Z', 5.0),
                (:dp_mea_b, '2024-12-24T06:00:00Z', 6.0),
                (:dp_mea_b, '2024-12-24T12:00:00Z', 7.0),
                (:dp_mea_b, '2024-12-24T18:00:00Z', 8.0);
        """), parameters=dict(dp_mea_a=dp_mea_a, dp_mea_b=dp_mea_b))
        con.execute(sql.text("""
            INSERT INTO forecasts(dp_id, fc_time, obs_time, value) VALUES
                (:dp_fc_a, '2024-12-23T00:00:00Z', '2024-12-24T00:00:00Z', 1.0),
                (:dp_fc_a, '2024-12-23T00:00:00Z', '2024-12-24T06:00:00Z', 2.0),
                (:dp_fc_a, '2024-12-23T00:00:00Z', '2024-12-24T12:00:00Z', 3.0),
                (:dp_fc_a, '2024-12-23T00:00:00Z', '2024-12-24T18:00:00Z', 4.0),

                (:dp_fc_a, '2024-12-23T12:00:00Z', '2024-12-24T00:00:00Z', 1.5),
                (:dp_fc_a, '2024-12-23T12:00:00Z', '2024-12-24T06:00:00Z', 2.5),
                (:dp_fc_a, '2024-12-23T12:00:00Z', '2024-12-24T12:00:00Z', 3.5),
                (:dp_fc_a, '2024-12-23T12:00:00Z', '2024-12-24T18:00:00Z', 4.5),

                (:dp_fc_b, '2024-12-23T00:00:00Z', '2024-12-24T00:00:00Z', 5.0),
                (:dp_fc_b, '2024-12-23T00:00:00Z', '2024-12-24T06:00:00Z', 6.0),
                (:dp_fc_b, '2024-12-23T00:00:00Z', '2024-12-24T12:00:00Z', 7.0),
                (:dp_fc_b, '2024-12-23T00:00:00Z', '2024-12-24T18:00:00Z', 8.0);
        """), parameters=dict(dp_fc_a=dp_fc_a, dp_fc_b=dp_fc_b))

    return {
        **basic_dp_test_set,
        "dp_mea_a": dp_mea_a, "dp_mea_b": dp_mea_b, "dp_fc_a": dp_fc_a, "dp_fc_b": dp_fc_b
    }
