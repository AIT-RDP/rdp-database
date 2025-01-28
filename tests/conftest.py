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
import pytest
import sqlalchemy as sql
import dotenv

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
    with engine.connect():
        pass  # Just test the connection

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


@pytest.fixture()
def basic_dp_test_set(clean_db, sql_engine_data_source) -> dict[str, int]:
    """Defines a basic set of datapoints and returns their IDs."""
    return {
        "loc0-dev0-pub-0": _create_dp(
            sql_engine_data_source, "name_0", "device_0", "location_0", "provider_0",
            view_role="view_public"
        ),
        "loc0-dev0-pub-1": _create_dp(
            sql_engine_data_source, "name_1", "device_0", "location_0", "provider_0",
            view_role="view_public"
        ),
        "loc1-dev1-pub-0": _create_dp(
            sql_engine_data_source, "name_0", "device_1", "location_1", "provider_0",
            view_role="view_public"
        ),
        "loc0-dev0-pr-2": _create_dp(
            sql_engine_data_source, "name_2", "device_0", "location_0", "provider_0",
            view_role="view_private"
        ),
    }


def _create_dp(eng: sql.Engine, name, device_id, location_code, data_provider, unit=None, metadata=None,
               view_role=None):
    """Creates (or returns) the datapoint and returns its number"""

    if metadata is None:
        metadata = {}

    with eng.connect() as con:
        params = [  # metadata needs special treatment. Hence, the explicit construction
            sql.bindparam("metadata", metadata, type_=sql.dialects.postgresql.JSONB),
            sql.bindparam("name", name), sql.bindparam("device_id", device_id),
            sql.bindparam("location_code", location_code), sql.bindparam("data_provider", data_provider),
            sql.bindparam("unit", unit)
        ]

        res = con.execute(sql.text("""
            SELECT get_or_create_data_point_id(
                    :name, :device_id, :location_code, :data_provider, :unit, :metadata
                ) AS dp_id;
        """).bindparams(*params))

        res = res.mappings().fetchall()
        assert len(res) == 1, "No results returned by the query"
        assert "dp_id" in res[0], "No pd_id returned by query"

        dp_id = res[0]["dp_id"]

        # Update the view role, if needed
        if view_role is not None:
            con.execute(sql.text("""
                UPDATE data_points SET view_role=:view_role WHERE id=:dp_id;
            """), parameters=dict(view_role=view_role, dp_id=dp_id))

        con.commit()
    return dp_id


@pytest.fixture()
def mixed_dataset(basic_dp_test_set, sql_engine_data_source):
    """Defines a basic mixed dataset"""

    dp_mea_a = basic_dp_test_set["loc0-dev0-pub-0"]
    dp_mea_b = basic_dp_test_set["loc0-dev0-pr-2"]

    dp_fc_a = basic_dp_test_set["loc0-dev0-pub-1"]
    dp_fc_b = basic_dp_test_set["loc0-dev0-pr-2"]  # Intentionally the same datapoint for measurements and forecasts

    with sql_engine_data_source.connect() as con:
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
        con.commit()

    return {
        **basic_dp_test_set,
        "dp_mea_a": dp_mea_a, "dp_mea_b": dp_mea_b, "dp_fc_a": dp_fc_a, "dp_fc_b": dp_fc_b
    }
