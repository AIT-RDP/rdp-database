"""
Provides the common SQL test fixtures to specifically assess the database layout

The logic was originally developed in the Edge4Energy activities. Make sure that the .env file is not populated by the
test runner itself, as the runner implementation does no variables replacement and the runner environment would take
precedence.
"""
import json
import os
import urllib.parse
from typing import Dict, Iterable, Union, Any

import alembic.config
import pandas as pd
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


@pytest.fixture()
def sql_meta(sql_engine_postgres: sql.engine.Engine) -> sql.MetaData:
    """Returns the meta-data object representing the DB scheme"""
    meta = sql.MetaData()
    meta.reflect(bind=sql_engine_postgres)
    return meta


@pytest.fixture()
def sql_test_set(sql_engine_postgres: sql.engine.Engine, sql_meta: sql.MetaData) -> Dict[str, pd.DataFrame]:
    """
    Populates the tables with a temporary set of test data points.

    :return: A dictionary with the reference data for testing purpose.
    """
    with sql_engine_postgres.begin() as con:
        device_type_rows = _insert_table_data(con, sql_meta, "device_type", data=[
            dict(display_name="Atari 800XL", icon="atari_1"),
            dict(display_name="Atari 1200XL", icon="atari_2"),
        ])
        device_rows = _insert_table_data(con, sql_meta, "device", data=[
            dict(device_id='3679bd0d-235f-4dfb-9100-f011d246be39', config_ref=1, type=int(device_type_rows.index[0]),
                 name="Gateway 1", owner="03751ddd-440f-440a-86a4-ce0dda3c32cf", management_ip="192.168.57.1"),
            dict(device_id='42c1b502-53bf-4bd2-ac18-512ca5138f93', config_ref=1, type=int(device_type_rows.index[1]),
                 name="Gateway 2", owner=None, management_ip="192.168.57.2"),
        ])

        asset_type_rows = _insert_table_data(con, sql_meta, "asset_type", data=[
            dict(display_name="Millenium Falcon", icon="star_wars_1"),
            dict(display_name="Voyager", icon="nasa_1")
        ])

        asset_rows = _insert_table_data(con, sql_meta, "asset", data=[
            dict(asset_id="190b09e2-11d8-4846-82be-19a30fd38b9f", type=int(asset_type_rows.index[0]),
                 name="Twelve parsecs!", gateway="42c1b502-53bf-4bd2-ac18-512ca5138f93"),
            dict(asset_id="fa8874c8-0e2f-4b15-ac8e-bfebd338046b", type=int(asset_type_rows.index[1]), name="Voyager I",
                 gateway="3679bd0d-235f-4dfb-9100-f011d246be39"),
            dict(asset_id="082efa33-0cb2-40a8-b54c-78bb050f2bbe", type=int(asset_type_rows.index[1]), name="Voyager II",
                 gateway="3679bd0d-235f-4dfb-9100-f011d246be39"),
        ])

    yield dict(
        device_type=device_type_rows,
        device=device_rows,
        asset_type=asset_type_rows,
        asset=asset_rows
    )

    with sql_engine_postgres.begin() as con:
        _delete_from_table(con, "asset", asset_rows.index)
        _delete_from_table(con, "asset_type", asset_type_rows.index)
        _delete_from_table(con, "device", device_rows.index)
        _delete_from_table(con, "device_type", device_type_rows.index)


def _insert_table_data(con: sql.Connection, sql_meta: sql.MetaData, table_name: str,
                       data: Iterable[Dict[str, Any]]) -> pd.DataFrame:
    """
    Inserts the test data into the table

    :param con: The SQL connection
    :param sql_meta: The meta-data object
    :param table_name: The name of the SQL table within the MetaData Structure
    :param data: The tata to insert in a record format
    :return: The data as formatted by a data frame
    """

    # Note that directly writing the text statement with INSERT ... RETURNING does not yield a curser connected to
    # results. Hence, the clean way of using the meta-data object had to be chosen.
    dst_table = sql_meta.tables[table_name]
    dst_columns = dict(**dst_table.columns)
    res = con.execute(sql.insert(dst_table).returning(*list(dst_columns.values())), parameters=data)

    data_table = pd.DataFrame.from_records([row._asdict() for row in res.fetchall()],
                                           index=[col.name for col in dst_table.primary_key.columns])
    return data_table


def _delete_from_table(con: sql.Connection, table_name: str, elements: pd.Index):
    """
    Deletes the specific elements from the given table

    This function is only intended for testing purpose. Don't use it in production. It's neither efficient not safe!

    :param con: The current connection
    :param table_name: The name of the table that holds the elements
    :param elements: A pandas index or multi index that holds the elements. Make sure to set the level names according
        to the primary key columns.
    """

    filter_expr = " AND ".join(f"{name} = :{name}" for name in elements.names)
    rows = [
        {name: val for name, val in zip(elements.names, vals if isinstance(vals, tuple) else (vals,))}
        for vals in elements
    ]
    con.execute(sql.text(f"""DELETE FROM {table_name} WHERE {filter_expr}"""), parameters=rows)
