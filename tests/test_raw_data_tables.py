"""
Runs the tests on the raw data table schemes
"""

import pandas as pd
import pytest
import sqlalchemy.engine
import sqlalchemy.exc
import sqlalchemy.sql as sql
import sqlalchemy.dialects


@pytest.mark.parametrize("table_name,value_a,value_b,dp_a,dp_b,param_type", [
    ("raw_unitemporal_double", 123.4, 567.8, "loc0-dev0-pub-0", "loc2-dev0-pub-0-uni-dbl-1", None),
    ("raw_unitemporal_double", -123.4, -567.8, "loc2-dev0-pr-0-uni-dbl-0", "loc2-dev0-pub-0-uni-dbl-1", None),
    ("raw_unitemporal_bigint", -1000, 2200, "loc2-dev0-pr-0-uni-int-0", "loc2-dev0-pub-0-uni-int-1", None),
    ("raw_unitemporal_boolean", True, False, "loc2-dev0-pr-0-uni-bool-0", "loc2-dev0-pub-0-uni-bool-1", None),
    (
            "raw_unitemporal_jsonb", {"yippee": "objects"}, "Nö möre umlautß!", "loc2-dev0-pr-0-uni-json-0",
            "loc2-dev0-pub-0-uni-json-1", sqlalchemy.dialects.postgresql.JSONB
    ),
])
def test_raw_unitemporal_access(
        basic_dp_test_set, sql_engine_data_source: sqlalchemy.engine.Engine,
        table_name, value_a, value_b, dp_a, dp_b, param_type
):
    """Tests whether direct inserts into the raw double table are feasible"""
    dp_id_a = basic_dp_test_set[dp_a]
    dp_id_b = basic_dp_test_set[dp_b]

    with sql_engine_data_source.begin() as con:
        con.execute(sql.text(f"""
            INSERT INTO {table_name}(dp_id, obs_time, value) VALUES
                (:dp_id_a, '2025-01-01T00:00:00Z', :value_a),
                (:dp_id_b, '2025-01-01T12:00:00Z', :value_b)
        """).bindparams(
            sql.bindparam("value_a", value_a, type_=param_type),
            sql.bindparam("value_b", value_b, type_=param_type),
            sql.bindparam("dp_id_a", dp_id_a), sql.bindparam("dp_id_b", dp_id_b),
        ))

        data = pd.read_sql(f"""
            SELECT dp_id, obs_time, value FROM {table_name} ORDER BY dp_id, obs_time;
        """, con)

        pd.testing.assert_frame_equal(data, pd.DataFrame({
            "dp_id": [dp_id_a, dp_id_b],
            "obs_time": pd.to_datetime(["2025-01-01T00:00:00Z", "2025-01-01T12:00:00Z"]),
            "value": [value_a, value_b]
        }), check_names=False)


@pytest.mark.parametrize("table_name,value_a,value_b,dp_a,dp_b,param_type", [
    ("raw_bitemporal_double", 123.4, 567.8, "loc0-dev0-pub-0", "loc2-dev0-pub-0-bi-dbl-1", None),
    ("raw_bitemporal_double", -123.4, -567.8, "loc2-dev0-pr-0-bi-dbl-0", "loc2-dev0-pub-0-bi-dbl-1", None),
    ("raw_bitemporal_bigint", -1000, 2200, "loc2-dev0-pr-0-bi-int-0", "loc2-dev0-pub-0-bi-int-1", None),
    ("raw_bitemporal_boolean", True, False, "loc2-dev0-pr-0-bi-bool-0", "loc2-dev0-pub-0-bi-bool-1", None),
    (
            "raw_bitemporal_jsonb", {"yippee": "objects"}, "Nö möre umlautß!", "loc2-dev0-pr-0-bi-json-0",
            "loc2-dev0-pub-0-bi-json-1", sqlalchemy.dialects.postgresql.JSONB
    ),
])
def test_raw_bitemporal_access(
        basic_dp_test_set, sql_engine_data_source: sqlalchemy.engine.Engine,
        table_name, value_a, value_b, dp_a, dp_b, param_type
):
    """Tests whether direct inserts into the raw double table are feasible"""
    dp_id_a = basic_dp_test_set[dp_a]
    dp_id_b = basic_dp_test_set[dp_b]

    with sql_engine_data_source.begin() as con:
        con.execute(sql.text(f"""
            INSERT INTO {table_name}(dp_id, fc_time, obs_time, value) VALUES
                (:dp_id_a, '2024-01-01T00:00:00Z', '2025-01-01T00:00:00Z', :value_a),
                (:dp_id_b, '2024-01-01T00:00:00Z', '2025-01-01T12:00:00Z', :value_b)
        """).bindparams(
            sql.bindparam("value_a", value_a, type_=param_type),
            sql.bindparam("value_b", value_b, type_=param_type),
            sql.bindparam("dp_id_a", dp_id_a), sql.bindparam("dp_id_b", dp_id_b),
        ))

        data = pd.read_sql(f"""
            SELECT dp_id, fc_time, obs_time, value FROM {table_name} ORDER BY dp_id, fc_time, obs_time;
        """, con)

        pd.testing.assert_frame_equal(data, pd.DataFrame({
            "dp_id": [dp_id_a, dp_id_b],
            "fc_time": pd.to_datetime(["2024-01-01T00:00:00Z", "2024-01-01T00:00:00Z"]),
            "obs_time": pd.to_datetime(["2025-01-01T00:00:00Z", "2025-01-01T12:00:00Z"]),
            "value": [value_a, value_b]
        }), check_names=False)


@pytest.mark.parametrize("table_name, invalid_temporality, invalid_type", [
    ("raw_unitemporal_double", "loc2-dev0-pub-0-bi-dbl-1", "loc2-dev0-pub-0-uni-int-1"),
    ("raw_unitemporal_bigint", "loc2-dev0-pub-0-bi-int-1", "loc2-dev0-pub-0-uni-dbl-1"),
    ("raw_unitemporal_boolean", "loc2-dev0-pub-0-bi-bool-1", "loc2-dev0-pub-0-uni-int-1"),
    ("raw_unitemporal_jsonb", "loc2-dev0-pub-0-bi-json-1", "loc2-dev0-pub-0-uni-bool-1"),
])
def test_raw_unitemporal_invalid_inserts(
        basic_dp_test_set, sql_engine_data_source: sqlalchemy.engine.Engine,
        table_name, invalid_temporality, invalid_type
):
    """Tests some invalid inserts for the unitemporal series"""

    # Wrong temporality
    dp_id = basic_dp_test_set[invalid_temporality]
    with pytest.raises(sqlalchemy.exc.InternalError, match=".*Invalid temporality.*"):
        with sql_engine_data_source.begin() as con:
            con.execute(sql.text(f"""
                        INSERT INTO {table_name}(dp_id, obs_time, value) VALUES
                            (:dp_id, '2025-01-01T01:00:00Z', NULL);
                    """), parameters=dict(dp_id=dp_id))

    # Wrong data type
    dp_id = basic_dp_test_set[invalid_type]
    with pytest.raises(sqlalchemy.exc.InternalError, match=".*Invalid data type.*"):
        with sql_engine_data_source.begin() as con:
            con.execute(sql.text(f"""
                        INSERT INTO {table_name}(dp_id, obs_time, value) VALUES
                            (:dp_id, '2025-01-01T01:00:00Z', NULL);
                    """), parameters=dict(dp_id=dp_id))


@pytest.mark.parametrize("table_name, invalid_temporality, invalid_type", [
    ("raw_bitemporal_double", "loc2-dev0-pub-0-uni-dbl-1", "loc2-dev0-pub-0-bi-int-1"),
    ("raw_bitemporal_bigint", "loc2-dev0-pub-0-uni-int-1", "loc2-dev0-pub-0-bi-dbl-1"),
    ("raw_bitemporal_boolean", "loc2-dev0-pub-0-uni-bool-1", "loc2-dev0-pub-0-bi-int-1"),
    ("raw_bitemporal_jsonb", "loc2-dev0-pub-0-uni-json-1", "loc2-dev0-pub-0-bi-bool-1"),
])
def test_raw_bitemporal_invalid_inserts(
        basic_dp_test_set, sql_engine_data_source: sqlalchemy.engine.Engine,
        table_name, invalid_temporality, invalid_type
):
    """Tests some invalid inserts for the unitemporal series"""

    # Wrong temporality
    dp_id = basic_dp_test_set[invalid_temporality]
    with pytest.raises(sqlalchemy.exc.InternalError, match=".*Invalid temporality.*"):
        with sql_engine_data_source.begin() as con:
            con.execute(sql.text(f"""
                        INSERT INTO {table_name}(dp_id, fc_time, obs_time, value) VALUES
                            (:dp_id, '2025-01-01T00:00:00Z', '2025-01-01T01:00:00Z', NULL);
                    """), parameters=dict(dp_id=dp_id))

    # Wrong data type
    dp_id = basic_dp_test_set[invalid_type]
    with pytest.raises(sqlalchemy.exc.InternalError, match=".*Invalid data type.*"):
        with sql_engine_data_source.begin() as con:
            con.execute(sql.text(f"""
                        INSERT INTO {table_name}(dp_id, fc_time, obs_time, value) VALUES
                            (:dp_id, '2025-01-01T00:00:00Z', '2025-01-01T01:00:00Z', NULL);
                    """), parameters=dict(dp_id=dp_id))
