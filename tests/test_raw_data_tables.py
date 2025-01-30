"""
Runs the tests on the raw data table schemes
"""

import pandas as pd
import pytest
import sqlalchemy.engine
import sqlalchemy.exc
import sqlalchemy.sql as sql


def test_raw_unitemporal_double_access(basic_dp_test_set, sql_engine_data_source: sqlalchemy.engine.Engine):
    """Tests whether direct inserts into the raw double table are feasible"""
    dp_id_a = basic_dp_test_set["loc0-dev0-pub-0"]
    dp_id_b = basic_dp_test_set["loc2-dev0-pub-0-uni-dbl-1"]

    with sql_engine_data_source.begin() as con:
        con.execute(sql.text(f"""
            INSERT INTO raw_unitemporal_double(dp_id, obs_time, value) VALUES
                (:dp_id_a, '2025-01-01T00:00:00Z', 123.4),
                (:dp_id_b, '2025-01-01T12:00:00Z', 567.8)
        """), parameters=dict(dp_id_a=dp_id_a, dp_id_b=dp_id_b))

        data = pd.read_sql("""
            SELECT dp_id, obs_time, value FROM raw_unitemporal_double ORDER BY dp_id, obs_time;
        """, con)

        pd.testing.assert_frame_equal(data, pd.DataFrame({
            "dp_id": [dp_id_a, dp_id_b],
            "obs_time": pd.to_datetime(["2025-01-01T00:00:00Z", "2025-01-01T12:00:00Z"]),
            "value": [123.4, 567.8]
        }), check_names=False)


def test_raw_bitemporal_double_access(basic_dp_test_set, sql_engine_data_source: sqlalchemy.engine.Engine):
    """Tests whether direct inserts into the raw double table are feasible"""
    dp_id_a = basic_dp_test_set["loc0-dev0-pub-0"]
    dp_id_b = basic_dp_test_set["loc2-dev0-pub-0-bi-dbl-1"]

    with sql_engine_data_source.begin() as con:
        con.execute(sql.text(f"""
            INSERT INTO raw_bitemporal_double(dp_id, fc_time, obs_time, value) VALUES
                (:dp_id_a, '2025-01-01T00:00:00Z', '2025-01-01T12:00:00Z', 123.4),
                (:dp_id_b, '2025-01-01T01:00:00Z', '2025-01-01T13:00:00Z', 567.8)
        """), parameters=dict(dp_id_a=dp_id_a, dp_id_b=dp_id_b))

        data = pd.read_sql("""
            SELECT dp_id, fc_time, obs_time, value FROM raw_bitemporal_double ORDER BY dp_id, fc_time, obs_time;
        """, con)

        pd.testing.assert_frame_equal(data, pd.DataFrame({
            "dp_id": [dp_id_a, dp_id_b],
            "fc_time": pd.to_datetime(["2025-01-01T00:00:00Z", "2025-01-01T01:00:00Z"]),
            "obs_time": pd.to_datetime(["2025-01-01T12:00:00Z", "2025-01-01T13:00:00Z"]),
            "value": [123.4, 567.8]
        }), check_names=False)


def test_raw_unitemporal_double_invalid_inserts(basic_dp_test_set, sql_engine_data_source: sqlalchemy.engine.Engine):
    """Tests some invalid inserts for the unitemporal double series"""

    # Wrong temporality
    dp_id = basic_dp_test_set["loc2-dev0-pub-0-bi-dbl-1"]
    with pytest.raises(sqlalchemy.exc.InternalError, match=".*Invalid temporality.*"):
        with sql_engine_data_source.begin() as con:
            con.execute(sql.text(f"""
                        INSERT INTO raw_unitemporal_double(dp_id, obs_time, value) VALUES
                            (:dp_id, '2025-01-01T01:00:00Z', 567.8);
                    """), parameters=dict(dp_id=dp_id))

    # Wrong data type
    dp_id = basic_dp_test_set["loc2-dev0-pub-0-uni-int-1"]
    with pytest.raises(sqlalchemy.exc.InternalError, match=".*Invalid data type.*"):
        with sql_engine_data_source.begin() as con:
            con.execute(sql.text(f"""
                        INSERT INTO raw_unitemporal_double(dp_id, obs_time, value) VALUES
                            (:dp_id, '2025-01-01T01:00:00Z', 567.8);
                    """), parameters=dict(dp_id=dp_id))


def test_raw_bitemporal_double_invalid_inserts(basic_dp_test_set, sql_engine_data_source: sqlalchemy.engine.Engine):
    """Tests some invalid inserts for the unitemporal double series"""

    # Wrong temporality
    dp_id = basic_dp_test_set["loc2-dev0-pub-0-uni-dbl-1"]
    with pytest.raises(sqlalchemy.exc.InternalError, match=".*Invalid temporality.*"):
        with sql_engine_data_source.begin() as con:
            con.execute(sql.text(f"""
                        INSERT INTO raw_bitemporal_double(dp_id, fc_time, obs_time, value) VALUES
                            (:dp_id, '2025-01-01T00:00:00Z', '2025-01-01T01:00:00Z', 567.8);
                    """), parameters=dict(dp_id=dp_id))

    # Wrong data type
    dp_id = basic_dp_test_set["loc2-dev0-pub-0-bi-int-1"]
    with pytest.raises(sqlalchemy.exc.InternalError, match=".*Invalid data type.*"):
        with sql_engine_data_source.begin() as con:
            con.execute(sql.text(f"""
                        INSERT INTO raw_bitemporal_double(dp_id, fc_time, obs_time, value) VALUES
                            (:dp_id, '2025-01-01T00:00:00Z', '2025-01-01T01:00:00Z', 567.8);
                    """), parameters=dict(dp_id=dp_id))
