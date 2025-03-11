"""
Tests the basic raw series-related functionality (measurements, forecasts) of the database scheme

The basic api is superseded by the raw API
"""

import pandas as pd
import pytest
import sqlalchemy.sql as sql
import sqlalchemy.exc


def test_ts_basic_insert_and_read_measurements(mixed_dataset, sql_engine_private_vis):
    """tests the basic insert and read mechanism"""

    dp_mea_a = mixed_dataset["dp_mea_a"]
    dp_mea_b = mixed_dataset["dp_mea_b"]

    with sql_engine_private_vis.begin() as con:
        data = pd.read_sql("""
            SELECT dp_id, obs_time, value FROM measurements ORDER BY dp_id, obs_time, value;
        """, con)

    pd.testing.assert_frame_equal(data, pd.DataFrame({
        "dp_id": [dp_mea_a, dp_mea_a, dp_mea_a, dp_mea_a, dp_mea_b, dp_mea_b, dp_mea_b, dp_mea_b],
        "obs_time": pd.to_datetime([
            "2024-12-24T00:00:00Z", "2024-12-24T06:00:00Z", "2024-12-24T12:00:00Z", "2024-12-24T18:00:00Z",
            "2024-12-24T00:00:00Z", "2024-12-24T06:00:00Z", "2024-12-24T12:00:00Z", "2024-12-24T18:00:00Z"
        ]),
        "value": [1., 2., 3., 4., 5., 6., 7., 8.]
    }))


def test_ts_basic_insert_and_read_forecasts(mixed_dataset, sql_engine_private_vis):
    """tests the basic insert and read mechanism"""

    dp_fc_a = mixed_dataset["dp_fc_a"]
    dp_fc_b = mixed_dataset["dp_fc_b"]

    with sql_engine_private_vis.begin() as con:
        data = pd.read_sql("""
            SELECT dp_id, fc_time, obs_time, value FROM forecasts ORDER BY dp_id, fc_time, obs_time, value;
        """, con)

    pd.testing.assert_frame_equal(data, pd.DataFrame({
        "dp_id": [
            dp_fc_a, dp_fc_a, dp_fc_a, dp_fc_a,
            dp_fc_a, dp_fc_a, dp_fc_a, dp_fc_a,
            dp_fc_b, dp_fc_b, dp_fc_b, dp_fc_b
        ],
        "fc_time": pd.to_datetime([
            "2024-12-23T00:00:00Z", "2024-12-23T00:00:00Z", "2024-12-23T00:00:00Z", "2024-12-23T00:00:00Z",
            "2024-12-23T12:00:00Z", "2024-12-23T12:00:00Z", "2024-12-23T12:00:00Z", "2024-12-23T12:00:00Z",
            "2024-12-23T00:00:00Z", "2024-12-23T00:00:00Z", "2024-12-23T00:00:00Z", "2024-12-23T00:00:00Z",
        ]),
        "obs_time": pd.to_datetime([
            "2024-12-24T00:00:00Z", "2024-12-24T06:00:00Z", "2024-12-24T12:00:00Z", "2024-12-24T18:00:00Z",
            "2024-12-24T00:00:00Z", "2024-12-24T06:00:00Z", "2024-12-24T12:00:00Z", "2024-12-24T18:00:00Z",
            "2024-12-24T00:00:00Z", "2024-12-24T06:00:00Z", "2024-12-24T12:00:00Z", "2024-12-24T18:00:00Z"
        ]),
        "value": [
            1., 2., 3., 4.,
            1.5, 2.5, 3.5, 4.5,
            5., 6., 7., 8.
        ]
    }))


def test_ts_deny_public_vis(basic_dp_test_set, sql_engine_public_vis):
    """Tests whether direct access by the public visualization user is denied"""
    with pytest.raises(sqlalchemy.exc.ProgrammingError, match=".*permission denied for.*"):
        with sql_engine_public_vis.begin() as con:
            pd.read_sql("SELECT * FROM measurements;", con)

    with pytest.raises(sqlalchemy.exc.ProgrammingError, match=".*permission denied for.*"):
        with sql_engine_public_vis.begin() as con:
            pd.read_sql("SELECT * FROM forecasts;", con)
