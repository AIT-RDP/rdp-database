"""
Tests the primary data views that join the raw time series and the measurement details for easier access
"""
import pandas as pd
import pytest
import pandas
import sqlalchemy.engine


def test_unitemporal_double_public(typed_dataset, sql_engine_public_vis):
    """Tests the data access via the corresponding details view"""

    data = _get_unitemporal_data(sql_engine_public_vis, "double")
    pd.testing.assert_frame_equal(data, pd.DataFrame({
        "dp_id": [typed_dataset["loc2-dev0-pub-0-uni-dbl-1"]] * 2,
        "obs_time": pd.to_datetime(["2024-01-01T01:00:00Z", "2024-01-01T02:00:00Z"]),
        "value": [-3., -4.],
        "name": ["name_1"] * 2,
        "device_id": ["device_0"] * 2,
        "location_code": ["location_2"] * 2,
        "data_provider": ["provider_1"] * 2,
        "unit": [None] * 2,
        "view_role": ["view_public"] * 2,
        "metadata": [{}] * 2,
        "data_type": ["double"] * 2,
        "temporality": ["unitemporal"] * 2
    }), check_names=False)


def test_unitemporal_double_private(typed_dataset, sql_engine_private_vis):
    """Tests the data access via the corresponding details view"""

    data = _get_unitemporal_data(sql_engine_private_vis, "double")
    pd.testing.assert_frame_equal(data, pd.DataFrame({
        "dp_id": [typed_dataset["loc2-dev0-pr-0-uni-dbl-0"]] * 2 + [typed_dataset["loc2-dev0-pub-0-uni-dbl-1"]] * 2,
        "obs_time": pd.to_datetime([
            "2024-01-01T01:00:00Z", "2024-01-01T02:00:00Z",
            "2024-01-01T01:00:00Z", "2024-01-01T02:00:00Z"
        ]),
        "value": [1., 2., -3., -4.],
        "name": ["name_0"] * 2 + ["name_1"] * 2,
        "device_id": ["device_0"] * 4,
        "location_code": ["location_2"] * 4,
        "data_provider": ["provider_1"] * 4,
        "unit": [None] * 4,
        "view_role": ["view_internal"] * 2 + ["view_public"] * 2,
        "metadata": [{}] * 4,
        "data_type": ["double"] * 4,
        "temporality": ["unitemporal"] * 4
    }), check_names=False)


def test_unitemporal_bigint_public(typed_dataset, sql_engine_public_vis):
    """Tests the data access via the corresponding details view"""

    data = _get_unitemporal_data(sql_engine_public_vis, "bigint")
    pd.testing.assert_frame_equal(data, pd.DataFrame({
        "dp_id": [typed_dataset["loc2-dev0-pub-0-uni-int-1"]] * 2,
        "obs_time": pd.to_datetime(["2024-01-01T01:00:00Z", "2024-01-01T02:00:00Z"]),
        "value": [-3, -4],
        "name": ["name_1"] * 2,
        "device_id": ["device_0"] * 2,
        "location_code": ["location_3"] * 2,
        "data_provider": ["provider_1"] * 2,
        "unit": [None] * 2,
        "view_role": ["view_public"] * 2,
        "metadata": [{}] * 2,
        "data_type": ["bigint"] * 2,
        "temporality": ["unitemporal"] * 2
    }), check_names=False)


def test_unitemporal_bigint_private(typed_dataset, sql_engine_private_vis):
    """Tests the data access via the corresponding details view"""

    data = _get_unitemporal_data(sql_engine_private_vis, "bigint")
    pd.testing.assert_frame_equal(data, pd.DataFrame({
        "dp_id": [typed_dataset["loc2-dev0-pr-0-uni-int-0"]] * 2 + [typed_dataset["loc2-dev0-pub-0-uni-int-1"]] * 2,
        "obs_time": pd.to_datetime([
            "2024-01-01T01:00:00Z", "2024-01-01T02:00:00Z",
            "2024-01-01T01:00:00Z", "2024-01-01T02:00:00Z"
        ]),
        "value": [1, 2, -3, -4],
        "name": ["name_0"] * 2 + ["name_1"] * 2,
        "device_id": ["device_0"] * 4,
        "location_code": ["location_3"] * 4,
        "data_provider": ["provider_1"] * 4,
        "unit": [None] * 4,
        "view_role": ["view_internal"] * 2 + ["view_public"] * 2,
        "metadata": [{}] * 4,
        "data_type": ["bigint"] * 4,
        "temporality": ["unitemporal"] * 4
    }), check_names=False)


def test_unitemporal_boolean_public(typed_dataset, sql_engine_public_vis):
    """Tests the data access via the corresponding details view"""

    data = _get_unitemporal_data(sql_engine_public_vis, "boolean")
    pd.testing.assert_frame_equal(data, pd.DataFrame({
        "dp_id": [typed_dataset["loc2-dev0-pub-0-uni-bool-1"]] * 2,
        "obs_time": pd.to_datetime(["2024-01-01T01:00:00Z", "2024-01-01T02:00:00Z"]),
        "value": [False, True],
        "name": ["name_1"] * 2,
        "device_id": ["device_0"] * 2,
        "location_code": ["location_4"] * 2,
        "data_provider": ["provider_1"] * 2,
        "unit": [None] * 2,
        "view_role": ["view_public"] * 2,
        "metadata": [{}] * 2,
        "data_type": ["boolean"] * 2,
        "temporality": ["unitemporal"] * 2
    }), check_names=False)


def test_unitemporal_boolean_private(typed_dataset, sql_engine_private_vis):
    """Tests the data access via the corresponding details view"""

    data = _get_unitemporal_data(sql_engine_private_vis, "boolean")
    pd.testing.assert_frame_equal(data, pd.DataFrame({
        "dp_id": [typed_dataset["loc2-dev0-pr-0-uni-bool-0"]] * 2 + [typed_dataset["loc2-dev0-pub-0-uni-bool-1"]] * 2,
        "obs_time": pd.to_datetime([
            "2024-01-01T01:00:00Z", "2024-01-01T02:00:00Z",
            "2024-01-01T01:00:00Z", "2024-01-01T02:00:00Z"
        ]),
        "value": [True, False, False, True],
        "name": ["name_0"] * 2 + ["name_1"] * 2,
        "device_id": ["device_0"] * 4,
        "location_code": ["location_4"] * 4,
        "data_provider": ["provider_1"] * 4,
        "unit": [None] * 4,
        "view_role": ["view_internal"] * 2 + ["view_public"] * 2,
        "metadata": [{}] * 4,
        "data_type": ["boolean"] * 4,
        "temporality": ["unitemporal"] * 4
    }), check_names=False)


def test_unitemporal_jsonb_public(typed_dataset, sql_engine_public_vis):
    """Tests the data access via the corresponding details view"""

    data = _get_unitemporal_data(sql_engine_public_vis, "jsonb")
    pd.testing.assert_frame_equal(data, pd.DataFrame({
        "dp_id": [typed_dataset["loc2-dev0-pub-0-uni-json-1"]] * 2,
        "obs_time": pd.to_datetime(["2024-01-01T01:00:00Z", "2024-01-01T02:00:00Z"]),
        "value": [{"myval": -3}, {"myval": -4}],
        "name": ["name_1"] * 2,
        "device_id": ["device_0"] * 2,
        "location_code": ["location_5"] * 2,
        "data_provider": ["provider_1"] * 2,
        "unit": [None] * 2,
        "view_role": ["view_public"] * 2,
        "metadata": [{}] * 2,
        "data_type": ["jsonb"] * 2,
        "temporality": ["unitemporal"] * 2
    }), check_names=False)


def test_unitemporal_jsonb_private(typed_dataset, sql_engine_private_vis):
    """Tests the data access via the corresponding details view"""

    data = _get_unitemporal_data(sql_engine_private_vis, "jsonb")
    pd.testing.assert_frame_equal(data, pd.DataFrame({
        "dp_id": [typed_dataset["loc2-dev0-pr-0-uni-json-0"]] * 2 + [typed_dataset["loc2-dev0-pub-0-uni-json-1"]] * 2,
        "obs_time": pd.to_datetime([
            "2024-01-01T01:00:00Z", "2024-01-01T02:00:00Z",
            "2024-01-01T01:00:00Z", "2024-01-01T02:00:00Z"
        ]),
        "value": [{"myval": 1}, {"myval": 2}, {"myval": -3}, {"myval": -4}],
        "name": ["name_0"] * 2 + ["name_1"] * 2,
        "device_id": ["device_0"] * 4,
        "location_code": ["location_5"] * 4,
        "data_provider": ["provider_1"] * 4,
        "unit": [None] * 4,
        "view_role": ["view_internal"] * 2 + ["view_public"] * 2,
        "metadata": [{}] * 4,
        "data_type": ["jsonb"] * 4,
        "temporality": ["unitemporal"] * 4
    }), check_names=False)


def _get_unitemporal_data(engine: sqlalchemy.engine.Engine, type_name: str) -> pd.DataFrame:
    """Queries the data from the corresponding view"""

    with engine.begin() as con:
        return pd.read_sql(f"""
            SELECT dp_id, obs_time, value, name, device_id, location_code, data_provider, unit, view_role, metadata, 
                    data_type, temporality
                FROM unitemporal_{type_name}_details
                ORDER BY dp_id, obs_time;
        """, con)


def test_bitemporal_double_public(typed_dataset, sql_engine_public_vis):
    """Tests the data access via the corresponding details view"""

    data = _get_bitemporal_data(sql_engine_public_vis, "double")
    pd.testing.assert_frame_equal(data, pd.DataFrame({
        "dp_id": [typed_dataset["loc2-dev0-pub-0-bi-dbl-1"]] * 2,
        "fc_time": pd.to_datetime([
            "2024-01-01T01:00:00Z", "2024-01-01T02:00:00Z"
        ]),
        "obs_time": pd.to_datetime([
            "2024-01-02T01:00:00Z", "2024-01-02T02:00:00Z"
        ]),
        "value": [-3., -4.],
        "name": ["name_1"] * 2,
        "device_id": ["device_0"] * 2,
        "location_code": ["location_2"] * 2,
        "data_provider": ["provider_2"] * 2,
        "unit": [None] * 2,
        "view_role": ["view_public"] * 2,
        "metadata": [{}] * 2,
        "data_type": ["double"] * 2,
        "temporality": ["bitemporal"] * 2
    }), check_names=False)


def test_bitemporal_double_private(typed_dataset, sql_engine_private_vis):
    """Tests the data access via the corresponding details view"""

    data = _get_bitemporal_data(sql_engine_private_vis, "double")
    pd.testing.assert_frame_equal(data, pd.DataFrame({
        "dp_id": [typed_dataset["loc2-dev0-pr-0-bi-dbl-0"]] * 2 + [typed_dataset["loc2-dev0-pub-0-bi-dbl-1"]] * 2,
        "fc_time": pd.to_datetime([
            "2024-01-01T01:00:00Z", "2024-01-01T02:00:00Z",
            "2024-01-01T01:00:00Z", "2024-01-01T02:00:00Z"
        ]),
        "obs_time": pd.to_datetime([
            "2024-01-02T01:00:00Z", "2024-01-02T02:00:00Z",
            "2024-01-02T01:00:00Z", "2024-01-02T02:00:00Z"
        ]),
        "value": [1., 2., -3., -4.],
        "name": ["name_0"] * 2 + ["name_1"] * 2,
        "device_id": ["device_0"] * 4,
        "location_code": ["location_2"] * 4,
        "data_provider": ["provider_2"] * 4,
        "unit": [None] * 4,
        "view_role": ["view_internal"] * 2 + ["view_public"] * 2,
        "metadata": [{}] * 4,
        "data_type": ["double"] * 4,
        "temporality": ["bitemporal"] * 4
    }), check_names=False)


def test_bitemporal_bigint_public(typed_dataset, sql_engine_public_vis):
    """Tests the data access via the corresponding details view"""

    data = _get_bitemporal_data(sql_engine_public_vis, "bigint")
    pd.testing.assert_frame_equal(data, pd.DataFrame({
        "dp_id": [typed_dataset["loc2-dev0-pub-0-bi-int-1"]] * 2,
        "fc_time": pd.to_datetime([
            "2024-01-01T01:00:00Z", "2024-01-01T02:00:00Z"
        ]),
        "obs_time": pd.to_datetime([
            "2024-01-02T01:00:00Z", "2024-01-02T02:00:00Z"
        ]),
        "value": [-3, -4],
        "name": ["name_1"] * 2,
        "device_id": ["device_0"] * 2,
        "location_code": ["location_3"] * 2,
        "data_provider": ["provider_2"] * 2,
        "unit": [None] * 2,
        "view_role": ["view_public"] * 2,
        "metadata": [{}] * 2,
        "data_type": ["bigint"] * 2,
        "temporality": ["bitemporal"] * 2
    }), check_names=False)


def test_bitemporal_bigint_private(typed_dataset, sql_engine_private_vis):
    """Tests the data access via the corresponding details view"""

    data = _get_bitemporal_data(sql_engine_private_vis, "bigint")
    pd.testing.assert_frame_equal(data, pd.DataFrame({
        "dp_id": [typed_dataset["loc2-dev0-pr-0-bi-int-0"]] * 2 + [typed_dataset["loc2-dev0-pub-0-bi-int-1"]] * 2,
        "fc_time": pd.to_datetime([
            "2024-01-01T01:00:00Z", "2024-01-01T02:00:00Z",
            "2024-01-01T01:00:00Z", "2024-01-01T02:00:00Z"
        ]),
        "obs_time": pd.to_datetime([
            "2024-01-02T01:00:00Z", "2024-01-02T02:00:00Z",
            "2024-01-02T01:00:00Z", "2024-01-02T02:00:00Z"
        ]),
        "value": [1, 2, -3, -4],
        "name": ["name_0"] * 2 + ["name_1"] * 2,
        "device_id": ["device_0"] * 4,
        "location_code": ["location_3"] * 4,
        "data_provider": ["provider_2"] * 4,
        "unit": [None] * 4,
        "view_role": ["view_internal"] * 2 + ["view_public"] * 2,
        "metadata": [{}] * 4,
        "data_type": ["bigint"] * 4,
        "temporality": ["bitemporal"] * 4
    }), check_names=False)


def test_bitemporal_boolean_public(typed_dataset, sql_engine_public_vis):
    """Tests the data access via the corresponding details view"""

    data = _get_bitemporal_data(sql_engine_public_vis, "boolean")
    pd.testing.assert_frame_equal(data, pd.DataFrame({
        "dp_id": [typed_dataset["loc2-dev0-pub-0-bi-bool-1"]] * 2,
        "fc_time": pd.to_datetime([
            "2024-01-01T01:00:00Z", "2024-01-01T02:00:00Z"
        ]),
        "obs_time": pd.to_datetime([
            "2024-01-02T01:00:00Z", "2024-01-02T02:00:00Z"
        ]),
        "value": [False, True],
        "name": ["name_1"] * 2,
        "device_id": ["device_0"] * 2,
        "location_code": ["location_4"] * 2,
        "data_provider": ["provider_2"] * 2,
        "unit": [None] * 2,
        "view_role": ["view_public"] * 2,
        "metadata": [{}] * 2,
        "data_type": ["boolean"] * 2,
        "temporality": ["bitemporal"] * 2
    }), check_names=False)


def test_bitemporal_boolean_private(typed_dataset, sql_engine_private_vis):
    """Tests the data access via the corresponding details view"""

    data = _get_bitemporal_data(sql_engine_private_vis, "boolean")
    pd.testing.assert_frame_equal(data, pd.DataFrame({
        "dp_id": [typed_dataset["loc2-dev0-pr-0-bi-bool-0"]] * 2 + [typed_dataset["loc2-dev0-pub-0-bi-bool-1"]] * 2,
        "fc_time": pd.to_datetime([
            "2024-01-01T01:00:00Z", "2024-01-01T02:00:00Z",
            "2024-01-01T01:00:00Z", "2024-01-01T02:00:00Z"
        ]),
        "obs_time": pd.to_datetime([
            "2024-01-02T01:00:00Z", "2024-01-02T02:00:00Z",
            "2024-01-02T01:00:00Z", "2024-01-02T02:00:00Z"
        ]),
        "value": [True, False, False, True],
        "name": ["name_0"] * 2 + ["name_1"] * 2,
        "device_id": ["device_0"] * 4,
        "location_code": ["location_4"] * 4,
        "data_provider": ["provider_2"] * 4,
        "unit": [None] * 4,
        "view_role": ["view_internal"] * 2 + ["view_public"] * 2,
        "metadata": [{}] * 4,
        "data_type": ["boolean"] * 4,
        "temporality": ["bitemporal"] * 4
    }), check_names=False)


def test_bitemporal_jsonb_public(typed_dataset, sql_engine_public_vis):
    """Tests the data access via the corresponding details view"""

    data = _get_bitemporal_data(sql_engine_public_vis, "jsonb")
    pd.testing.assert_frame_equal(data, pd.DataFrame({
        "dp_id": [typed_dataset["loc2-dev0-pub-0-bi-json-1"]] * 2,
        "fc_time": pd.to_datetime([
            "2024-01-01T01:00:00Z", "2024-01-01T02:00:00Z"
        ]),
        "obs_time": pd.to_datetime([
            "2024-01-02T01:00:00Z", "2024-01-02T02:00:00Z"
        ]),
        "value": [{"myval": -3}, {"myval": -4}],
        "name": ["name_1"] * 2,
        "device_id": ["device_0"] * 2,
        "location_code": ["location_5"] * 2,
        "data_provider": ["provider_2"] * 2,
        "unit": [None] * 2,
        "view_role": ["view_public"] * 2,
        "metadata": [{}] * 2,
        "data_type": ["jsonb"] * 2,
        "temporality": ["bitemporal"] * 2
    }), check_names=False)


def test_bitemporal_jsonb_private(typed_dataset, sql_engine_private_vis):
    """Tests the data access via the corresponding details view"""

    data = _get_bitemporal_data(sql_engine_private_vis, "jsonb")
    pd.testing.assert_frame_equal(data, pd.DataFrame({
        "dp_id": [typed_dataset["loc2-dev0-pr-0-bi-json-0"]] * 2 + [typed_dataset["loc2-dev0-pub-0-bi-json-1"]] * 2,
        "fc_time": pd.to_datetime([
            "2024-01-01T01:00:00Z", "2024-01-01T02:00:00Z",
            "2024-01-01T01:00:00Z", "2024-01-01T02:00:00Z"
        ]),
        "obs_time": pd.to_datetime([
            "2024-01-02T01:00:00Z", "2024-01-02T02:00:00Z",
            "2024-01-02T01:00:00Z", "2024-01-02T02:00:00Z"
        ]),
        "value": [{"myval": 1}, {"myval": 2}, {"myval": -3}, {"myval": -4}],
        "name": ["name_0"] * 2 + ["name_1"] * 2,
        "device_id": ["device_0"] * 4,
        "location_code": ["location_5"] * 4,
        "data_provider": ["provider_2"] * 4,
        "unit": [None] * 4,
        "view_role": ["view_internal"] * 2 + ["view_public"] * 2,
        "metadata": [{}] * 4,
        "data_type": ["jsonb"] * 4,
        "temporality": ["bitemporal"] * 4
    }), check_names=False)


def _get_bitemporal_data(engine: sqlalchemy.engine.Engine, type_name: str) -> pd.DataFrame:
    """Queries the data from the corresponding view"""

    with engine.begin() as con:
        return pd.read_sql(f"""
            SELECT dp_id, fc_time, obs_time, value, name, device_id, location_code, data_provider, unit, view_role, metadata, 
                    data_type, temporality
                FROM bitemporal_{type_name}_details
                ORDER BY dp_id, fc_time, obs_time;
        """, con)
