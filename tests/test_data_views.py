"""
Tests the primary data vies (details and filtered samples) including their access control policies
"""
import numpy as np
import pandas as pd


def test_measurements_details_public(mixed_dataset, sql_engine_public_vis):
    """Tests the details test set with a public user"""

    with sql_engine_public_vis.begin() as con:
        data = pd.read_sql("""
            SELECT dp_id, name, device_id, location_code, data_provider, obs_time, value, unit, view_role, metadata
                FROM measurements_details
                ORDER BY name, device_id, location_code, data_provider, obs_time
        """, con)

    assert data is not None
    assert isinstance(data, pd.DataFrame)

    pd.testing.assert_frame_equal(data, pd.DataFrame({
        "dp_id": [mixed_dataset["loc0-dev0-pub-0"]] * 4,
        "name": ["name_0"] * 4,
        "device_id": ["device_0"] * 4,
        "location_code": ["location_0"] * 4,
        "data_provider": ["provider_0"] * 4,
        "obs_time": pd.to_datetime([
            "2024-12-24T00:00:00Z", "2024-12-24T06:00:00Z", "2024-12-24T12:00:00Z", "2024-12-24T18:00:00Z"
        ]),
        "value": [1., 2., 3., 4.],
        "unit": ["ISO football fields"] * 4,
        "view_role": ["view_public"] * 4,
        "metadata": [{"note": "with 10mm grass only"}] * 4
    }), check_names=False)


def test_measurements_details_private(mixed_dataset, sql_engine_private_vis):
    """Tests the details test set with a public user"""

    with sql_engine_private_vis.begin() as con:
        data = pd.read_sql("""
            SELECT dp_id, name, device_id, location_code, data_provider, obs_time, value, unit, view_role, metadata
                FROM measurements_details
                ORDER BY name, device_id, location_code, data_provider, obs_time
        """, con)

    assert data is not None
    assert isinstance(data, pd.DataFrame)

    pd.testing.assert_frame_equal(data, pd.DataFrame({
        "dp_id": [mixed_dataset["loc0-dev0-pub-0"]] * 4 + [mixed_dataset["loc0-dev0-pr-2"]] * 4,
        "name": ["name_0"] * 4 + ["name_2"] * 4,
        "device_id": ["device_0"] * 8,
        "location_code": ["location_0"] * 8,
        "data_provider": ["provider_0"] * 8,
        "obs_time": pd.to_datetime([
            "2024-12-24T00:00:00Z", "2024-12-24T06:00:00Z", "2024-12-24T12:00:00Z", "2024-12-24T18:00:00Z",
            "2024-12-24T00:00:00Z", "2024-12-24T06:00:00Z", "2024-12-24T12:00:00Z", "2024-12-24T18:00:00Z",
        ]),
        "value": [1., 2., 3., 4., 5., 6., 7., 8.],
        "unit": ["ISO football fields"] * 4 + [None] * 4,
        "view_role": ["view_public"] * 4 + ["view_internal"] * 4,
        "metadata": [{"note": "with 10mm grass only"}] * 4 + [{}] * 4
    }), check_names=False)


def test_measurements_details_private(mixed_dataset, sql_engine_private_vis):
    """Tests the details test set with a private user"""

    with sql_engine_private_vis.begin() as con:
        data = pd.read_sql("""
            SELECT dp_id, name, device_id, location_code, data_provider, obs_time, value, unit, view_role, metadata
                FROM measurements_details
                ORDER BY name, device_id, location_code, data_provider, obs_time
        """, con)

    assert data is not None
    assert isinstance(data, pd.DataFrame)

    pd.testing.assert_frame_equal(data, pd.DataFrame({
        "dp_id": [mixed_dataset["loc0-dev0-pub-0"]] * 4 + [mixed_dataset["loc0-dev0-pr-2"]] * 4,
        "name": ["name_0"] * 4 + ["name_2"] * 4,
        "device_id": ["device_0"] * 8,
        "location_code": ["location_0"] * 8,
        "data_provider": ["provider_0"] * 8,
        "obs_time": pd.to_datetime([
            "2024-12-24T00:00:00Z", "2024-12-24T06:00:00Z", "2024-12-24T12:00:00Z", "2024-12-24T18:00:00Z",
            "2024-12-24T00:00:00Z", "2024-12-24T06:00:00Z", "2024-12-24T12:00:00Z", "2024-12-24T18:00:00Z",
        ]),
        "value": [1., 2., 3., 4., 5., 6., 7., 8.],
        "unit": ["ISO football fields"] * 4 + [None] * 4,
        "view_role": ["view_public"] * 4 + ["view_internal"] * 4,
        "metadata": [{"note": "with 10mm grass only"}] * 4 + [{}] * 4
    }), check_names=False)


def test_forecasts_details_public(mixed_dataset, sql_engine_public_vis):
    """Tests the details test set with a public user"""

    with sql_engine_public_vis.begin() as con:
        data = pd.read_sql("""
            SELECT dp_id, name, device_id, location_code, data_provider, fc_time, obs_time, value, unit, view_role, metadata
                FROM forecasts_details
                ORDER BY name, device_id, location_code, data_provider, fc_time, obs_time
        """, con)

    assert data is not None
    assert isinstance(data, pd.DataFrame)

    pd.testing.assert_frame_equal(data, pd.DataFrame({
        "dp_id": [mixed_dataset["loc0-dev0-pub-1"]] * 8,
        "name": ["name_1"] * 8,
        "device_id": ["device_0"] * 8,
        "location_code": ["location_0"] * 8,
        "data_provider": ["provider_0"] * 8,
        "fc_time": pd.to_datetime(
            ["2024-12-23T00:00:00Z"] * 4 + ["2024-12-23T12:00:00Z"] * 4
        ),
        "obs_time": pd.to_datetime([
            "2024-12-24T00:00:00Z", "2024-12-24T06:00:00Z", "2024-12-24T12:00:00Z", "2024-12-24T18:00:00Z",
            "2024-12-24T00:00:00Z", "2024-12-24T06:00:00Z", "2024-12-24T12:00:00Z", "2024-12-24T18:00:00Z"
        ]),
        "value": [
            1., 2., 3., 4.,
            1.5, 2.5, 3.5, 4.5,
        ],
        "unit": ["DIN bathtubs"] * 8,
        "view_role": ["view_public"] * 8,
        "metadata": [{"note": "don't trust the units"}] * 8
    }), check_names=False)


def test_forecasts_details_private(mixed_dataset, sql_engine_private_vis):
    """Tests the details test set with a public user"""

    with sql_engine_private_vis.begin() as con:
        data = pd.read_sql("""
            SELECT dp_id, name, device_id, location_code, data_provider, fc_time, obs_time, value, unit, view_role, metadata
                FROM forecasts_details
                ORDER BY name, device_id, location_code, data_provider, fc_time, obs_time
        """, con)

    assert data is not None
    assert isinstance(data, pd.DataFrame)

    pd.testing.assert_frame_equal(data, pd.DataFrame({
        "dp_id": [mixed_dataset["loc0-dev0-pub-1"]] * 8 + [mixed_dataset["loc0-dev0-pr-2"]] * 4,
        "name": ["name_1"] * 8 + ["name_2"] * 4,
        "device_id": ["device_0"] * 12,
        "location_code": ["location_0"] * 12,
        "data_provider": ["provider_0"] * 12,
        "fc_time": pd.to_datetime(
            ["2024-12-23T00:00:00Z"] * 4 + ["2024-12-23T12:00:00Z"] * 4 + ["2024-12-23T00:00:00Z"] * 4
        ),
        "obs_time": pd.to_datetime([
            "2024-12-24T00:00:00Z", "2024-12-24T06:00:00Z", "2024-12-24T12:00:00Z", "2024-12-24T18:00:00Z",
            "2024-12-24T00:00:00Z", "2024-12-24T06:00:00Z", "2024-12-24T12:00:00Z", "2024-12-24T18:00:00Z",
            "2024-12-24T00:00:00Z", "2024-12-24T06:00:00Z", "2024-12-24T12:00:00Z", "2024-12-24T18:00:00Z",
        ]),
        "value": [
            1., 2., 3., 4.,
            1.5, 2.5, 3.5, 4.5,
            5., 6., 7., 8.
        ],
        "unit": ["DIN bathtubs"] * 8 + [None] * 4,
        "view_role": ["view_public"] * 8 + ["view_internal"] * 4,
        "metadata": [{"note": "don't trust the units"}] * 8 + [{}] * 4
    }), check_names=False)


def test_measurements_samples_private(mixed_dataset, sql_engine_private_vis):
    """Tests the details test set with a private user"""

    with sql_engine_private_vis.begin() as con:
        data = pd.read_sql("""
            SELECT dp_id, obs_time, value
                FROM measurements_samples
                ORDER BY dp_id, obs_time
        """, con)

    assert data is not None
    assert isinstance(data, pd.DataFrame)

    pd.testing.assert_frame_equal(data, pd.DataFrame({
        "dp_id": [mixed_dataset["loc0-dev0-pub-0"]] * 4 + [mixed_dataset["loc0-dev0-pr-2"]] * 4,
        "obs_time": pd.to_datetime([
            "2024-12-24T00:00:00Z", "2024-12-24T06:00:00Z", "2024-12-24T12:00:00Z", "2024-12-24T18:00:00Z",
            "2024-12-24T00:00:00Z", "2024-12-24T06:00:00Z", "2024-12-24T12:00:00Z", "2024-12-24T18:00:00Z",
        ]),
        "value": [1., 2., 3., 4., 5., 6., 7., 8.],
    }), check_names=False)


def test_measurements_samples_public(mixed_dataset, sql_engine_public_vis):
    """Tests the details test set with a public user"""

    with sql_engine_public_vis.begin() as con:
        data = pd.read_sql("""
            SELECT dp_id, obs_time, value
                FROM measurements_samples
                ORDER BY dp_id, obs_time
        """, con)

    assert data is not None
    assert isinstance(data, pd.DataFrame)

    pd.testing.assert_frame_equal(data, pd.DataFrame({
        "dp_id": [mixed_dataset["loc0-dev0-pub-0"]] * 4,
        "obs_time": pd.to_datetime([
            "2024-12-24T00:00:00Z", "2024-12-24T06:00:00Z", "2024-12-24T12:00:00Z", "2024-12-24T18:00:00Z",
        ]),
        "value": [1., 2., 3., 4.],
    }), check_names=False)


def test_forecasts_samples_private(mixed_dataset, sql_engine_private_vis):
    """Tests the details test set with a public user"""

    with sql_engine_private_vis.begin() as con:
        data = pd.read_sql("""
            SELECT dp_id, fc_time, obs_time, value
                FROM forecasts_samples
                ORDER BY dp_id, fc_time, obs_time
        """, con)

    assert data is not None
    assert isinstance(data, pd.DataFrame)

    pd.testing.assert_frame_equal(data, pd.DataFrame({
        "dp_id": [mixed_dataset["loc0-dev0-pub-1"]] * 8 + [mixed_dataset["loc0-dev0-pr-2"]] * 4,
        "fc_time": pd.to_datetime(
            ["2024-12-23T00:00:00Z"] * 4 + ["2024-12-23T12:00:00Z"] * 4 + ["2024-12-23T00:00:00Z"] * 4
        ),
        "obs_time": pd.to_datetime([
            "2024-12-24T00:00:00Z", "2024-12-24T06:00:00Z", "2024-12-24T12:00:00Z", "2024-12-24T18:00:00Z",
            "2024-12-24T00:00:00Z", "2024-12-24T06:00:00Z", "2024-12-24T12:00:00Z", "2024-12-24T18:00:00Z",
            "2024-12-24T00:00:00Z", "2024-12-24T06:00:00Z", "2024-12-24T12:00:00Z", "2024-12-24T18:00:00Z",
        ]),
        "value": [
            1., 2., 3., 4.,
            1.5, 2.5, 3.5, 4.5,
            5., 6., 7., 8.
        ],
    }), check_names=False)


def test_forecasts_samples_public(mixed_dataset, sql_engine_public_vis):
    """Tests the details test set with a public user"""

    with sql_engine_public_vis.begin() as con:
        data = pd.read_sql("""
            SELECT dp_id, fc_time, obs_time, value
                FROM forecasts_samples
                ORDER BY dp_id, fc_time, obs_time
        """, con)

    assert data is not None
    assert isinstance(data, pd.DataFrame)

    pd.testing.assert_frame_equal(data, pd.DataFrame({
        "dp_id": [mixed_dataset["loc0-dev0-pub-1"]] * 8,
        "fc_time": pd.to_datetime(
            ["2024-12-23T00:00:00Z"] * 4 + ["2024-12-23T12:00:00Z"] * 4
        ),
        "obs_time": pd.to_datetime([
            "2024-12-24T00:00:00Z", "2024-12-24T06:00:00Z", "2024-12-24T12:00:00Z", "2024-12-24T18:00:00Z",
            "2024-12-24T00:00:00Z", "2024-12-24T06:00:00Z", "2024-12-24T12:00:00Z", "2024-12-24T18:00:00Z"
        ]),
        "value": [
            1., 2., 3., 4.,
            1.5, 2.5, 3.5, 4.5,
        ]
    }), check_names=False)
