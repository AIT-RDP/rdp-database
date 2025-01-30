"""
Tests the basic data point creation and update logic
"""

import pandas as pd
import pytest
import sqlalchemy.exc
import sqlalchemy.sql as sql


def test_direct_dp_creation_minimal(clean_db, sql_engine_data_source):
    """Tests directly creating a data point by inserting into the table"""

    with sql_engine_data_source.begin() as con:
        con.execute(sql.text("""
            INSERT INTO data_points(name, device_id, location_code, data_provider) VALUES
                ('name_0', 'device_0', 'nowhere special', 'crystal ball'),
                ('name_1', NULL, 'here', 'common knowledge');
        """))

        data = pd.read_sql("""
            SELECT id, name, device_id, location_code, data_provider, unit, view_role, metadata
                FROM data_points
                ORDER BY name;
        """, con)

    assert data is not None
    assert isinstance(data, pd.DataFrame)

    pd.testing.assert_series_equal(data["name"], pd.Series(["name_0", "name_1"]), check_names=False)
    pd.testing.assert_series_equal(data["device_id"], pd.Series(["device_0", None]), check_names=False)
    pd.testing.assert_series_equal(data["location_code"], pd.Series(["nowhere special", "here"]), check_names=False)
    pd.testing.assert_series_equal(data["unit"], pd.Series([None, None]), check_names=False)
    pd.testing.assert_series_equal(data["view_role"], pd.Series(["view_internal", "view_internal"]), check_names=False)
    pd.testing.assert_series_equal(data["metadata"], pd.Series([{}, {}]), check_names=False)
    pd.testing.assert_series_equal(data["id"], pd.Series([1, 2]), check_names=False)


def test_direct_dp_creation_extensive(clean_db, sql_engine_data_source):
    """
    Tests directly creating a data point by inserting into the table

    In contrast to the minimal variant, additional elements are populated
    """

    with sql_engine_data_source.begin() as con:
        con.execute(sql.text("""
            INSERT INTO data_points(name, device_id, location_code, data_provider, view_role, metadata) VALUES
                ('name_0', 'device_0', 'nowhere special', 'crystal ball', 'view_internal', '{"hello": "json"}'),
                ('name_1', NULL, 'here', 'common knowledge', 'view_public', '"hi everyone"');
        """))

        data = pd.read_sql("""
            SELECT id, name, device_id, location_code, data_provider, unit, view_role, metadata
                FROM data_points
                ORDER BY name;
        """, con)

    assert data is not None
    assert isinstance(data, pd.DataFrame)

    pd.testing.assert_series_equal(data["name"], pd.Series(["name_0", "name_1"]), check_names=False)
    pd.testing.assert_series_equal(data["device_id"], pd.Series(["device_0", None]), check_names=False)
    pd.testing.assert_series_equal(data["location_code"], pd.Series(["nowhere special", "here"]), check_names=False)
    pd.testing.assert_series_equal(data["unit"], pd.Series([None, None]), check_names=False)
    pd.testing.assert_series_equal(data["view_role"], pd.Series(["view_internal", "view_public"]), check_names=False)
    pd.testing.assert_series_equal(data["metadata"], pd.Series([{"hello": "json"}, "hi everyone"]), check_names=False)
    pd.testing.assert_series_equal(data["id"], pd.Series([1, 2]), check_names=False)


def test_direct_dp_creation_target_series(clean_db, sql_engine_data_source):
    """
    Tests the insert mechanism having some target series specification
    """

    with sql_engine_data_source.begin() as con:
        con.execute(sql.text("""
            INSERT INTO data_points(name, device_id, location_code, data_provider, data_type, temporality) VALUES
                ('name_0', 'device_0', 'nowhere special', 'crystal ball', 'double', 'unitemporal'),
                ('name_1', NULL, 'here', 'common knowledge', 'double', 'bitemporal'),
                ('name_2', NULL, 'here', 'common knowledge', 'double', NULL);
        """))

        data = pd.read_sql("""
            SELECT id, name, data_type, temporality
                FROM data_points
                ORDER BY name;
        """, con)

    assert data is not None
    assert isinstance(data, pd.DataFrame)

    pd.testing.assert_series_equal(data["name"], pd.Series(["name_0", "name_1", "name_2"]), check_names=False)
    pd.testing.assert_series_equal(data["data_type"], pd.Series(["double", "double", "double"]), check_names=False)
    pd.testing.assert_series_equal(data["temporality"], pd.Series([
        "unitemporal", "bitemporal", None
    ]), check_names=False)
    pd.testing.assert_series_equal(data["id"], pd.Series([1, 2, 3]), check_names=False)


def test_role_based_dp_access(clean_db, sql_engine_data_source, sql_engine_private_vis, sql_engine_public_vis):
    """Tests the view roles and their access privileges on the data points table"""

    # Insert the data points
    with sql_engine_data_source.begin() as con:
        con.execute(sql.text("""
            INSERT INTO data_points(name, device_id, location_code, data_provider, view_role) VALUES
                ('name_0', 'device_0', 'nowhere special', 'crystal ball', 'view_internal'),
                ('name_1', NULL, 'here', 'common knowledge', 'view_public');
        """))

    # Test the access right using the private user
    with sql_engine_private_vis.begin() as con:
        data = pd.read_sql("""SELECT name FROM data_points ORDER BY name;""", con)
        pd.testing.assert_series_equal(data["name"], pd.Series(["name_0", "name_1"]), check_names=False)

    # Test the access right using the public user
    with sql_engine_public_vis.begin() as con:
        data = pd.read_sql("""SELECT name FROM data_points ORDER BY name;""", con)
        pd.testing.assert_series_equal(data["name"], pd.Series(["name_1"]), check_names=False)


def test_dp_update(clean_db, sql_engine_data_source):
    """Tests updating the data points after they have been created"""

    with sql_engine_data_source.begin() as con:
        con.execute(sql.text("""
            INSERT INTO data_points(name, device_id, location_code, data_provider, view_role) VALUES
                ('name_0', 'device_0', 'nowhere special', 'crystal ball', 'view_internal'),
                ('name_1', NULL, 'here', 'common knowledge', 'view_public');
        """))

    with sql_engine_data_source.begin() as con:
        con.execute(sql.text("""
            UPDATE data_points SET metadata= '{"hello":"testing"}'::JSONB WHERE name = 'name_0';
            UPDATE data_points SET name='name_2' WHERE name = 'name_1';
        """))

    with sql_engine_data_source.begin() as con:
        data = pd.read_sql("""SELECT * FROM data_points ORDER BY name;""", con)

    pd.testing.assert_series_equal(data["name"], pd.Series(["name_0", "name_2"]), check_names=False)
    pd.testing.assert_series_equal(data["metadata"], pd.Series([{"hello": "testing"}, {}]), check_names=False)


def test_get_or_create_data_point_id_basic(clean_db, sql_engine_data_source):
    """Tests the on-the-fly data point creation"""

    with sql_engine_data_source.begin() as con:
        # Add the first entry
        res_1 = pd.read_sql("""
            SELECT get_or_create_data_point_id(
                    'name_2', 'device_0', 'over there', 'magic', '1', '{"":""}'::jsonb
                ) as dp_id;
        """, con)
        assert res_1 is not None
        assert res_1.index.size == 1
        pd.testing.assert_series_equal(res_1["dp_id"], pd.Series([1]), check_names=False)

        # Add just another entry to test whether something is spuriously overwritten
        res_2 = pd.read_sql("""
            SELECT get_or_create_data_point_id('name_3', 'device_1', 'here', 'hard work') as dp_id;
        """, con)
        assert res_2 is not None
        assert res_2.index.size == 1
        pd.testing.assert_series_equal(res_2["dp_id"], pd.Series([2]), check_names=False)

        # Gather the first entry again. Per definition, the metadata and unit fields should not be updated.
        res_3 = pd.read_sql("""
            SELECT get_or_create_data_point_id(
                    'name_2', 'device_0', 'over there', 'magic', '2', '{"--":"--"}'::jsonb
                ) as dp_id;
        """, con)
        assert res_3 is not None
        assert res_3.index.size == 1
        pd.testing.assert_series_equal(res_3["dp_id"], res_1["dp_id"], check_names=False)

    # Query the whole datapoints table
    with sql_engine_data_source.begin() as con:
        data = pd.read_sql("""SELECT * FROM data_points ORDER BY name;""", con)

    pd.testing.assert_series_equal(data["name"], pd.Series(["name_2", "name_3"]), check_names=False)
    pd.testing.assert_series_equal(data["device_id"], pd.Series(["device_0", "device_1"]), check_names=False)
    pd.testing.assert_series_equal(data["location_code"], pd.Series(["over there", "here"]), check_names=False)
    pd.testing.assert_series_equal(data["data_provider"], pd.Series(["magic", "hard work"]), check_names=False)
    pd.testing.assert_series_equal(data["view_role"], pd.Series(["view_internal", "view_internal"]), check_names=False)
    pd.testing.assert_series_equal(data["unit"], pd.Series(["1", None]), check_names=False)
    pd.testing.assert_series_equal(data["metadata"], pd.Series([{"": ""}, {}]), check_names=False)


def test_basic_dp_test_set(basic_dp_test_set, sql_engine_data_source):
    """Tests the test-set itself"""

    with sql_engine_data_source.begin() as con:
        data = pd.read_sql("""
            SELECT id, name, device_id, location_code, data_provider, view_role 
                FROM data_points 
                ORDER BY name;
            """, con, index_col="id")

    expected_test_dps = ["loc0-dev0-pub-0", "loc0-dev0-pub-1", "loc1-dev1-pub-0", "loc0-dev0-pr-2"]

    assert data.index.size >= len(expected_test_dps)
    data_point_ids = [basic_dp_test_set[dp] for dp in expected_test_dps]
    data_points = data.loc[data_point_ids]  # Order and filter by expectation

    pd.testing.assert_frame_equal(data_points, pd.DataFrame({
        "name": ["name_0", "name_1", "name_0", "name_2"],
        "device_id": ["device_0", "device_0", "device_1", "device_0"],
        "location_code": ["location_0", "location_0", "location_1", "location_0"],
        "data_provider": ["provider_0", "provider_0", "provider_0", "provider_0"],
        "view_role": ["view_public", "view_public", "view_public", "view_internal"]
    }, index=data_point_ids), check_names=False)


def test_invalid_data_type(clean_db, sql_engine_data_source):
    """Tests whether an error is raised on invalid data type specs"""

    with pytest.raises(sqlalchemy.exc.DataError, match=".*invalid input value for enum.*"):
        with sql_engine_data_source.begin() as con:
            con.execute(sql.text("""
                INSERT INTO data_points(name, device_id, location_code, data_provider, view_role, data_type) VALUES
                    ('name_0', 'device_0', 'nowhere special', 'crystal ball', 'view_internal', 'qbit');
            """))


def test_invalid_temporality(clean_db, sql_engine_data_source):
    """Tests whether an error is raised on invalid data temporality specs"""

    with pytest.raises(sqlalchemy.exc.DataError, match=".*invalid input value for enum.*"):
        with sql_engine_data_source.begin() as con:
            con.execute(sql.text("""
                INSERT INTO data_points(name, device_id, location_code, data_provider, view_role, temporality) VALUES
                    ('name_0', 'device_0', 'nowhere special', 'crystal ball', 'view_internal', 'qbit');
            """))
