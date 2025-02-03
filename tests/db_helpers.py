"""
Implements some test helper functions that can be used in multiple modules
"""

import sqlalchemy as sql


def create_dp(eng: sql.Engine, name, device_id, location_code, data_provider, unit=None, metadata=None,
              view_role=None, data_type="double", temporality=None):
    """Creates (or returns) the datapoint and returns its number"""

    if metadata is None:
        metadata = {}
    if view_role is None:
        view_role = "view_internal"

    with eng.begin() as con:
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
        con.execute(sql.text("""
            UPDATE data_points 
                SET view_role=:view_role, data_type=:data_type, temporality=:temporality 
                WHERE id=:dp_id;
        """), parameters=dict(view_role=view_role, data_type=data_type, temporality=temporality, dp_id=dp_id))

    return dp_id
