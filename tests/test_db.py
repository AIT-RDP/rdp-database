"""
Runs some basic tests on the DB connection and DB version
"""

import os
import re
import warnings

import sqlalchemy.sql as sql


def test_db_version(sql_engine_postgres):
    """Test whether the DB connection is succesful and whether the DB version is as expected"""

    with sql_engine_postgres.connect() as con:
        res = con.execute(sql.text("SHOW server_version"))
        res = res.mappings().fetchall()

        assert len(res) == 1, "No results returned by the query"
        assert "server_version" in res[0], "No version returned"
        server_version = res[0]["server_version"]

        assert "RDP_EXPECTED_POSTGRES_VERSION" in os.environ, "Please add the variable RDP_EXPECTED_POSTGRES_VERSION"
        version_pattern = os.environ["RDP_EXPECTED_POSTGRES_VERSION"]
        assert re.search(version_pattern, server_version), \
            f"Unexpected server version '{server_version}', expected '{version_pattern}'"
