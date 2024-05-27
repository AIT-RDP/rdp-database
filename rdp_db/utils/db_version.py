"""
Implements some utilities that manage the version of the target database
"""

import sqlalchemy as sql

_detected_version = None


def init_version(connection: sql.Connection) -> None:
    """
    Initializes the version module by querying the current database version
    :param connection: The connection to the active database
    """

    resp = connection.execute(sql.text("""
        SELECT setting as version FROM pg_settings WHERE name = 'server_version_num';
    """))
    result = resp.fetchone()

    global _detected_version
    _detected_version = int(result.version)


def get_version_int() -> int:
    """
    Returns the current database version as integer of the form <major>*10000 + <minor>

    The module must be properly initialized beforehand.
    """

    if _detected_version is None:
        raise ValueError("The version has not been properly initialized")

    return _detected_version