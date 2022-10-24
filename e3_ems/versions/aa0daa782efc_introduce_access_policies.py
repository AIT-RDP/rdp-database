"""Introduce access policies

Revision ID: aa0daa782efc
Revises: b12e079058dd
Create Date: 2022-10-20 09:54:31.889883

"""
import os

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'aa0daa782efc'
down_revision = 'b12e079058dd'
branch_labels = None
depends_on = None


def upgrade():
    """Extends the data point table and adds access policies"""

    data_vis_user = os.environ['POSTGRES_DATA_VIS_USER']
    data_source_user = os.environ['POSTGRES_DATA_SOURCE_USER']
    data_pub_vis_user = os.environ['POSTGRES_DATA_PUB_VIS_USER']

    op.execute(f"""
        CREATE ROLE "{data_pub_vis_user}" 
            NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT
            LOGIN PASSWORD '{os.environ['POSTGRES_DATA_PUB_VIS_PASSWORD']}';
        COMMENT ON ROLE "{data_pub_vis_user}" IS 'Restricted user for public data';
    """)

    op.execute("""
        CREATE ROLE view_base NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT NOLOGIN NOREPLICATION NOBYPASSRLS;
        COMMENT ON ROLE view_base IS 'Base role to define data view permissions';
        
        GRANT SELECT ON TABLE data_points TO view_base;  -- no select on the raw tables to avoid costly checks there
        GRANT SELECT ON SEQUENCE data_points_id_seq TO view_base;
        GRANT SELECT, TRIGGER ON TABLE forecasts_latest, forecasts_details, measurements_details TO view_base;
    """)

    op.execute("""
        CREATE ROLE restricting_view_executor 
            NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT NOLOGIN NOREPLICATION NOBYPASSRLS
            IN ROLE view_base;
        COMMENT ON ROLE restricting_view_executor IS 'Dedicated owner (executor) of protected views to avoid leaks';
        
        GRANT SELECT ON TABLE measurements, forecasts TO restricting_view_executor; -- Access to raw data needed
        ALTER VIEW measurements_details OWNER TO restricting_view_executor;
        ALTER VIEW forecasts_details OWNER TO restricting_view_executor;
        ALTER VIEW forecasts_latest OWNER TO restricting_view_executor;
    """)

    op.execute(f"""        
        CREATE ROLE view_internal NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT NOLOGIN NOREPLICATION NOBYPASSRLS
            IN ROLE view_base
            ROLE "{data_vis_user}", "{data_source_user}";
        COMMENT ON ROLE view_internal IS 'Allows to view internal data that is not meant for public access';
    """)

    op.execute(f"""        
        CREATE ROLE view_public NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT NOLOGIN NOREPLICATION NOBYPASSRLS
            IN ROLE view_base
            ROLE "{data_vis_user}", "{data_source_user}", "{data_pub_vis_user}";
        COMMENT ON ROLE view_public IS 'Allows to view public data that is explicitly meant for public access';
    """)

    op.execute(f"""        
        CREATE ROLE data_source_base NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT NOLOGIN NOREPLICATION NOBYPASSRLS
            ROLE "{data_source_user}";
        COMMENT ON ROLE data_source_base IS 'Allow to add new data including new data points';
        
        GRANT SELECT, INSERT, UPDATE ON TABLE data_points TO data_source_base;
        GRANT USAGE, SELECT, UPDATE ON SEQUENCE data_points_id_seq TO data_source_base;
        GRANT INSERT, SELECT, UPDATE ON TABLE forecasts, measurements TO data_source_base;
        GRANT EXECUTE ON FUNCTION public.get_or_create_data_point_id(varchar, varchar, varchar, varchar) 
            TO data_source_base;
    """)

    op.execute(f"""
        ALTER TABLE data_points ADD view_role text NOT NULL DEFAULT 'view_internal';
        COMMENT ON COLUMN data_points.view_role IS 'Role that is allowed to view the associated data';
        
        ALTER TABLE data_points ENABLE ROW LEVEL SECURITY;
        
        CREATE POLICY pl_insert ON data_points AS PERMISSIVE FOR ALL TO data_source_base USING(true);
        CREATE POLICY pl_view_role ON data_points AS PERMISSIVE FOR SELECT TO view_base 
            USING (pg_has_role(current_user, view_role, 'MEMBER'));
    """)

    op.execute(f"""
        -- INHERIT needed to trigger the correct policies
        ALTER ROLE "{data_source_user}" INHERIT;
        ALTER ROLE "{data_vis_user}" INHERIT;
    """)


def downgrade():
    """Removes the access policies and reverts the data point table again"""

    data_pub_vis_user = os.environ['POSTGRES_DATA_PUB_VIS_USER']
    data_vis_user = os.environ['POSTGRES_DATA_VIS_USER']
    data_source_user = os.environ['POSTGRES_DATA_SOURCE_USER']

    op.execute(f"""
        ALTER ROLE "{data_source_user}" NOINHERIT;
        ALTER ROLE "{data_vis_user}" NOINHERIT;
    """)

    op.execute("""
        DROP POLICY pl_view_role ON data_points;
        DROP POLICY pl_insert ON data_points;
        ALTER TABLE data_points DISABLE ROW LEVEL SECURITY;
        ALTER TABLE data_points DROP COLUMN view_role;
    """)

    op.execute("""
        REVOKE ALL ON TABLE data_points FROM data_source_base;
        REVOKE ALL ON SEQUENCE data_points_id_seq FROM data_source_base;
        REVOKE ALL ON TABLE forecasts, measurements FROM data_source_base;
        REVOKE ALL ON FUNCTION public.get_or_create_data_point_id(varchar, varchar, varchar, varchar)
            FROM data_source_base;

        DROP ROLE data_source_base;
    """)

    op.execute(f"""
        DROP ROLE view_public;
        DROP ROLE view_internal;
        
        ALTER VIEW measurements_details OWNER TO current_user;
        ALTER VIEW forecasts_details OWNER TO current_user;
        ALTER VIEW forecasts_latest OWNER TO current_user;
        
        REVOKE ALL ON TABLE forecasts, measurements FROM restricting_view_executor;
        DROP ROLE restricting_view_executor;
        
        REVOKE ALL ON TABLE data_points FROM view_base;
        REVOKE ALL ON SEQUENCE data_points_id_seq FROM view_base;
        REVOKE ALL ON TABLE forecasts_latest, forecasts_details, measurements_details FROM view_base;
        DROP ROLE view_base;
        
        DROP ROLE "{data_pub_vis_user}";
    """)
