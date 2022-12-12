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
    upgrade_security_model()
    upgrade_views()
    upgrade_fc_functions()


def upgrade_security_model():
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

    # More information: https://www.cybertec-postgresql.com/en/postgresql-row-level-security-views-and-a-lot-of-magic/
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


def upgrade_views():
    """Upgrade the views to reflect the security column"""

    op.execute("""
        CREATE OR REPLACE VIEW forecasts_latest(
            dp_id, obs_time, fc_time, value, name, device_id, location_code, data_provider, unit, view_role
        ) AS
            SELECT dp.id, obs_time, fc_time, value, dp.name, dp.device_id, dp.location_code, dp.data_provider, dp.unit,
                    dp.view_role
                FROM forecasts AS fc_full
                JOIN data_points AS dp ON (fc_full.dp_id = dp.id)
                WHERE fc_full.fc_time = (
                        SELECT max(fc_time)
                        FROM forecasts AS fc_red
                        WHERE fc_red.dp_id = fc_full.dp_id AND fc_red.obs_time = fc_full.obs_time
                    );
        ALTER VIEW forecasts_latest OWNER TO restricting_view_executor;
    """)

    op.execute("""
        CREATE OR REPLACE VIEW forecasts_details(
            dp_id, obs_time, fc_time, value, name, device_id, location_code, data_provider, unit, view_role
        ) AS
            SELECT dp.id, obs_time, fc_time, value, dp.name, dp.device_id, dp.location_code, dp.data_provider, dp.unit, 
                    dp.view_role
                FROM forecasts AS fc_full
                JOIN data_points AS dp
                        ON (fc_full.dp_id = dp.id)
                ;
        ALTER VIEW measurements_details OWNER TO restricting_view_executor;
    """)

    op.execute("""
        CREATE OR REPLACE VIEW measurements_details(
            dp_id, obs_time, value, name, device_id, location_code, data_provider, unit, view_role
        ) AS
            SELECT dp.id, obs_time, value, dp.name, dp.device_id, dp.location_code, dp.data_provider, dp.unit, 
                    dp.view_role
                FROM measurements AS mea
                JOIN data_points AS dp
                        ON (mea.dp_id = dp.id)
                ;
        ALTER VIEW forecasts_details OWNER TO restricting_view_executor;
    """)


def upgrade_fc_functions():
    """Introduces a forecasting function that filters results and considers the horizon"""

    # Create direct access views to avoid handling the auxiliary information
    op.execute("""
        CREATE OR REPLACE VIEW measurements_samples(
            dp_id, obs_time, value
        ) AS
            SELECT dp_id, obs_time, value
                FROM measurements
                WHERE dp_id in (SELECT id FROM data_points)
                ;
        ALTER VIEW measurements_samples OWNER TO restricting_view_executor;
        GRANT SELECT, TRIGGER ON TABLE measurements_samples TO view_base;
    """)

    op.execute("""
        CREATE OR REPLACE VIEW forecasts_samples(
            dp_id, obs_time, fc_time, value
        ) AS
            SELECT dp_id, obs_time, fc_time, value
                FROM forecasts
                WHERE dp_id in (SELECT id FROM data_points)
                ;
        ALTER VIEW forecasts_samples OWNER TO restricting_view_executor;
        GRANT SELECT, TRIGGER ON TABLE forecasts_samples TO view_base;
    """)

    op.execute("""
        CREATE OR REPLACE FUNCTION forecasts_horizon(
            fc_horizon INTERVAL,
            fc_series_begin TIMESTAMPTZ,
            fc_series_end TIMESTAMPTZ,
            fc_name VARCHAR(128),
            fc_location_code VARCHAR(128),
            fc_data_provider VARCHAR(128),
            fc_device_id VARCHAR(128) DEFAULT NULL,
            regexp BOOL DEFAULT FALSE
        ) RETURNS TABLE(
            dp_id INTEGER, 
            obs_time TIMESTAMPTZ, 
            fc_time TIMESTAMPTZ, 
            value DOUBLE PRECISION, 
            name VARCHAR(128), 
            device_id VARCHAR(128), 
            location_code VARCHAR(128), 
            data_provider VARCHAR(128), 
            unit TEXT,
            view_role TEXT
        )
        STABLE
        SECURITY INVOKER 
        PARALLEL SAFE 
        AS $$
        DECLARE
        BEGIN
            IF regexp THEN
                RETURN QUERY SELECT fc_full.dp_id, fc_full.obs_time, fc_full.fc_time, fc_full.value, fc_full.name,  
                        fc_full.device_id, fc_full.location_code, fc_full.data_provider, fc_full.unit, fc_full.view_role
                    FROM forecasts_details AS fc_full
                    WHERE fc_full.fc_time = (
                            SELECT max(fc_red.fc_time) FROM forecasts_samples AS fc_red
                                WHERE fc_red.dp_id = fc_full.dp_id AND
                                    fc_red.obs_time = fc_full.obs_time AND
                                    (fc_red.obs_time - fc_red.fc_time) >= forecasts_horizon.fc_horizon
                        ) AND
                        fc_full.name ~ forecasts_horizon.fc_name AND
                        fc_full.location_code ~ forecasts_horizon.fc_location_code AND
                        fc_full.data_provider ~ forecasts_horizon.fc_data_provider AND
                        ((fc_full.device_id IS NULL AND forecasts_horizon.fc_device_id IS NULL) OR 
                            (fc_full.device_id ~ forecasts_horizon.fc_device_id)) AND
                        fc_full.obs_time BETWEEN forecasts_horizon.fc_series_begin AND forecasts_horizon.fc_series_end; 
            ELSE
                RETURN QUERY SELECT fc_full.dp_id, fc_full.obs_time, fc_full.fc_time, fc_full.value, fc_full.name,  
                        fc_full.device_id, fc_full.location_code, fc_full.data_provider, fc_full.unit, fc_full.view_role
                    FROM forecasts_details AS fc_full
                    WHERE fc_full.fc_time = (
                            SELECT max(fc_red.fc_time) FROM forecasts_samples AS fc_red
                                WHERE fc_red.dp_id = fc_full.dp_id AND
                                    fc_red.obs_time = fc_full.obs_time AND
                                    (fc_red.obs_time - fc_red.fc_time) >= forecasts_horizon.fc_horizon
                        ) AND
                        fc_full.name = forecasts_horizon.fc_name AND
                        fc_full.location_code = forecasts_horizon.fc_location_code AND
                        fc_full.data_provider = forecasts_horizon.fc_data_provider AND
                        fc_full.device_id IS NOT DISTINCT FROM forecasts_horizon.fc_device_id AND
                        fc_full.obs_time BETWEEN forecasts_horizon.fc_series_begin AND forecasts_horizon.fc_series_end;
            END IF;
        END;          
        $$ LANGUAGE plpgsql;
        
        GRANT EXECUTE ON FUNCTION forecasts_horizon(
                INTERVAL, TIMESTAMPTZ, TIMESTAMPTZ, VARCHAR(128), VARCHAR(128), VARCHAR(128), VARCHAR(128), BOOL
            ) TO view_base;
    """)


def downgrade():
    downgrade_fc_functions()
    downgrade_views()
    downgrade_security_model()


def downgrade_fc_functions():
    """Removes the additional forecasting functions again"""
    op.execute("""
        DROP VIEW IF EXISTS measurements_samples;
        DROP VIEW IF EXISTS forecasts_samples;
        DROP FUNCTION IF EXISTS forecasts_horizon(
                INTERVAL, TIMESTAMPTZ, TIMESTAMPTZ, VARCHAR(128), VARCHAR(128), VARCHAR(128), VARCHAR(128), BOOL
            );
    """)


def downgrade_views():
    """Upgrade the views to reflect the security column"""

    data_vis_user = os.environ['POSTGRES_DATA_VIS_USER']

    op.execute(f"""
        DROP VIEW forecasts_latest; -- cannot drop columns from view
        CREATE OR REPLACE VIEW forecasts_latest(
            dp_id, obs_time, fc_time, value, name, device_id, location_code, data_provider, unit
        ) AS
            SELECT dp.id, obs_time, fc_time, value, dp.name, dp.device_id, dp.location_code, dp.data_provider, dp.unit
                FROM forecasts AS fc_full
                JOIN data_points AS dp
                        ON (fc_full.dp_id = dp.id)
                WHERE fc_full.fc_time = (
                        SELECT max(fc_time) FROM forecasts AS fc_red
                                WHERE fc_red.dp_id = fc_full.dp_id AND
                                        fc_red.obs_time = fc_full.obs_time
                );
        ALTER VIEW forecasts_latest OWNER TO current_user;
        GRANT SELECT, TRIGGER ON TABLE forecasts_latest TO "{data_vis_user}";
    """)

    op.execute(f"""
        DROP VIEW forecasts_details; -- cannot drop columns from view
        CREATE OR REPLACE VIEW forecasts_details(
            dp_id, obs_time, fc_time, value, name, device_id, location_code, data_provider, unit
        ) AS
            SELECT dp.id, obs_time, fc_time, value, dp.name, dp.device_id, dp.location_code, dp.data_provider, dp.unit
                FROM forecasts AS fc_full
                JOIN data_points AS dp
                        ON (fc_full.dp_id = dp.id)
                ;
        ALTER VIEW forecasts_details OWNER TO current_user;
        GRANT SELECT, TRIGGER ON TABLE forecasts_details TO "{data_vis_user}";
    """)

    op.execute(f"""
        DROP VIEW measurements_details; -- cannot drop columns from view
        CREATE OR REPLACE VIEW measurements_details(
            dp_id, obs_time, value, name, device_id, location_code, data_provider, unit
        ) AS
            SELECT dp.id, obs_time, value, dp.name, dp.device_id, dp.location_code, dp.data_provider, dp.unit
                FROM measurements AS mea
                JOIN data_points AS dp
                        ON (mea.dp_id = dp.id)
                ;
        ALTER VIEW measurements_details OWNER TO current_user;
        GRANT SELECT, TRIGGER ON TABLE measurements_details TO "{data_vis_user}";
    """)


def downgrade_security_model():
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
               
        REVOKE ALL ON TABLE forecasts, measurements FROM restricting_view_executor;
        DROP ROLE restricting_view_executor;
        
        REVOKE ALL ON TABLE data_points FROM view_base;
        REVOKE ALL ON SEQUENCE data_points_id_seq FROM view_base;
        REVOKE ALL ON TABLE forecasts_latest, forecasts_details, measurements_details FROM view_base;
        DROP ROLE view_base;
        
        DROP ROLE "{data_pub_vis_user}";
    """)
