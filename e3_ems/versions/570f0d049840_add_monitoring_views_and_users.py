"""Add monitoring views and users

Revision ID: 570f0d049840
Revises: d2baa52b21c2
Create Date: 2022-08-08 15:01:39.421127

"""
import os

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '570f0d049840'
down_revision = 'd2baa52b21c2'
branch_labels = None
depends_on = None


def upgrade():
    _upgrade_views()
    _upgrade_basic_monitoring_users()


def _upgrade_views():
    """Creates the views showing measurement and forecasting details"""

    op.execute("""
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
        
        COMMENT ON VIEW forecasts_latest IS 'Shows the latest available predictions for each observation';
    """)

    op.execute("""
        CREATE OR REPLACE VIEW forecasts_details(
            dp_id, obs_time, fc_time, value, name, device_id, location_code, data_provider, unit
        ) AS
            SELECT dp.id, obs_time, fc_time, value, dp.name, dp.device_id, dp.location_code, dp.data_provider, dp.unit
                FROM forecasts AS fc_full
                JOIN data_points AS dp
                        ON (fc_full.dp_id = dp.id)
                ;
        
        COMMENT ON VIEW forecasts_details IS 'Shows detailed information on on both recent and outdated forecasts';
    """)

    op.execute("""
        CREATE OR REPLACE VIEW measurements_details(
            dp_id, obs_time, value, name, device_id, location_code, data_provider, unit
        ) AS
            SELECT dp.id, obs_time, value, dp.name, dp.device_id, dp.location_code, dp.data_provider, dp.unit
                FROM measurements AS mea
                JOIN data_points AS dp
                        ON (mea.dp_id = dp.id)
                ;

        COMMENT ON VIEW measurements_details IS 'Shows detailed information on each measurement';
    """)


def _upgrade_basic_monitoring_users():
    """Creates the basic monitoring users"""

    data_source_user = os.environ['POSTGRES_DATA_SOURCE_USER']
    op.execute(f"""
        CREATE ROLE {data_source_user} 
            NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT 
            LOGIN PASSWORD '{os.environ['POSTGRES_DATA_SOURCE_PASSWORD']}';

        GRANT SELECT, INSERT, UPDATE ON TABLE data_points TO {data_source_user};
        GRANT USAGE, SELECT, UPDATE ON SEQUENCE data_points_id_seq TO {data_source_user};
        GRANT INSERT, SELECT, UPDATE ON TABLE forecasts, measurements TO {data_source_user};
        GRANT EXECUTE ON FUNCTION public.get_or_create_data_point_id(varchar, varchar, varchar, varchar) 
            TO {data_source_user};

    """)

    data_vis_user = os.environ['POSTGRES_DATA_VIS_USER']
    op.execute(f"""
        CREATE ROLE {data_vis_user} 
            NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT 
            LOGIN PASSWORD '{os.environ['POSTGRES_DATA_VIS_PASSWORD']}';

        GRANT SELECT ON TABLE data_points, forecasts, measurements TO {data_vis_user};
        GRANT SELECT ON SEQUENCE data_points_id_seq TO {data_vis_user};
        GRANT SELECT, TRIGGER ON TABLE forecasts_latest, forecasts_details, measurements_details TO {data_vis_user};
    """)


def downgrade():
    data_source_user = os.environ['POSTGRES_DATA_SOURCE_USER']
    op.execute(f"""
        REVOKE ALL ON TABLE data_points, forecasts, measurements FROM {data_source_user};
        REVOKE ALL ON SEQUENCE data_points_id_seq FROM {data_source_user};
        REVOKE ALL ON FUNCTION public.get_or_create_data_point_id(varchar, varchar, varchar, varchar) 
            FROM {data_source_user};
        
        DROP USER {data_source_user};
    """)

    data_vis_user = os.environ['POSTGRES_DATA_VIS_USER']
    op.execute(f"""
        REVOKE ALL ON TABLE data_points, forecasts, measurements FROM {data_vis_user};
        REVOKE ALL ON SEQUENCE data_points_id_seq FROM {data_vis_user};
        REVOKE ALL ON TABLE forecasts_latest, forecasts_details, measurements_details FROM {data_vis_user};

        DROP USER {data_vis_user};
    """)

    op.execute("DROP VIEW measurements_details;")
    op.execute("DROP VIEW forecasts_details;")
    op.execute("DROP VIEW forecasts_latest;")
