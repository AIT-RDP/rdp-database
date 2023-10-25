"""Add metadata to data_points

Revision ID: 3a7a5c75ae76
Revises: 62ffa7f9c9a4
Create Date: 2023-10-24 13:20:09.817526

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '3a7a5c75ae76'
down_revision = '62ffa7f9c9a4'
branch_labels = None
depends_on = None


def upgrade():
    _upgrade_tables()
    _upgrade_views()

def downgrade():
    _downgrade_views()
    _downgrade_tables()



def _upgrade_tables():
    """Alter data_points table adding metadata"""
    op.execute("""
        ALTER TABLE data_points 
        ADD COLUMN metadata jsonb not null default '{}'::jsonb;

        COMMENT ON COLUMN data_points.metadata IS 'Additional data points metadata';
    """)

def _upgrade_views():
    """Upgrade views adding metadata column"""

    # Fix measurements_details view adding metadata column 
    op.execute("""
        CREATE OR REPLACE VIEW measurements_details(
            dp_id, obs_time, value, name, device_id, location_code, data_provider, unit, view_role, metadata
        ) AS
            SELECT dp.id, obs_time, value, dp.name, dp.device_id, dp.location_code, dp.data_provider, dp.unit, 
                    dp.view_role, dp.metadata
                FROM measurements AS mea
                JOIN data_points AS dp
                        ON (mea.dp_id = dp.id)
                ;
    """)

    # Fix forecasts_details view adding metadata column 
    op.execute("""
        CREATE OR REPLACE VIEW forecasts_details(
            dp_id, obs_time, fc_time, value, name, device_id, location_code, data_provider, unit, view_role, metadata
        ) AS
            SELECT dp.id, obs_time, fc_time, value, dp.name, dp.device_id, dp.location_code, dp.data_provider, dp.unit, 
                    dp.view_role, dp.metadata
                FROM forecasts AS fc_full
                JOIN data_points AS dp
                        ON (fc_full.dp_id = dp.id)
                ;
    """)

    # Fix forecasts_latest view adding metadata column 
    op.execute("""
        CREATE OR REPLACE VIEW forecasts_latest(
            dp_id, obs_time, fc_time, value, name, device_id, location_code, data_provider, unit, view_role, metadata
        ) AS
            SELECT dp.id, obs_time, fc_time, value, dp.name, dp.device_id, dp.location_code, dp.data_provider, dp.unit,
                    dp.view_role, dp.metadata
                FROM forecasts AS fc_full
                JOIN data_points AS dp ON (fc_full.dp_id = dp.id)
                WHERE fc_full.fc_time = (
                        SELECT max(fc_time)
                        FROM forecasts AS fc_red
                        WHERE fc_red.dp_id = fc_full.dp_id AND fc_red.obs_time = fc_full.obs_time
                    );
    """)

def _downgrade_tables():
    """Alter data_points table removing metadata"""

    op.execute("""
        ALTER TABLE data_points
        DROP COLUMN metadata;
    """)

def _downgrade_views():
    """Downgrade views removing metadata column"""

    # Fix measurements_details view removing metadata column 
    op.execute("""
        DROP VIEW measurements_details; -- cannot drop columns from view
        CREATE OR REPLACE VIEW measurements_details(
            dp_id, obs_time, value, name, device_id, location_code, data_provider, unit, view_role
        ) AS
            SELECT dp.id, obs_time, value, dp.name, dp.device_id, dp.location_code, dp.data_provider, dp.unit, 
                    dp.view_role
                FROM measurements AS mea
                JOIN data_points AS dp
                        ON (mea.dp_id = dp.id)
                ;
    """)

    # Fix forecasts_details view removing metadata column 
    op.execute("""
        DROP VIEW forecasts_details; -- cannot drop columns from view
        CREATE OR REPLACE VIEW forecasts_details(
            dp_id, obs_time, fc_time, value, name, device_id, location_code, data_provider, unit, view_role
        ) AS
            SELECT dp.id, obs_time, fc_time, value, dp.name, dp.device_id, dp.location_code, dp.data_provider, dp.unit, 
                    dp.view_role
                FROM forecasts AS fc_full
                JOIN data_points AS dp
                        ON (fc_full.dp_id = dp.id)
                ;
    """)


    # Fix forecasts_latest view removing metadata column 
    op.execute("""
        DROP VIEW forecasts_latest; -- cannot drop columns from view
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
    """)