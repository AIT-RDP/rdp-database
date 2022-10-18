"""Add forcast horizon function

Revision ID: b12e079058dd
Revises: 570f0d049840
Create Date: 2022-10-18 14:23:17.863659

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'b12e079058dd'
down_revision = '570f0d049840'
branch_labels = None
depends_on = None


def upgrade():
    """Creates a function that returns the latest known prediction for a given forecast horizon"""

    op.execute("""
        CREATE OR REPLACE FUNCTION forecasts_horizon(
            horizon INTERVAL
        ) RETURNS TABLE(
            dp_id INTEGER, 
            obs_time TIMESTAMPTZ, 
            fc_time TIMESTAMPTZ, 
            value DOUBLE PRECISION, 
            name VARCHAR(128), 
            device_id VARCHAR(128), 
            location_code VARCHAR(128), 
            data_provider VARCHAR(128), 
            unit TEXT
        ) AS $$
            SELECT dp.id, obs_time, fc_time, value, dp.name, dp.device_id, dp.location_code, dp.data_provider, dp.unit
                FROM forecasts AS fc_full
                JOIN data_points AS dp
                        ON (fc_full.dp_id = dp.id)
                WHERE fc_full.fc_time = (
                        SELECT max(fc_time) FROM forecasts AS fc_red
                                WHERE fc_red.dp_id = fc_full.dp_id AND
                                        fc_red.obs_time = fc_full.obs_time AND
                                        (fc_red.obs_time - fc_red.fc_time) >= forecasts_horizon.horizon
                )
        $$ LANGUAGE SQL;
    """)


def downgrade():
    """Reverts the changes"""
    op.execute("DROP FUNCTION get_or_create_data_point_id;")
