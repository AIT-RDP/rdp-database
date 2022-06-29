"""Initial DB Setup

Revision ID: 49bc8370e2fd
Revises: 
Create Date: 2022-06-28 14:53:16.574769

"""
import os
import re

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '49bc8370e2fd'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    """Creates the database stub and activates Timescale DB"""
    op.execute("""
        CREATE TABLE data_points (
            id SERIAL NOT NULL,
            name VARCHAR(128) NOT NULL,
            device_id VARCHAR(128) DEFAULT NULL,
            location_code VARCHAR(128) NOT NULL,
            data_provider VARCHAR(128) NOT NULL,
            unit TEXT DEFAULT NULL,
            PRIMARY KEY (id)
        );
        
        COMMENT ON TABLE data_points IS 'Meta-information of each single time-series (forecast and measurements)';
        COMMENT ON COLUMN data_points.id IS 'Unique data point id for further reference';
        COMMENT ON COLUMN data_points.name IS 'The identifying name of each observation';
        COMMENT ON COLUMN data_points.device_id IS 'Optional identifier of the measurement device';
        COMMENT ON COLUMN data_points.location_code IS 'The location name of the associated observations';
        COMMENT ON COLUMN data_points.data_provider IS 'Service that provides the information';
        COMMENT ON COLUMN data_points.unit IS 'AN optional unit description'; 
    """)

    op.execute("""
        CREATE TABLE forecasts (
            dp_id INTEGER NOT NULL,
            obs_time TIMESTAMPTZ NOT NULL, -- Real-time instant of the forecasted observation
            fc_time TIMESTAMPTZ NOT NULL,  -- Time the forecast was made or fetched
            value DOUBLE PRECISION,
            PRIMARY KEY (dp_id, obs_time, fc_time),
            FOREIGN KEY (dp_id) REFERENCES data_points(id)
        );
        COMMENT ON TABLE forecasts IS 'Forecasted information that has a separated forecasting and observation time';
        COMMENT ON COLUMN forecasts.dp_id IS 'The reference to the corresponding data point description';
        COMMENT ON COLUMN forecasts.obs_time IS 'Time of the forecasted observation';
        COMMENT ON COLUMN forecasts.fc_time IS 'Time the forecast was computed or fetched';
        COMMENT ON COLUMN forecasts.value IS 'The forecasted value';
    """)

    op.execute("""
        CREATE TABLE measurements (
            dp_id INTEGER NOT NULL,
            obs_time TIMESTAMPTZ NOT NULL,
            value DOUBLE PRECISION,
            PRIMARY KEY (dp_id, obs_time),
            FOREIGN KEY (dp_id) REFERENCES data_points(id)
        );
        COMMENT ON TABLE measurements IS 'Measurement time series that can be identified by a static data point';
        COMMENT ON COLUMN measurements.dp_id IS 'The reference to the corresponding data point description';
        COMMENT ON COLUMN measurements.obs_time IS 'The real-time instant of the observation';
        COMMENT ON COLUMN measurements.value IS 'The recorded value';
    """)

def downgrade():
    """Removes the entire database"""
    op.execute("DROP TABLE measurements")
    op.execute("DROP TABLE forecasts")
    op.execute("DROP TABLE data_points")