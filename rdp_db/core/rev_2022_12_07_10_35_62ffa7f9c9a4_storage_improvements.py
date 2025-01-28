"""Storage improvements

Revision ID: 62ffa7f9c9a4
Revises: aa0daa782efc
Create Date: 2022-12-07 10:35:36.588583

"""
from alembic import op
import sqlalchemy as sa

import rdp_db.core.rev_2022_10_20_09_54_aa0daa782efc_introduce_access_policies as access_policy_def

# revision identifiers, used by Alembic.
revision = '62ffa7f9c9a4'
down_revision = 'aa0daa782efc'
branch_labels = None
depends_on = None


def upgrade():
    """Removes redundant indices and enforces some improvements"""
    upgrade_hypertable_organization()
    upgrade_forecasts_horizon()


def upgrade_hypertable_organization():
    """Upgrades the hypertable storage and compression parameters"""

    op.execute(sa.text("""
        DROP INDEX forecasts_obs_time_idx;
        DROP INDEX measurements_obs_time_idx;
        DROP INDEX idx_forecasts_id_obs_time;
        DROP INDEX idx_measurements_id_obs_time;
    """))

    op.execute(sa.text("""
        ALTER TABLE measurements SET (
            timescaledb.compress=true,
            timescaledb.compress_segmentby='dp_id'
        );
        SELECT set_chunk_time_interval('measurements', INTERVAL '5 days');
        SELECT add_compression_policy('measurements', INTERVAL '2 days');
    """))

    op.execute(sa.text("""
        ALTER TABLE forecasts SET (
            timescaledb.compress=true,
            timescaledb.compress_segmentby='dp_id',
            timescaledb.compress_orderby='obs_time DESC, fc_time DESC'
        );
        SELECT set_chunk_time_interval('forecasts', INTERVAL '24 hours');
        SELECT add_compression_policy('forecasts', INTERVAL '2 days');
    """))


def upgrade_forecasts_horizon():
    """Alters the forecasts' horizon function to speed up the execution"""

    op.execute(sa.text("""
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
                RETURN QUERY SELECT fc_full.dp_id, fc_full.obs_time, last(fc_full.fc_time, fc_full.fc_time) AS fc_time, 
                        last(fc_full.value, fc_full.fc_time) AS value, fc_full.name, fc_full.device_id, 
                        fc_full.location_code, fc_full.data_provider, fc_full.unit, fc_full.view_role
                    FROM forecasts_details AS fc_full
                    WHERE (fc_full.obs_time - fc_full.fc_time) >= forecasts_horizon.fc_horizon AND
                        fc_full.name ~ forecasts_horizon.fc_name AND
                        fc_full.location_code ~ forecasts_horizon.fc_location_code AND
                        fc_full.data_provider ~ forecasts_horizon.fc_data_provider AND
                        ((fc_full.device_id IS NULL AND forecasts_horizon.fc_device_id IS NULL) OR 
                            (fc_full.device_id ~ forecasts_horizon.fc_device_id)) AND
                        fc_full.obs_time BETWEEN forecasts_horizon.fc_series_begin AND forecasts_horizon.fc_series_end
                    GROUP BY fc_full.dp_id, fc_full.obs_time, fc_full.name, fc_full.device_id, fc_full.location_code, 
                        fc_full.data_provider, fc_full.unit, fc_full.view_role;                
            ELSE
                RETURN QUERY SELECT fc_full.dp_id, fc_full.obs_time, last(fc_full.fc_time, fc_full.fc_time) AS fc_time, 
                        last(fc_full.value, fc_full.fc_time) AS value, fc_full.name, fc_full.device_id, 
                        fc_full.location_code, fc_full.data_provider, fc_full.unit, fc_full.view_role
                    FROM forecasts_details AS fc_full
                    WHERE (fc_full.obs_time - fc_full.fc_time) >= forecasts_horizon.fc_horizon AND
                        fc_full.name = forecasts_horizon.fc_name AND
                        fc_full.location_code = forecasts_horizon.fc_location_code AND
                        fc_full.data_provider = forecasts_horizon.fc_data_provider AND
                        fc_full.device_id IS NOT DISTINCT FROM forecasts_horizon.fc_device_id AND
                        fc_full.obs_time BETWEEN forecasts_horizon.fc_series_begin AND forecasts_horizon.fc_series_end
                    GROUP BY fc_full.dp_id, fc_full.obs_time, fc_full.name, fc_full.device_id, fc_full.location_code, 
                        fc_full.data_provider, fc_full.unit, fc_full.view_role;
            END IF;
        END;          
        $$ LANGUAGE plpgsql;

        GRANT EXECUTE ON FUNCTION forecasts_horizon(
                INTERVAL, TIMESTAMPTZ, TIMESTAMPTZ, VARCHAR(128), VARCHAR(128), VARCHAR(128), VARCHAR(128), BOOL
            ) TO view_base;
    """))


def downgrade():
    """Introduces the redundancies again"""

    access_policy_def.upgrade_fc_functions()
    downgrade_hypertable_organization()


def downgrade_hypertable_organization():
    """Downgrades the hypertable fc organization and decompresses the data"""
    op.execute(sa.text("""
        LOCK TABLE forecasts IN ACCESS EXCLUSIVE MODE;  -- Make sure the table is entirely ours to avoid deadlocks
        SELECT remove_compression_policy('forecasts', true); 
        SELECT set_chunk_time_interval('forecasts', INTERVAL '7 days');
        -- Decompressed all compressed chunks. If that command fails with a deadlock, consider to stop the feeder 
        -- processes like RedSQL
        SELECT decompress_chunk(format('%I.%I', chunk_schema, chunk_name)::regclass) 
            FROM chunk_compression_stats('forecasts')
            WHERE compression_status = 'Compressed';
        ALTER TABLE forecasts SET (timescaledb.compress=false);
    """))

    op.execute(sa.text("""
        LOCK TABLE measurements IN ACCESS EXCLUSIVE MODE;  -- Make sure the table is entirely ours to avoid deadlocks
        SELECT remove_compression_policy('measurements', true);
        SELECT set_chunk_time_interval('measurements', INTERVAL '7 days');
        -- Decompressed all compressed chunks. If that command fails with a deadlock, consider to stop the feeder 
        -- processes like RedSQL
        SELECT decompress_chunk(format('%I.%I', chunk_schema, chunk_name)::regclass) 
            FROM chunk_compression_stats('measurements')
            WHERE compression_status = 'Compressed';
        ALTER TABLE measurements SET (timescaledb.compress=false);
    """))

    op.execute(sa.text("""
        CREATE INDEX idx_measurements_id_obs_time ON measurements(dp_id, obs_time);
        CREATE INDEX idx_forecasts_id_obs_time ON forecasts(dp_id, obs_time);
        CREATE INDEX measurements_obs_time_idx ON public.measurements USING btree (obs_time DESC);
        CREATE INDEX forecasts_obs_time_idx ON public.forecasts USING btree (obs_time DESC);
    """))
