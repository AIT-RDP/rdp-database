"""Storage improvements

Revision ID: 62ffa7f9c9a4
Revises: aa0daa782efc
Create Date: 2022-12-07 10:35:36.588583

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '62ffa7f9c9a4'
down_revision = 'aa0daa782efc'
branch_labels = None
depends_on = None


def upgrade():
    """Removes redundant indices and enforces some improvements"""
    op.execute("""
        DROP INDEX forecasts_obs_time_idx;
        DROP INDEX measurements_obs_time_idx;
        DROP INDEX idx_forecasts_id_obs_time;
        DROP INDEX idx_measurements_id_obs_time;
    """)

    op.execute("""
        ALTER TABLE measurements SET (
            timescaledb.compress=true,
            timescaledb.compress_segmentby='dp_id'
        );
        SELECT add_compression_policy('measurements', INTERVAL '2 days');
    """)

    op.execute("""
        ALTER TABLE forecasts SET (
            timescaledb.compress=true,
            timescaledb.compress_segmentby='dp_id',
            timescaledb.compress_orderby='obs_time DESC, fc_time DESC'
        );
        SELECT add_compression_policy('forecasts', INTERVAL '2 days');
    """)

def downgrade():
    """Introduces the redundancies again"""

    op.execute("""
        SELECT remove_compression_policy('forecasts', true);
        -- Decompressed all compressed chunks. If that command fails with a deadlock, consider to stop the feeder 
        -- processes like RedSQL
        SELECT decompress_chunk(format('%I.%I', chunk_schema, chunk_name)::regclass) 
            FROM chunk_compression_stats('forecasts')
            WHERE compression_status = 'Compressed';
        ALTER TABLE forecasts SET (timescaledb.compress=false);
    """)

    op.execute("""
        SELECT remove_compression_policy('measurements', true);
        -- Decompressed all compressed chunks. If that command fails with a deadlock, consider to stop the feeder 
        -- processes like RedSQL
        SELECT decompress_chunk(format('%I.%I', chunk_schema, chunk_name)::regclass) 
            FROM chunk_compression_stats('measurements')
            WHERE compression_status = 'Compressed';
        ALTER TABLE measurements SET (timescaledb.compress=false);
    """)

    op.execute("""
        CREATE INDEX idx_measurements_id_obs_time ON measurements(dp_id, obs_time);
        CREATE INDEX idx_forecasts_id_obs_time ON forecasts(dp_id, obs_time);
        CREATE INDEX measurements_obs_time_idx ON public.measurements USING btree (obs_time DESC);
        CREATE INDEX forecasts_obs_time_idx ON public.forecasts USING btree (obs_time DESC);
    """)