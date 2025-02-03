"""
datatype extension

Extends the time series such that additional datatypes can be handled. Note that this script is in no way
sql-injection-save. If you request an SQL injection, you'll get it. Make sure to feed the script only with trusted
content.

Revision ID: 0678397a4d04
Revises: 615038092266
Create Date: 2025-01-29 11:21:05.075181

"""
import os

from alembic import op
import sqlalchemy as sql

# revision identifiers, used by Alembic.
revision = '0678397a4d04'
down_revision = '615038092266'
branch_labels = None
depends_on = None


def upgrade():
    """Installs the additional time series tables and establishes the compatibility measures"""

    upgrade_move_data()
    upgrade_create_legacy_views()
    upgrade_type_system()
    upgrade_new_ts_tables()
    upgrade_type_checks()


def upgrade_move_data():
    """Rename the existing forecasts and measurements tables and drop the permissions that are not needed anymore"""

    data_vis_user = os.environ['POSTGRES_DATA_VIS_USER']
    data_source_user = os.environ['POSTGRES_DATA_SOURCE_USER']

    op.execute(sql.text(f"""
        ALTER TABLE measurements RENAME TO raw_unitemporal_double;
        -- No more direct data access for the visualization user. Use the guarded views instead.
        REVOKE ALL PRIVILEGES ON TABLE raw_unitemporal_double FROM {data_vis_user}, {data_source_user}; 
    """))

    op.execute(sql.text(f"""
        ALTER TABLE forecasts RENAME TO raw_bitemporal_double;
        -- No more direct data access for the visualization user. Use the guarded views instead.
        REVOKE ALL PRIVILEGES ON TABLE raw_bitemporal_double FROM {data_vis_user}, {data_source_user}; 
    """))


def upgrade_create_legacy_views():
    """implements the legacy views for backwards compatibility"""

    data_vis_user = os.environ['POSTGRES_DATA_VIS_USER']
    data_source_user = os.environ['POSTGRES_DATA_SOURCE_USER']

    op.execute(sql.text(f"""
        CREATE VIEW measurements AS SELECT * FROM raw_unitemporal_double;
        GRANT SELECT, INSERT, UPDATE ON measurements TO data_source_base, {data_source_user};
        GRANT SELECT ON measurements TO restricting_view_executor, {data_vis_user}; 
    """))

    op.execute(sql.text(f"""
        CREATE VIEW forecasts AS SELECT * FROM raw_bitemporal_double;
        GRANT SELECT, INSERT, UPDATE ON forecasts TO data_source_base, {data_source_user};
        GRANT SELECT ON forecasts TO restricting_view_executor, {data_vis_user}; 
    """))


def upgrade_type_system():
    """Introduces the basic type system that checks the metric type"""

    op.execute(sql.text("""
        CREATE TYPE time_series_data_type AS ENUM ('double', 'bigint', 'boolean', 'jsonb');
        CREATE TYPE time_series_temporality AS ENUM ('unitemporal', 'bitemporal');
    """))

    op.execute(sql.text("""
        ALTER TABLE data_points ADD COLUMN data_type time_series_data_type NOT NULL DEFAULT 'double';
        ALTER TABLE data_points ADD COLUMN temporality time_series_temporality DEFAULT NULL;
    """))

    op.execute(sql.text("""
        -- Make sure only the legacy double type can omit the temporality
        ALTER TABLE data_points ADD CONSTRAINT check_temporality CHECK (data_type='double' OR temporality IS NOT NULL); 
    """))


def upgrade_new_ts_tables():
    """Creates the new time-series tables for various data types"""

    create_unitemporal_table("bigint")
    create_bitemporal_table("bigint")

    create_unitemporal_table("boolean")
    create_bitemporal_table("boolean")

    create_unitemporal_table("jsonb")
    create_bitemporal_table("jsonb")


def create_unitemporal_table(data_type: str):
    """Creates an unitemporal tables for the given data type"""

    data_type_infix = data_type.replace(" ", "_")

    op.execute(sql.text(f"""
        CREATE TABLE raw_unitemporal_{data_type_infix} (
            dp_id INTEGER NOT NULL,
            obs_time TIMESTAMPTZ NOT NULL,
            value {data_type} NULL,
            PRIMARY KEY (dp_id, obs_time),
            FOREIGN KEY (dp_id) REFERENCES data_points(id)
        );
        COMMENT ON TABLE raw_unitemporal_{data_type_infix} 
            IS 'Stores the actual time series for type {data_type} having one observation timestamp';
    """))

    op.execute(sql.text(f"""
        SELECT create_hypertable(
                'raw_unitemporal_{data_type_infix}', 'obs_time', 
                chunk_time_interval=>INTERVAL '1 day'
            );   
        ALTER TABLE raw_unitemporal_{data_type_infix} SET (
            timescaledb.compress=true,
            timescaledb.compress_segmentby='dp_id',
            timescaledb.compress_orderby='obs_time'
        );
        SELECT add_compression_policy('raw_unitemporal_{data_type_infix}', INTERVAL '2 days');
    """))

    grant_data_table_permissions(f"raw_unitemporal_{data_type_infix}")


def create_bitemporal_table(data_type: str):
    """Creates a bitemporal tables for the given data type"""

    data_type_infix = data_type.replace(" ", "_")

    op.execute(sql.text(f"""
        CREATE TABLE raw_bitemporal_{data_type_infix} (
            dp_id INTEGER NOT NULL,
            obs_time TIMESTAMPTZ NOT NULL,
            fc_time TIMESTAMPTZ NOT NULL,
            value {data_type} NULL,
            PRIMARY KEY (dp_id, obs_time, fc_time),
            FOREIGN KEY (dp_id) REFERENCES data_points(id)
        );
        COMMENT ON TABLE raw_unitemporal_{data_type_infix} 
            IS 'Stores the actual time series for type {data_type} having a dedicated observation and generation time';
    """))

    op.execute(sql.text(f"""
        SELECT create_hypertable(
                'raw_bitemporal_{data_type_infix}', 'obs_time', 
                chunk_time_interval=>INTERVAL '1 day'
            );   
        ALTER TABLE raw_bitemporal_{data_type_infix} SET (
            timescaledb.compress=true,
            timescaledb.compress_segmentby='dp_id',
            timescaledb.compress_orderby='obs_time, fc_time DESC'
        );
        SELECT add_compression_policy('raw_bitemporal_{data_type_infix}', INTERVAL '2 days');
    """))

    grant_data_table_permissions(f"raw_bitemporal_{data_type_infix}")


def grant_data_table_permissions(table_name):
    """Grants the permissions fo the newly generate data table"""

    op.execute(sql.text(f"""
        GRANT SELECT, INSERT, UPDATE ON {table_name} TO data_source_base;
        GRANT SELECT ON {table_name} TO restricting_view_executor
    """))


def upgrade_type_checks():
    """
    Creates a bundle of type check functions to verify the insert statements.

    In case the corresponding trigger create a bottleneck, consider removing the type checks again.
    """

    op.execute(sql.text("""
        -- Defines the trigger function that checks the datapoint type before inserting
        -- The function requires two arguments, the first one is the target data type and the second one corresponds to
        -- the temporality of the table.
        CREATE OR REPLACE FUNCTION rdp_tr_check_type() RETURNS TRIGGER 
        LANGUAGE plpgsql STABLE PARALLEL RESTRICTED
        AS $$
        DECLARE
            target_data_type time_series_data_type := TG_ARGV[0];
            target_temporality time_series_temporality := TG_ARGV[1];
            ref_info RECORD;   
        BEGIN
            SELECT data_type, temporality INTO ref_info 
                FROM data_points
                WHERE id = NEW.dp_id;
            
            IF ref_info.data_type <> target_data_type THEN
                RAISE EXCEPTION 'Invalid data type % of %, expected %', 
                    ref_info.data_type, NEW.dp_id, target_data_type;
            END IF;

            IF COALESCE(ref_info.temporality, target_temporality) <> target_temporality THEN
                RAISE EXCEPTION 'Invalid temporality % of %, expected %', 
                    ref_info.temporality, NEW.dp_id, target_temporality;
            END IF;
            RETURN NEW;
        END;        
        $$
    """))

    add_type_check("raw_unitemporal_double", "double", "unitemporal")
    add_type_check("raw_bitemporal_double", "double", "bitemporal")

    add_type_check("raw_unitemporal_bigint", "bigint", "unitemporal")
    add_type_check("raw_bitemporal_bigint", "bigint", "bitemporal")

    add_type_check("raw_unitemporal_boolean", "boolean", "unitemporal")
    add_type_check("raw_bitemporal_boolean", "boolean", "bitemporal")

    add_type_check("raw_unitemporal_jsonb", "jsonb", "unitemporal")
    add_type_check("raw_bitemporal_jsonb", "jsonb", "bitemporal")


def add_type_check(table_name, data_type, temporality):
    """Creates the type check trigger on the particular table"""

    op.execute(sql.text(f"""
        CREATE OR REPLACE TRIGGER check_type 
            BEFORE INSERT OR UPDATE 
            ON {table_name}
            FOR EACH ROW
            EXECUTE FUNCTION rdp_tr_check_type('{data_type}', '{temporality}');
    """))


def downgrade():
    """Reverts the changes of this revision"""

    downgrade_new_ts_tables()
    downgrade_type_checks()
    downgrade_type_system()
    downgrade_legacy_views()
    downgrade_move_data()


def downgrade_new_ts_tables():
    """Drops the new tables including all the contained data"""

    # Cannot drop multiple hypertables at once: https://github.com/timescale/timescaledb/issues/2303
    op.execute(sql.text("""
        DROP TABLE IF EXISTS raw_unitemporal_bigint;
        DROP TABLE IF EXISTS raw_unitemporal_boolean;
        DROP TABLE IF EXISTS raw_unitemporal_jsonb;

        DROP TABLE IF EXISTS raw_bitemporal_bigint;
        DROP TABLE IF EXISTS raw_bitemporal_boolean;
        DROP TABLE IF EXISTS raw_bitemporal_jsonb;
    """))


def downgrade_type_checks():
    """Removes the type checks from the datapoints table"""

    op.execute(sql.text("""
        DROP TRIGGER IF EXISTS check_type ON raw_bitemporal_double;
        DROP TRIGGER IF EXISTS check_type ON raw_unitemporal_double;

        DROP TRIGGER IF EXISTS check_type ON raw_bitemporal_bigint;
        DROP TRIGGER IF EXISTS check_type ON raw_unitemporal_bigint;

        DROP TRIGGER IF EXISTS check_type ON raw_bitemporal_boolean;
        DROP TRIGGER IF EXISTS check_type ON raw_unitemporal_boolean;

        DROP TRIGGER IF EXISTS check_type ON raw_bitemporal_jsonb;
        DROP TRIGGER IF EXISTS check_type ON raw_unitemporal_jsonb;

        DROP FUNCTION rdp_tr_check_type;
    """))


def downgrade_type_system():
    """Downgrades the type system protecting the time series"""

    op.execute(sql.text("""
        ALTER TABLE data_points DROP CONSTRAINT check_temporality;
        ALTER TABLE data_points DROP COLUMN temporality;
        ALTER TABLE data_points DROP COLUMN data_type;
    
        DROP TYPE time_series_temporality;
        DROP TYPE time_series_data_type;
    """))


def downgrade_legacy_views():
    """Removes the legacy views for bridging to the new table scheme"""

    op.execute(sql.text("""
        DROP VIEW forecasts;
        DROP VIEW measurements;
    """))


def downgrade_move_data():
    """Moves the floating point time series data back to its old location and enable direct vis user access"""

    data_vis_user = os.environ['POSTGRES_DATA_VIS_USER']
    data_source_user = os.environ['POSTGRES_DATA_SOURCE_USER']

    op.execute(sql.text(f"""
        GRANT SELECT ON TABLE raw_bitemporal_double TO {data_vis_user};
        GRANT SELECT, UPDATE, INSERT ON TABLE raw_bitemporal_double TO {data_source_user};
        ALTER TABLE raw_bitemporal_double RENAME TO forecasts;
    """))

    op.execute(sql.text(f"""
        GRANT SELECT ON TABLE raw_unitemporal_double TO {data_vis_user};
        GRANT SELECT, UPDATE, INSERT ON TABLE raw_unitemporal_double TO {data_source_user};
        ALTER TABLE raw_unitemporal_double RENAME TO measurements;
    """))
