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
import rdp_db.core.rev_2024_02_01_09_25_b158d45bc708_add_initial_metadata_on_data_point_ as rev_metadata

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
    upgrade_data_views()
    upgrade_data_point_access_function()
    upgrade_resolve_destination_function()


def upgrade_move_data():
    """Rename the existing forecasts and measurements tables and drop the permissions that are not needed anymore"""

    data_vis_user = os.environ['POSTGRES_DATA_VIS_USER']
    data_source_user = os.environ['POSTGRES_DATA_SOURCE_USER']

    op.execute(sql.text(f"""
        ALTER TABLE measurements RENAME TO raw_unitemporal_double;
        ALTER TABLE raw_unitemporal_double RENAME COLUMN obs_time TO valid_time;
        
        -- No more direct data access for the visualization user. Use the guarded views instead.
        REVOKE ALL PRIVILEGES ON TABLE raw_unitemporal_double FROM {data_vis_user}, {data_source_user}; 
    """))

    op.execute(sql.text(f"""
        ALTER TABLE forecasts RENAME TO raw_bitemporal_double;
        ALTER TABLE raw_bitemporal_double RENAME COLUMN obs_time TO valid_time;
        ALTER TABLE raw_bitemporal_double RENAME COLUMN fc_time TO transaction_time;
        
        -- No more direct data access for the visualization user. Use the guarded views instead.
        REVOKE ALL PRIVILEGES ON TABLE raw_bitemporal_double FROM {data_vis_user}, {data_source_user}; 
    """))


def upgrade_create_legacy_views():
    """implements the legacy views for backwards compatibility"""

    data_vis_user = os.environ['POSTGRES_DATA_VIS_USER']
    data_source_user = os.environ['POSTGRES_DATA_SOURCE_USER']

    op.execute(sql.text(f"""
        CREATE VIEW measurements AS SELECT dp_id, valid_time AS obs_time, value FROM raw_unitemporal_double;
        GRANT SELECT, INSERT, UPDATE ON measurements TO data_source_base, {data_source_user};
        GRANT SELECT ON measurements TO restricting_view_executor, {data_vis_user}; 
    """))

    op.execute(sql.text(f"""
        CREATE VIEW forecasts AS 
            SELECT dp_id, valid_time AS obs_time, transaction_time AS fc_time, value 
                FROM raw_bitemporal_double;
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
            valid_time TIMESTAMPTZ NOT NULL,
            value {data_type} NULL,
            PRIMARY KEY (dp_id, valid_time),
            FOREIGN KEY (dp_id) REFERENCES data_points(id)
        );
        COMMENT ON TABLE raw_unitemporal_{data_type_infix} 
            IS 'Stores the actual time series for type {data_type} having one observation timestamp';
        COMMENT ON COLUMN raw_unitemporal_{data_type_infix}.dp_id
            IS 'Reference to the data_points entry that holds all the details on the time series';
        COMMENT ON COLUMN raw_unitemporal_{data_type_infix}.valid_time
            IS 'The (start) time for which the stored value is valid. E.g., the time at which the observation was made';
        COMMENT ON COLUMN raw_unitemporal_{data_type_infix}.value
            IS 'The actual payload at the particular instant of time';
    """))

    op.execute(sql.text(f"""
        SELECT create_hypertable(
                'raw_unitemporal_{data_type_infix}', 'valid_time', 
                chunk_time_interval=>INTERVAL '1 day'
            );   
        ALTER TABLE raw_unitemporal_{data_type_infix} SET (
            timescaledb.compress=true,
            timescaledb.compress_segmentby='dp_id',
            timescaledb.compress_orderby='valid_time'
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
            valid_time TIMESTAMPTZ NOT NULL,
            transaction_time TIMESTAMPTZ NOT NULL,
            value {data_type} NULL,
            PRIMARY KEY (dp_id, valid_time, transaction_time),
            FOREIGN KEY (dp_id) REFERENCES data_points(id)
        );
        COMMENT ON TABLE raw_bitemporal_{data_type_infix} 
            IS 'Stores the actual time series for type {data_type} having a dedicated observation and generation time';
        COMMENT ON COLUMN raw_bitemporal_{data_type_infix}.dp_id
            IS 'Reference to the data_points entry that holds all the details on the time series';
        COMMENT ON COLUMN raw_bitemporal_{data_type_infix}.valid_time
            IS 'The (start) time for which the stored value is valid. E.g., the time at which the observation was made';
        COMMENT ON COLUMN raw_bitemporal_{data_type_infix}.transaction_time
            IS 'The time at which the stored value was created or entered (e.g., the time a forecast was calculated)';
        COMMENT ON COLUMN raw_bitemporal_{data_type_infix}.value
            IS 'The actual payload for the particular instant of valid time, as created at the transaction time';

    """))

    op.execute(sql.text(f"""
        SELECT create_hypertable(
                'raw_bitemporal_{data_type_infix}', 'valid_time', 
                chunk_time_interval=>INTERVAL '1 day'
            );   
        ALTER TABLE raw_bitemporal_{data_type_infix} SET (
            timescaledb.compress=true,
            timescaledb.compress_segmentby='dp_id',
            timescaledb.compress_orderby='valid_time, transaction_time DESC'
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


def upgrade_data_views():
    """Creates the new data views for all type series"""

    append_typed_unitemporal_details_view("double", True)
    append_typed_unitemporal_details_view("bigint", False)
    append_typed_unitemporal_details_view("boolean", False)
    append_typed_unitemporal_details_view("jsonb", False)

    append_typed_bitemporal_details_view("double", True)
    append_typed_bitemporal_details_view("bigint", False)
    append_typed_bitemporal_details_view("boolean", False)
    append_typed_bitemporal_details_view("jsonb", False)


def append_typed_unitemporal_details_view(type_name: str, null_temporality: bool):
    """Appends a single view and sets the appropriate permissions"""

    if null_temporality:
        temporality_clause = "dp.temporality IS NULL OR"
    else:
        temporality_clause = ""

    op.execute(sql.text(f"""
        CREATE OR REPLACE VIEW unitemporal_{type_name}_details(
            dp_id, valid_time, value, name, device_id, location_code, data_provider, unit, view_role, metadata, 
            data_type, temporality
        ) AS 
        SELECT dp.id, valid_time, value, dp.name, dp.device_id, dp.location_code, dp.data_provider, dp.unit,
                dp.view_role, dp.metadata, dp.data_type, dp.temporality
            FROM raw_unitemporal_{type_name} AS raw
            JOIN data_points AS dp
                ON (raw.dp_id = dp.id)
            WHERE ({temporality_clause} dp.temporality = 'unitemporal') AND dp.data_type='{type_name}';
        COMMENT ON VIEW unitemporal_{type_name}_details IS 'Joint time series and data point information';
    """))

    op.execute(sql.text(f"""
        -- Enables access for vis users to the referenced data tables
        ALTER VIEW unitemporal_{type_name}_details OWNER TO restricting_view_executor;
        GRANT SELECT, TRIGGER ON unitemporal_{type_name}_details TO view_base;
    """))


def append_typed_bitemporal_details_view(type_name: str, null_temporality: bool):
    """Appends a single view and sets the appropriate permissions"""

    if null_temporality:
        temporality_clause = "dp.temporality IS NULL OR"
    else:
        temporality_clause = ""

    op.execute(sql.text(f"""
        CREATE OR REPLACE VIEW bitemporal_{type_name}_details(
            dp_id, valid_time, transaction_time, value, name, device_id, location_code, data_provider, unit, view_role,
            metadata, data_type, temporality
        ) AS 
        SELECT dp.id, valid_time, transaction_time, value, dp.name, dp.device_id, dp.location_code, dp.data_provider, 
                dp.unit, dp.view_role, dp.metadata, dp.data_type, dp.temporality
            FROM raw_bitemporal_{type_name} AS raw
            JOIN data_points AS dp
                ON (raw.dp_id = dp.id)
            WHERE ({temporality_clause} dp.temporality = 'bitemporal') AND dp.data_type='{type_name}';
        COMMENT ON VIEW bitemporal_{type_name}_details IS 'Joint time series and data point information';
    """))

    op.execute(sql.text(f"""
        -- Enables access for vis users to the referenced data tables
        ALTER VIEW bitemporal_{type_name}_details OWNER TO restricting_view_executor;
        GRANT SELECT, TRIGGER ON bitemporal_{type_name}_details TO view_base;
    """))


def upgrade_data_point_access_function():
    """Upgrade the data point creation function to consider temporality and data types as well"""

    op.execute(sql.text("""
        DROP FUNCTION get_or_create_data_point_id(
                VARCHAR, VARCHAR, VARCHAR, VARCHAR, TEXT, JSONB
            ); -- Avoid duplication 
    """))
    create_data_point_access_function()


def create_data_point_access_function():
    """
    Creates the actual data point access function.

    The creation routine may be useful for future downgrades as well
    """

    op.execute(sql.text(f"""
        CREATE OR REPLACE FUNCTION get_or_create_data_point_id(
                name VARCHAR(128),
                device_id VARCHAR(128),
                location_code VARCHAR(128),
                data_provider VARCHAR(128),
                initial_unit TEXT DEFAULT NULL,
                initial_metadata JSONB DEFAULT '{{}}'::jsonb,
                initial_data_type time_series_data_type DEFAULT 'double',
                initial_temporality time_Series_temporality DEFAULT NULL
            ) RETURNS INTEGER AS $$
        DECLARE
                dp_id INTEGER;
        BEGIN
            SELECT id FROM data_points
                WHERE data_points.name = get_or_create_data_point_id.name AND
                    ((data_points.device_id IS NULL AND get_or_create_data_point_id.device_id IS NULL) OR 
                     (data_points.device_id = get_or_create_data_point_id.device_id)) AND
                    data_points.location_code = get_or_create_data_point_id.location_code AND
                    data_points.data_provider = get_or_create_data_point_id.data_provider
                INTO dp_id;
            IF NOT FOUND THEN
                INSERT INTO data_points(
                        name, device_id, location_code, data_provider, unit, metadata, 
                        data_type, temporality
                    ) VALUES (
                        get_or_create_data_point_id.name,
                        get_or_create_data_point_id.device_id,
                        get_or_create_data_point_id.location_code,
                        get_or_create_data_point_id.data_provider,
                        get_or_create_data_point_id.initial_unit,
                        get_or_create_data_point_id.initial_metadata,
                        get_or_create_data_point_id.initial_data_type,
                        get_or_create_data_point_id.initial_temporality
                    ) RETURNING data_points.id INTO dp_id;
            END IF;
            RETURN dp_id;
        END;
        $$ LANGUAGE plpgsql;
        
        GRANT EXECUTE ON FUNCTION public.get_or_create_data_point_id(
                VARCHAR, VARCHAR, VARCHAR, VARCHAR, TEXT, JSONB, time_series_data_type, time_series_temporality
            ) TO data_source_base;
        
        COMMENT ON FUNCTION public.get_or_create_data_point_id(
                VARCHAR, VARCHAR, VARCHAR, VARCHAR, TEXT, JSONB, time_series_data_type, time_series_temporality
            ) IS 
            'Returns the datapoint id having the specified name, device_id, location_code, and data_provider. In 
             case non exist a data point will be created. The initial_unit and initial_metadata will only be used
             when creating the data point. No update will be performed.';
    """))


def upgrade_resolve_destination_function():
    """Introduces a function that can be used to resolve the dynamic destination information"""

    op.execute(sql.text(f"""
        CREATE OR REPLACE FUNCTION rdp_resolve_data_point_info(
                name VARCHAR(128),
                device_id VARCHAR(128),
                location_code VARCHAR(128),
                data_provider VARCHAR(128),
                initial_unit TEXT DEFAULT NULL,
                initial_metadata JSONB DEFAULT '{{}}'::jsonb,
                initial_data_type time_series_data_type DEFAULT 'double',
                initial_temporality time_Series_temporality DEFAULT NULL,
                -- Output types that read back the information
                OUT dp_id INTEGER,
                OUT data_type time_series_data_type,
                OUT temporality time_Series_temporality
            ) AS $$
        DECLARE
                dp_info RECORD;
        BEGIN
            SELECT data_points.id AS dp_id, data_points.data_type AS data_type, data_points.temporality AS temporality 
                FROM data_points
                WHERE data_points.name = rdp_resolve_data_point_info.name AND
                    ((data_points.device_id IS NULL AND rdp_resolve_data_point_info.device_id IS NULL) OR 
                     (data_points.device_id = rdp_resolve_data_point_info.device_id)) AND
                    data_points.location_code = rdp_resolve_data_point_info.location_code AND
                    data_points.data_provider = rdp_resolve_data_point_info.data_provider
                INTO dp_info;
            IF NOT FOUND THEN
                INSERT INTO data_points(
                        name, device_id, location_code, data_provider, unit, metadata, 
                        data_type, temporality
                    ) VALUES (
                        rdp_resolve_data_point_info.name,
                        rdp_resolve_data_point_info.device_id,
                        rdp_resolve_data_point_info.location_code,
                        rdp_resolve_data_point_info.data_provider,
                        rdp_resolve_data_point_info.initial_unit,
                        rdp_resolve_data_point_info.initial_metadata,
                        rdp_resolve_data_point_info.initial_data_type,
                        rdp_resolve_data_point_info.initial_temporality
                    ) RETURNING 
                        data_points.id AS dp_id, 
                        data_points.data_type AS data_type, 
                        data_points.temporality AS temporality
                    INTO dp_info;
            END IF;
            
            rdp_resolve_data_point_info.dp_id := dp_info.dp_id;
            rdp_resolve_data_point_info.data_type := dp_info.data_type;
            rdp_resolve_data_point_info.temporality := dp_info.temporality;
            
        END;
        $$ LANGUAGE plpgsql;

        GRANT EXECUTE ON FUNCTION public.rdp_resolve_data_point_info(
                VARCHAR, VARCHAR, VARCHAR, VARCHAR, TEXT, JSONB, time_series_data_type, time_series_temporality, 
                OUT INTEGER, OUT time_series_data_type, OUT time_series_temporality
            ) TO data_source_base;

        COMMENT ON FUNCTION public.rdp_resolve_data_point_info(
                VARCHAR, VARCHAR, VARCHAR, VARCHAR, TEXT, JSONB, time_series_data_type, time_series_temporality, 
                OUT INTEGER, OUT time_series_data_type, OUT time_series_temporality
            ) IS 
            'Returns the datapoint id having the specified name, device_id, location_code, and data_provider. In 
             case non exist a data point will be created. The initial_unit and initial_metadata will only be used
             when creating the data point. No update will be performed. To gather a concise view on the destination 
             tables, the function allows to read back the data_type and temporality from the data point.';
    """))


def downgrade():
    """Reverts the changes of this revision"""

    downgrade_resolve_destination_function()
    downgrade_data_point_access_function()
    downgrade_data_views()
    downgrade_new_ts_tables()
    downgrade_type_checks()
    downgrade_type_system()
    downgrade_legacy_views()
    downgrade_move_data()


def downgrade_resolve_destination_function():
    """Removes the corresponding function again"""

    op.execute(sql.text("""
        DROP FUNCTION IF EXISTS rdp_resolve_data_point_info(
                VARCHAR, VARCHAR, VARCHAR, VARCHAR, TEXT, JSONB, time_series_data_type, time_series_temporality, 
                OUT INTEGER, OUT time_series_data_type, OUT time_series_temporality
            );
    """))


def downgrade_data_point_access_function():
    """Installs the data point creation function from the previous release"""
    op.execute(sql.text("""
        DROP FUNCTION IF EXISTS get_or_create_data_point_id(
                VARCHAR, VARCHAR, VARCHAR, VARCHAR, TEXT, JSONB, time_series_data_type, time_series_temporality
            );
    """))
    rev_metadata.create_data_point_access_function()


def downgrade_data_views():
    """Drops the data views from the database"""

    op.execute(sql.text("""
        DROP VIEW IF EXISTS unitemporal_double_details, bitemporal_double_details;
        DROP VIEW IF EXISTS unitemporal_bigint_details, bitemporal_bigint_details;
        DROP VIEW IF EXISTS unitemporal_boolean_details, bitemporal_boolean_details;
        DROP VIEW IF EXISTS unitemporal_jsonb_details, bitemporal_jsonb_details;
    """))


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
        ALTER TABLE raw_bitemporal_double RENAME COLUMN valid_time TO obs_time;
        ALTER TABLE raw_bitemporal_double RENAME COLUMN transaction_time TO fc_time;
        ALTER TABLE raw_bitemporal_double RENAME TO forecasts;
    """))

    op.execute(sql.text(f"""
        GRANT SELECT ON TABLE raw_unitemporal_double TO {data_vis_user};
        GRANT SELECT, UPDATE, INSERT ON TABLE raw_unitemporal_double TO {data_source_user};
        ALTER TABLE raw_unitemporal_double RENAME COLUMN valid_time TO obs_time;
        ALTER TABLE raw_unitemporal_double RENAME TO measurements;
    """))
