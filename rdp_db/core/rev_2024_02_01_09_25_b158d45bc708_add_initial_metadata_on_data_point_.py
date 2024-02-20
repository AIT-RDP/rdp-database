"""add initial metadata on data point creation

Revision ID: b158d45bc708
Revises: 3a7a5c75ae76
Create Date: 2024-02-01 09:25:15.559802

"""
import os

from alembic import op
import sqlalchemy as sa
import rdp_db.core.rev_2022_08_04_14_46_d2baa52b21c2_automatic_data_point_resolution as dp_resolution

# revision identifiers, used by Alembic.
revision = 'b158d45bc708'
down_revision = '3a7a5c75ae76'
branch_labels = "rdp_db_core"
depends_on = None


def upgrade():
    data_source_user = os.environ['POSTGRES_DATA_SOURCE_USER']

    op.execute(sa.text(f"""
        DROP FUNCTION get_or_create_data_point_id(varchar, varchar, varchar, varchar); -- Avoid duplication
        
        CREATE OR REPLACE FUNCTION get_or_create_data_point_id(
                name VARCHAR(128),
                device_id VARCHAR(128),
                location_code VARCHAR(128),
                data_provider VARCHAR(128),
                initial_unit TEXT DEFAULT NULL,
                initial_metadata JSONB DEFAULT '{{}}'::jsonb) RETURNS INTEGER AS $$
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
                         INSERT INTO data_points(name, device_id, location_code, data_provider, unit, metadata)
                                VALUES (get_or_create_data_point_id.name,
                                        get_or_create_data_point_id.device_id,
                                        get_or_create_data_point_id.location_code,
                                        get_or_create_data_point_id.data_provider,
                                        get_or_create_data_point_id.initial_unit,
                                        get_or_create_data_point_id.initial_metadata)
                                RETURNING data_points.id INTO dp_id;
                END IF;
                RETURN dp_id;
        END;
        $$ LANGUAGE plpgsql;
        
        GRANT EXECUTE ON FUNCTION public.get_or_create_data_point_id(varchar, varchar, varchar, varchar, text, jsonb) 
            TO data_source_base;
        -- legacy permission for the data source user
        GRANT EXECUTE ON FUNCTION public.get_or_create_data_point_id(varchar, varchar, varchar, varchar, text, jsonb) 
            TO {data_source_user};
        
        COMMENT ON FUNCTION public.get_or_create_data_point_id(varchar, varchar, varchar, varchar, text, jsonb) IS 
            'Returns the datapoint id having the specified name, device_id, location_code, and data_provider. In 
             case non exist a data point will be created. The initial_unit and initial_metadata will only be used
             when creating the data point. No update will be performed.';
    """))


def downgrade():
    data_source_user = os.environ['POSTGRES_DATA_SOURCE_USER']

    op.execute(sa.text("""
        DROP FUNCTION get_or_create_data_point_id(varchar, varchar, varchar, varchar, text, jsonb);
    """))

    dp_resolution.upgrade()
    op.execute(sa.text(f"""
        GRANT EXECUTE ON FUNCTION public.get_or_create_data_point_id(varchar, varchar, varchar, varchar) 
            TO data_source_base;
        GRANT EXECUTE ON FUNCTION public.get_or_create_data_point_id(varchar, varchar, varchar, varchar) 
            TO {data_source_user};
    """))
