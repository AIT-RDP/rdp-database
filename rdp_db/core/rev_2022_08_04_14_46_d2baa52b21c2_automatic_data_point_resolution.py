"""Automatic data_point resolution

Revision ID: d2baa52b21c2
Revises: 49bc8370e2fd
Create Date: 2022-08-04 14:46:21.316713

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'd2baa52b21c2'
down_revision = '49bc8370e2fd'
branch_labels = None
depends_on = None


def upgrade():
    op.execute(sa.text("""
        CREATE OR REPLACE FUNCTION get_or_create_data_point_id(
                name VARCHAR(128),
                device_id VARCHAR(128),
                location_code VARCHAR(128),
                data_provider VARCHAR(128)) RETURNS INTEGER AS $$
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
                         INSERT INTO data_points(name, device_id, location_code, data_provider)
                                VALUES (get_or_create_data_point_id.name,
                                        get_or_create_data_point_id.device_id,
                                        get_or_create_data_point_id.location_code,
                                        get_or_create_data_point_id.data_provider)
                                RETURNING data_points.id INTO dp_id;
                END IF;
                RETURN dp_id;
        END;
        $$ LANGUAGE plpgsql;
    """))


def downgrade():
    op.execute(sa.text("DROP FUNCTION get_or_create_data_point_id;"))
