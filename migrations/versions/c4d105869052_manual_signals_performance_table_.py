"""Manual signals_performance table cleanup - Final

Revision ID: <YOUR_NEW_REVISION_ID> # REPLACE THIS with the actual ID from your new file
Revises: 0af8563a591e # REPLACE THIS with the actual down_revision from your new file
Create Date: <YOUR_NEW_CREATION_DATE> # REPLACE THIS with the actual creation date from your new file

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect # IMPORTANT: Add this import

# revision identifiers, used by Alembic.
revision = '<YOUR_NEW_REVISION_ID>' # REPLACE THIS with your actual new revision ID
down_revision = '0af8563a591e' # REPLACE THIS with your actual down_revision
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    inspector = inspect(bind)
    
    # Get current columns, unique constraints, and foreign keys to avoid duplicates
    existing_columns = [col['name'] for col in inspector.get_columns('signals_performance')]
    existing_unique_constraints = [c['name'] for c in inspector.get_unique_constraints('signals_performance')]
    existing_foreign_keys = [fk['name'] for fk in inspector.get_foreign_keys('signals_performance')]

    # --- Upgrade Operations ---

    # Step 1: Ensure columns are NOT NULL if the model specifies them as such
    # This needs to be done *before* adding unique/foreign key constraints if they are NOT NULL.
    # We use batch_alter_table for this as it handles SQLite's limitations.
    with op.batch_alter_table('signals_performance', schema=None) as batch_op:
        # Alter signal_id to NOT NULL if it exists and is nullable
        if 'signal_id' in existing_columns:
            col_info = next((col for col in inspector.get_columns('signals_performance') if col['name'] == 'signal_id'), None)
            if col_info and col_info['nullable']:
                # IMPORTANT: If there are NULL values in signal_id, this will fail.
                # You might need to run op.execute("UPDATE signals_performance SET signal_id = '<some_uuid>' WHERE signal_id IS NULL")
                # before this migration if you have existing NULLs.
                batch_op.alter_column('signal_id', existing_type=sa.String(length=36), nullable=False)
        
        # Alter symbol_id to NOT NULL if it exists and is nullable
        if 'symbol_id' in existing_columns:
            col_info = next((col for col in inspector.get_columns('signals_performance') if col['name'] == 'symbol_id'), None)
            if col_info and col_info['nullable']:
                # IMPORTANT: If there are NULL values in symbol_id, this will fail.
                # You might need to run op.execute("UPDATE signals_performance SET symbol_id = '<some_default_id>' WHERE symbol_id IS NULL")
                # before this migration if you have existing NULLs.
                batch_op.alter_column('symbol_id', existing_type=sa.String(length=50), nullable=False)

        # Alter symbol_name to NOT NULL if it exists and is nullable
        if 'symbol_name' in existing_columns:
            col_info = next((col for col in inspector.get_columns('signals_performance') if col['name'] == 'symbol_name'), None)
            if col_info and col_info['nullable']:
                # IMPORTANT: If there are NULL values in symbol_name, this will fail.
                # You might need to run op.execute("UPDATE signals_performance SET symbol_name = '<some_default_name>' WHERE symbol_name IS NULL")
                # before this migration if you have existing NULLs.
                batch_op.alter_column('symbol_name', existing_type=sa.String(length=255), nullable=False)

        # Alter 'reason' column type if it's not already TEXT
        current_reason_type = None
        for col in inspector.get_columns('signals_performance'):
            if col['name'] == 'reason':
                current_reason_type = str(col['type']).upper()
                break
        if current_reason_type != 'TEXT':
            batch_op.alter_column('reason',
                           existing_type=sa.VARCHAR(length=500),
                           type_=sa.Text(),
                           existing_nullable=True)
        
        # Drop the old 'signal_unique_id' column if it exists
        if 'signal_unique_id' in existing_columns:
            batch_op.drop_column('signal_unique_id')

        # Drop any unnamed unique constraints on signal_id that might have been created by previous failed attempts
        # This is a heuristic and might need manual intervention if the unnamed constraint is tricky to drop.
        # We try to drop any unnamed unique constraint that might exist on 'signal_id'
        # This is a bit risky if there are other unnamed unique constraints, but in this specific context, it's likely safe.
        # This must be done inside batch_alter_table for SQLite.
        for constraint in inspector.get_unique_constraints('signals_performance'):
            if constraint['name'] is None and 'signal_id' in constraint['column_names']:
                batch_op.drop_constraint(constraint['name'], type_='unique') # drop_constraint with None as name works inside batch_alter_table

        # Create named unique constraint for signal_id if it doesn't exist
        if '_signal_id_uc' not in existing_unique_constraints:
            batch_op.create_unique_constraint('_signal_id_uc', ['signal_id'])

        # Create named foreign key constraint for symbol_id if it doesn't exist
        if 'fk_signals_performance_symbol_id' not in existing_foreign_keys:
            batch_op.create_foreign_key('fk_signals_performance_symbol_id', 'comprehensive_symbol_data', ['symbol_id'], ['symbol_id'])
    
    # ### end Alembic commands ###


def downgrade():
    bind = op.get_bind()
    inspector = inspect(bind)
    existing_columns = [col['name'] for col in inspector.get_columns('signals_performance')]
    existing_unique_constraints = [c['name'] for c in inspector.get_unique_constraints('signals_performance')]
    existing_foreign_keys = [fk['name'] for fk in inspector.get_foreign_keys('signals_performance')]

    # --- Downgrade Operations ---

    with op.batch_alter_table('signals_performance', schema=None) as batch_op:
        # Drop foreign key constraint if it exists
        if 'fk_signals_performance_symbol_id' in existing_foreign_keys:
            batch_op.drop_constraint('fk_signals_performance_symbol_id', type_='foreignkey')

        # Drop unique constraint if it exists
        if '_signal_id_uc' in existing_unique_constraints:
            batch_op.drop_constraint('_signal_id_uc', type_='unique')

        # Alter 'reason' column type back if it's TEXT
        current_reason_type = None
        for col in inspector.get_columns('signals_performance'):
            if col['name'] == 'reason':
                current_reason_type = str(col['type']).upper()
                break
        if current_reason_type == 'TEXT':
            batch_op.alter_column('reason',
                           existing_type=sa.Text(),
                           type_=sa.VARCHAR(length=500),
                           existing_nullable=True)
        
        # Alter new columns to nullable=True before dropping (if they were nullable=False)
        for col_name, col_type in [('signal_id', sa.String(length=36)), 
                                   ('symbol_id', sa.String(length=50)), 
                                   ('symbol_name', sa.String(length=255))]:
            if col_name in existing_columns:
                col_info = next((col for col in inspector.get_columns('signals_performance') if col['name'] == col_name), None)
                if col_info and not col_info['nullable']: # Only alter if currently NOT nullable
                    batch_op.alter_column(col_name, existing_type=col_type, nullable=True)

        # Drop new columns if they exist
        if 'symbol_name' in existing_columns:
            batch_op.drop_column('symbol_name')
        if 'symbol_id' in existing_columns:
            batch_op.drop_column('symbol_id')
        if 'signal_id' in existing_columns:
            batch_op.drop_column('signal_id')
        
        # Add back old column if it doesn't exist
        if 'signal_unique_id' not in existing_columns:
            batch_op.add_column(sa.Column('signal_unique_id', sa.VARCHAR(length=36), nullable=False))

    # ### end Alembic commands ###
