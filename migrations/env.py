import os
import sys
from flask import current_app
from sqlalchemy import create_engine
from alembic import context
from sqlalchemy.orm import configure_mappers

# این خط تضمین می کند که Alembic پوشه backend را به sys.path اضافه کند
# تا بتواند به مدل ها و برنامه Flask دسترسی پیدا کند.
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

# این شیء پیکربندی Alembic است که دسترسی به مقادیر
# موجود در فایل .ini مورد استفاده را فراهم می کند.
config = context.config

# اینجا، metadata مدل شما برای پشتیبانی از 'autogenerate' اضافه می شود.
# این خط تضمین می کند که Alembic از همان MetaData که Flask-Migrate استفاده می کند، بهره ببرد.
# این شامل تمام مدل هایی است که در زمان راه اندازی برنامه Flask بارگذاری شده اند.
target_metadata = current_app.extensions['migrate'].db.metadata

# اطمینان حاصل می کند که تمام مدل ها بارگذاری شده اند تا autogenerate بتواند آنها را ببیند.
configure_mappers()

def run_migrations_offline() -> None:
    """مهاجرت ها را در حالت 'offline' اجرا می کند."""
    url = current_app.config.get("SQLALCHEMY_DATABASE_URI")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()

def run_migrations_online() -> None:
    """مهاجرت ها را در حالت 'online' اجرا می کند."""
    url = current_app.config.get("SQLALCHEMY_DATABASE_URI")
    if not url:
        raise ValueError("SQLALCHEMY_DATABASE_URI not found in Flask app config.")

    # اضافه کردن connect_args برای اطمینان از کدگذاری UTF-8
    connectable = create_engine(
        url,
        connect_args={"client_encoding": "utf8"}
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
            # این تابع را اضافه کردیم تا Alembic کامنت های ستون ها را نادیده بگیرد.
            include_object=lambda object, name, type_, reflected, compare_to: (
                object.comment is None if type_ == "column" else True
            )
        )
        with context.begin_transaction():
            context.run_migrations()

if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()