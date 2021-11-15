import pytest
from database import Database
from database_data import DB_DATA
from database_schema import db_schema
from pytest_postgresql.janitor import DatabaseJanitor
from sqlalchemy import create_engine

from datapool_client import DataPool, Plot, ToolBox


@pytest.fixture(scope="session")
def engine_postgresql(postgresql_proc):
    # variable definition

    with DatabaseJanitor(
        postgresql_proc.user,
        postgresql_proc.host,
        postgresql_proc.port,
        postgresql_proc.dbname,
        postgresql_proc.version,
        password=postgresql_proc.password,
    ):
        yield create_engine(
            f"postgresql+psycopg2://{postgresql_proc.user}:{postgresql_proc.password}@{postgresql_proc.host}:"
            f"{postgresql_proc.port}/{postgresql_proc.dbname}"
        )


@pytest.fixture(scope="session")
def dp(postgresql_proc):
    return DataPool(
        user=postgresql_proc.user,
        host=postgresql_proc.host,
        port=postgresql_proc.port,
        database=postgresql_proc.dbname,
        password=postgresql_proc.password,
        verbose=False,
    )


@pytest.fixture(scope="session")
def toolbox(postgresql_proc):
    return ToolBox(
        user=postgresql_proc.user,
        host=postgresql_proc.host,
        port=postgresql_proc.port,
        database=postgresql_proc.dbname,
        password=postgresql_proc.password,
        verbose=False,
    )


@pytest.fixture(scope="session")
def plot(postgresql_proc):
    return Plot(
        user=postgresql_proc.user,
        host=postgresql_proc.host,
        port=postgresql_proc.port,
        database=postgresql_proc.dbname,
        password=postgresql_proc.password,
        verbose=False,
    )


@pytest.fixture(scope="session")
def setup_postgres(engine_postgresql):
    db = Database(engine=engine_postgresql)
    tables = db.init_database_model(db_schema)
    for name, table in tables.items():
        db.fill_table(table, DB_DATA[name])
    return db, tables
