#!/usr/bin/env python
"""
-------------------------------------------------------
2021-02-01 -- Christian Foerster
christian.foerster@eawag.ch
-------------------------------------------------------
"""
from collections.abc import Iterable

from sqlalchemy.orm import sessionmaker


class AlchemySession:
    def __init__(self, engine):
        self.__engine = engine

    def __enter__(self):
        session = sessionmaker(bind=self.__engine)
        self.session = session()
        return self.session

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.session.close()


def format_return(tables):
    if isinstance(tables, Iterable):
        return {table.__tablename__: table for table in tables}
    else:
        return {tables.__tablename__: tables}


class Database:
    def __init__(self, engine):
        self.db_engine = engine

    def init_database_model(self, schema):
        """
        Examples of table definition
        ----------------------------

        https://www.pythonsheets.com/notes/python-sqlalchemy.html
        https://leportella.com/sqlalchemy-tutorial.html
        https://docs.sqlalchemy.org/en/13/core/type_basics.html?highlight=types#module-sqlalchemy.types

        """

        tables = schema(self.db_engine)

        return format_return(tables)

    def fill_table(self, table, rows, batch_size=10000):
        """
        rows: list containing dict (key colname: val value)
        """
        with AlchemySession(self.db_engine) as session:
            try:
                start, end = 0, batch_size
                data_len = len(rows)
                while start < data_len:
                    self.db_engine.execute(table.__table__.insert(), rows[start:end])
                    start += batch_size
                    end += batch_size

                session.commit()

            except Exception as error:
                session.rollback()
                raise error

            finally:
                session.close()

    def iter_table(self, table):
        with AlchemySession(self.db_engine) as session:
            tbl = session.query(table)
            for row in tbl:
                yield row._asdict()

    def execute_query(self, query, commit=False):
        with AlchemySession(self.db_engine) as session:
            session.execute(query)

            if commit:
                session.commit()
