from datetime import datetime as _datetime
from datetime import datetime as _dt
from tempfile import TemporaryFile as _TemporaryFile
from textwrap import dedent
from warnings import warn

import psycopg2 as _psycopg2
from numpy import arange as _arange
from numpy import array as _array
from numpy import zeros as _zeros
from pandas import DataFrame as _DataFrame
from pandas import merge as _merge
from pandas import read_csv as _read_csv

from datapool_client.core.column_map import COLUMN_MAP
from datapool_client.core.formatting import format_meta_data, reshape
from datapool_client.core.utilities import (
    choose_arguments_connection_arguments,
    clean_query_string,
    parse_dates,
    replace_in_query,
)


class Connector:
    def __init__(
            self,
            host=None,
            port=None,
            database=None,
            user=None,
            password=None,
            instance=None,
            check=True,
            to_replace={},
            verbose=True,
    ):
        """
        Please provide the connection details.
        (Defaults are for the datapool_client...)

        Example:
        host="192.168.178.33"
        port="5432"
        database="mydatabasename"
        user="db_user"
        password="db_userpassword"
        """

        self._verbose = verbose

        self._to_replace_in_query = to_replace
        self._connection_details = choose_arguments_connection_arguments(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            instance=instance,
        )

        if check:
            self._check_connection()

        self.last_query = None

    def _connect(self):
        self._conn = _psycopg2.connect(
            host=self._connection_details["host"],
            port=self._connection_details["port"],
            database=self._connection_details["database"],
            user=self._connection_details["user"],
            password=self._connection_details["password"],
        )
        self._cur = self._conn.cursor()

    def _close(self):
        self._cur.close()
        self._conn.close()

    def _check_connection(self):
        try:
            self._connect()
            self._close()
            if self._verbose:
                print("You are successfully connected to the database!")

        except Exception as e:
            print("Connection could not be established. Please check the connections data!")
            raise e

    def _query(
            self,
            query_str,
            vrs=None,
            to_dataframe=True,
            show_query=False,
            allow_modifications=False,
            columns=(),
    ):

        query_str = clean_query_string(query_str, self._to_replace_in_query)

        self.last_query = query_str

        if show_query:
            print(query_str)

        if to_dataframe:
            result = self.__query_dataframe(query_str[:-1])
            if columns:
                result.columns = columns

            return result

        else:
            return self.__query_raw(query_str, vrs, allow_modifications)

    def __query_dataframe(self, query):
        try:
            self._connect()
            with _TemporaryFile() as tmpfile:
                copy_sql = "COPY ({query}) TO STDOUT WITH CSV {head}".format(
                    query=query, head="HEADER"
                )
                self._cur.copy_expert(copy_sql, tmpfile)
                tmpfile.seek(0)
                return _read_csv(tmpfile, parse_dates=True)
        except Exception as e:
            self._conn.rollback()
            self._close()
            raise e

    def __query_raw(self, query_str, vrs, allow_modifications):
        try:
            self._connect()
            self._cur.execute(query_str, vrs)
            if allow_modifications:
                self._conn.commit()
            return {"descripton": self._cur.description, "data": self._cur.fetchall()}
        except Exception as e:
            self._conn.rollback()
            self._close()
            raise e


class DataPoolBaseDatabase(Connector):
    def query(self, query: str, vars=None, show_query=False, allow_modifications=False):
        """
        Run sql query and retrieve basic python data structures.

        query: str,
            SQL query string
        vars: tuple,
            variables to be passed to psycopg2.connection.execute
        show_query: bool,
            display sql query to stdout
        allow_modifications: bool,
            commit modifications to database
        """
        return self._query(query, vars, False, show_query, allow_modifications)

    def query_df(self, query: str, show_query=False, allow_modifications=False):
        """
        Run sql query and retrieve pandas dataframe.

        query: str,
            SQL query string
        show_query: bool,
            display sql query to stdout
        allow_modifications: bool,
            commit modifications to database
        """
        return self._query(query, None, True, show_query, allow_modifications)


class DataPoolBaseTable(Connector):
    def _columns(self, table: str):
        query_str = f"SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_NAME = '{table}';"
        res = self._query(query_str, None, True, False, False, ["cols"])
        return res.cols.tolist()

    def _rows(self, table: str):
        """Returns an estimate"""
        query_str = f"SELECT reltuples::bigint FROM pg_catalog.pg_class WHERE relname = '{table}';"
        res = self._query(query_str, None, False, False, False)
        return res["data"][0][0]

    def _all(self, table: str, to_dataframe=True, show_query=False):
        query_str = f"Select * from {table};"
        return self._query(query_str, None, to_dataframe, show_query, False)


class Parameter(DataPoolBaseTable):
    __table_name = "parameter"

    @property
    def rows(self):
        return self._rows(self.__table_name)

    @property
    def columns(self):
        return self._columns(self.__table_name)

    def all(self, to_dataframe=True, show_query=False):
        return self._all(self.__table_name, to_dataframe, show_query)

    def from_source_type(self, source_type, to_dataframe=True, show_query=False):
        """
        Parameters
        ----------
        source_type:          str with name of source_type
        to_dataframe:         bool, specifying whether the query output should be formatted as dataframe
        show_query:           bool, specifying whether to print the query

        Return
        ------
        pd.DataFrame or dict

        Example
        -------
        dp = DataPool()
        source_type = "Hach_Flo-Dar"
        dp.parameter.from_source_type(source_type)

        """
        query_str = dedent(
            f"""
            WITH parameter_ids AS (
                WITH source_ids AS (
                    WITH source_type_ids AS (
                        SELECT source_type_id::integer FROM source_type WHERE source_type.name = '{source_type}'
                    )
                    SELECT source_id::integer FROM source WHERE source_type_id = 
                    ANY(ARRAY(SELECT source_type_id::integer FROM source_type_ids))
                )
                SELECT DISTINCT parameter_id::integer FROM signal WHERE source_id = 
                ANY(ARRAY(SELECT source_id::integer FROM source_ids))
            )
            
            SELECT parameter.name FROM parameter
            WHERE parameter.parameter_id = ANY(ARRAY(SELECT parameter_id::integer FROM parameter_ids))
            """
        )

        return self._query(
            query_str,
            None,
            to_dataframe,
            show_query,
            False,
            COLUMN_MAP["parameter_from_source_type"],
        )

    def from_all_sources(self, show_query=False):
        """
        Parameters
        ----------
        show_query:           bool, specifying whether to print the query

        Return
        ------
        dict

        Example
        -------
        dp = DataPool()
        dp.parameter.from_all_sources()

        """

        response = self._query(
            "SELECT DISTINCT source_id, parameter_id FROM signal;",
            None,
            True,
            show_query,
            False,
        )
        source = self._query(
            "SELECT source_id, name FROM source;", None, True, show_query, False
        )
        source.set_index("source_id", inplace=True)
        source_map = source.to_dict()["name"]

        parameter = self._query(
            "SELECT parameter_id, name FROM parameter;", None, True, show_query, False
        )
        parameter.set_index("parameter_id", inplace=True)
        parameter_map = parameter.to_dict()["name"]

        response.source_id.replace(source_map, inplace=True)
        response.parameter_id.replace(parameter_map, inplace=True)

        source_to_parameter = {}
        for _source in response.source_id.unique():
            source_to_parameter[_source] = response[
                response["source_id"] == _source
                ].parameter_id.tolist()

        return source_to_parameter


class Site(DataPoolBaseTable):
    __table_name = "site"

    @property
    def rows(self):
        return self._rows(self.__table_name)

    @property
    def columns(self):
        return self._columns(self.__table_name)

    def all(self, to_dataframe=True, show_query=False):
        return self._all(self.__table_name, to_dataframe, show_query)

    def get_img_count(self, to_dataframe=True, show_query=False):
        query_str = dedent(
            """
            SELECT name, coord_x, coord_y, coord_z, street, postcode,
            COUNT(picture.filename), site.description
            FROM site
            LEFT JOIN picture ON site.site_id = picture.site_id
            GROUP BY site.site_id       
            """
        )

        return self._query(
            query_str,
            None,
            to_dataframe,
            show_query,
            False,
            COLUMN_MAP["site_all_img_count"],
        )

    def from_source(self, source_name, to_dataframe=True, show_query=False):
        """
        Parameters
        ----------
        source_name:          str, with name of source
        to_dataframe:         bool, specifying whether the query output should be formatted as dataframe
        show_query:           bool, specifying whether to print the query

        Return
        ------
        pd.DataFrame or dict

        Example
        -------
        dp = DataPool()
        source_type = "bn_r03_rub_morg"
        dp.site.from_source(source_name)
        """
        query_str = dedent(
            f"""
            WITH site_ids AS (
                WITH source_ids AS (
                    SELECT source_id::integer FROM source WHERE source.name = '{source_name}'
                )
                SELECT DISTINCT site_id::integer FROM signal WHERE source_id = 
                ANY(ARRAY(SELECT source_id::integer FROM source_ids))
            )
            SELECT site.name FROM site
            WHERE site.site_id = ANY(ARRAY(SELECT site_id::integer FROM site_ids))
            """
        )
        return self._query(
            query_str,
            None,
            to_dataframe,
            show_query,
            False,
            COLUMN_MAP["site_from_source"],
        )


class Project(DataPoolBaseTable):
    __table_name = "project"

    @property
    def rows(self):
        return self._rows(self.__table_name)

    @property
    def columns(self):
        return self._columns(self.__table_name)

    def all(self, to_dataframe=True, show_query=False):
        return self._all(self.__table_name, to_dataframe, show_query)


class LabResult(DataPoolBaseTable):
    __table_name = "lab_result"

    @property
    def rows(self):
        return self._rows(self.__table_name)

    @property
    def columns(self):
        return self._columns(self.__table_name)

    def all(self, to_dataframe=True, show_query=False):
        return self._all(self.__table_name, to_dataframe, show_query)


class MetaActionType(DataPoolBaseTable):
    __table_name = "meta_action_type"

    @property
    def rows(self):
        return self._rows(self.__table_name)

    @property
    def columns(self):
        return self._columns(self.__table_name)

    def all(self, to_dataframe=True, show_query=False):
        return self._all(self.__table_name, to_dataframe, show_query)


class MetaLogType(DataPoolBaseTable):
    __table_name = "meta_log_type"

    @property
    def rows(self):
        return self._rows(self.__table_name)

    @property
    def columns(self):
        return self._columns(self.__table_name)

    def all(self, to_dataframe=True, show_query=False):
        return self._all(self.__table_name, to_dataframe, show_query)


class BinaryData(DataPoolBaseTable):
    __table_name = "binary_data"

    @property
    def rows(self):
        return self._rows(self.__table_name)

    @property
    def columns(self):
        return self._columns(self.__table_name)

    def all(self, to_dataframe=True, show_query=False):
        return self._all(self.__table_name, to_dataframe, show_query)


class Person(DataPoolBaseTable):
    __table_name = "person"

    @property
    def rows(self):
        return self._rows(self.__table_name)

    @property
    def columns(self):
        return self._columns(self.__table_name)

    def all(self, to_dataframe=True, show_query=False):
        return self._all(self.__table_name, to_dataframe, show_query)


class MetaFlag(DataPoolBaseTable):
    __table_name = "meta_flag"

    @property
    def rows(self):
        return self._rows(self.__table_name)

    @property
    def columns(self):
        return self._columns(self.__table_name)

    def all(self, to_dataframe=True, show_query=False):
        return self._all(self.__table_name, to_dataframe, show_query)


class MetaData(DataPoolBaseTable):
    __table_name = "meta_data"

    @property
    def rows(self):
        return self._rows(self.__table_name)

    @property
    def columns(self):
        return self._columns(self.__table_name)

    def all(self, format_data=True, to_dataframe=True, show_query=False):
        res = self._all(self.__table_name, to_dataframe, show_query)

        if isinstance(res, _DataFrame) and format_data:
            res = format_meta_data(res)

        return res

    def get(
            self,
            *,
            source_name=None,
            site_name=None,
            format_data=True,
            to_dataframe=True,
            show_query=False,
    ):
        """
        Parameters
        ----------
        source_name:          str, with name of source
        site_name:            str, with name of site
        format_data:          bool, format meta data -> convert additional info to columns
        to_dataframe:         bool, specifying whether the query output should be formatted as dataframe
        show_query:           bool, specifying whether to print the query

        Return
        ------
        pd.DataFrame or dict

        Example
        -------
        dp = DataPool()
        dp.meta_data.get(source_name="my_source_name")
        """

        if (source_name is None) and (site_name is None):
            raise ValueError(
                "Please pass a filter! Choose at least one of those 'source_name', 'site_name'."
            )

        query_str = f"""
                    SELECT 
                        source.name,
                        site.name,
                        meta_data.description,
                        meta_data.additional_meta_info
                    FROM meta_data
                    INNER JOIN source ON source.source_id = meta_data.source_id
                    INNER JOIN site ON site.site_id = meta_data.site_id
                    WHERE
                    """
        if source_name:
            query_str += f""" source.name = '{source_name}'"""

        if source_name and site_name:
            query_str += " AND"

        if site_name:
            query_str += f""" site.name = '{site_name}'"""

        res = self._query(
            dedent(query_str),
            None,
            to_dataframe,
            show_query,
            False,
            COLUMN_MAP["meta_data_get"],
        )

        if isinstance(res, _DataFrame) and format_data:
            res = format_meta_data(res)

        return res


class MetaPicture(DataPoolBaseTable):
    __table_name = "meta_picture"

    @property
    def rows(self):
        return self._rows(self.__table_name)

    @property
    def columns(self):
        return self._columns(self.__table_name)

    def all(self, to_dataframe=True, show_query=False):
        query_str = dedent(
            f"""
            SELECT 
                picture_id,
                meta_data_id,
                meta_data_history_id,
                filename,
                description
            FROM meta_picture;
            """
        )
        return self._query(query_str, None, to_dataframe, show_query, False)

    def all_with_data_column(self, to_dataframe=True, show_query=False):
        return self._all(self.__table_name, to_dataframe, show_query)


class MetaDataHistory(DataPoolBaseTable):
    __table_name = "meta_data_history"

    @property
    def rows(self):
        return self._rows(self.__table_name)

    @property
    def columns(self):
        return self._columns(self.__table_name)

    def all(self, format_data=True, to_dataframe=True, show_query=False):
        res = self._all(self.__table_name, to_dataframe, show_query)

        if isinstance(res, _DataFrame) and format_data:
            res = format_meta_data(res)

        return res

    def get(
            self,
            *,
            source_name=None,
            site_name=None,
            start="1900-01-01 00:00:00",
            end=None,
            format_data=True,
            to_dataframe=True,
            show_query=False,
    ):
        """
        Parameters
        ----------
        source_name:          str, with name of source
        site_name:            str, with name of site
        start:                str, specifying a datetime ideally in the format yyyy-mm-dd HH:MM:SS
        end:                  str, specifying a datetime ideally in the format yyyy-mm-dd HH:MM:SS
        format_data:          bool, format meta data -> convert additional info to columns
        to_dataframe:         bool, specifying whether the query output should be formatted as dataframe
        show_query:           bool, specifying whether to print the query

        Return
        ------
        pd.DataFrame or dict

        Example
        -------
        dp = DataPool()
        dp.meta_data.get(source_name="my_source_name")
        """

        if end is None:
            end = _dt.now().strftime("%Y-%m-%d %H:%M:%S")

        st, en = parse_dates(start, end)

        query_str = f"""
                    SELECT 
                        source.name,
                        site.name,
                        meta_log_type.name,
                        meta_action_type.name,
                        meta_flag.name,
                        person.name,
                        meta_data_history.timestamp_start,
                        meta_data_history.timestamp_end,
                        meta_data_history.comment,
                        meta_data_history.additional_meta_info
                    FROM meta_data_history
                    LEFT JOIN meta_log_type ON meta_log_type.meta_log_type_id = meta_data_history.meta_log_type_id
                    LEFT JOIN meta_action_type ON meta_action_type.meta_action_type_id = meta_data_history.meta_action_type_id
                    LEFT JOIN meta_flag ON meta_flag.meta_flag_id = meta_data_history.meta_flag_id
                    INNER JOIN meta_data ON meta_data.meta_data_id = meta_data_history.meta_data_id
                    INNER JOIN person ON person.person_id = meta_data_history.person_id
                    INNER JOIN source ON source.source_id = meta_data.source_id
                    INNER JOIN site ON site.site_id = meta_data.site_id
                    WHERE meta_data_history.timestamp_start >= '{st}'
                    AND meta_data_history.timestamp_end <= '{en}'
                    """
        if source_name is not None:
            query_str += f"""AND source.name = '{source_name}'"""

        if site_name is not None:
            query_str += f"""AND site.name = '{site_name}'"""

        res = self._query(
            dedent(query_str),
            None,
            to_dataframe,
            show_query,
            False,
            COLUMN_MAP["meta_data_history_get"],
        )

        if isinstance(res, _DataFrame) and format_data:
            res = format_meta_data(res)

        return res


class Picture(DataPoolBaseTable):
    __table_name = "picture"

    @property
    def rows(self):
        return self._rows(self.__table_name)

    @property
    def columns(self):
        return self._columns(self.__table_name)

    def all(self, to_dataframe=True, show_query=False):
        query_str = dedent(
            f"""
            SELECT 
                picture_id, 
                site_id, 
                filename, 
                description, 
                timestamp
            FROM picture;
            """
        )
        return self._query(query_str, None, to_dataframe, show_query, False)

    def all_with_data_column(self, to_dataframe=True, show_query=False):
        return self._all(self.__table_name, to_dataframe, show_query)


class Source(DataPoolBaseTable):
    __table_name = "source"

    @property
    def rows(self):
        return self._rows(self.__table_name)

    @property
    def columns(self):
        return self._columns(self.__table_name)

    def all(self, to_dataframe=True, show_query=False):
        return self._all(self.__table_name, to_dataframe, show_query)

    def from_parameter(self, parameter_name, to_dataframe=True, show_query=False):
        """
        Parameters
        ----------
        parameter_name:       str, with name of parameter
        to_dataframe:         bool, specifying whether the query output should be formatted as dataframe
        show_query:           bool, specifying whether to print the query

        Return
        ------
        pd.DataFrame or dict

        Example
        -------
        dp = DataPool()
        dp.source.from_parameter(source_name="my_parameter_name")
        """
        query_str = dedent(
            f"""
            WITH parameter_ids as (
                SELECT parameter_id::integer FROM parameter WHERE parameter.name = '{parameter_name}'
            ), source_ids as (
                SELECT DISTINCT source_id::integer FROM signal 
                WHERE signal.parameter_id IN (
                    SELECT parameter_id::integer FROM parameter_ids
                )
            )    
            SELECT source.name from source 
            WHERE source.source_id IN (
                SELECT source_id::integer from source_ids
            );
            """
        )
        return self._query(
            query_str,
            None,
            to_dataframe,
            show_query,
            False,
            COLUMN_MAP["source_from_parameter"],
        )

    def from_project(self, project_name, to_dataframe=True, show_query=False):
        """
        Parameters
        ----------
        project_name:         str, with name of project
        to_dataframe:         bool, specifying whether the query output should be formatted as dataframe
        show_query:           bool, specifying whether to print the query

        Return
        ------
        pd.DataFrame or dict

        Example
        -------
        dp = DataPool()
        dp.project.from_project(source_name="my_project_name")
        """
        query_str = dedent(
            f"""
            WITH project_ids as (
                SELECT project_id::integer FROM project WHERE project.title = '{project_name}'
            )
            SELECT * from source 
            WHERE source.project_id IN (
                SELECT project_id::integer from project_ids
            );
            """
        )
        return self._query(query_str, None, to_dataframe, show_query, False)

    def get_range(self, source_name, to_dataframe=True, show_query=False):
        """Get range of data source

        Parameters
        ----------
        source_name:          str, with name of source
        to_dataframe:         bool, specifying whether the query output should be formatted as dataframe
        show_query:           bool, specifying whether to print the query

        Return
        ------
        pd.DataFrame or dict

        Example
        -------
        dp = DataPool()
        dp.source.get_range(source_name="my_source_name")
        """
        query_str = dedent(
            f"""
            WITH source_ids as (
                SELECT source_id::integer FROM source WHERE source.name = '{source_name}'
            ), signal_group as (
                SELECT distinct signal.timestamp::timestamp FROM signal 
                WHERE signal.source_id IN (
                    SELECT source_id::integer FROM source_ids
                )
            )    
            SELECT DISTINCT MIN(signal_group.timestamp), MAX(signal_group.timestamp) from signal_group
            ORDER BY MIN(signal_group.timestamp), MAX(signal_group.timestamp)
            """
        )
        return self._query(
            query_str,
            None,
            to_dataframe,
            show_query,
            False,
            COLUMN_MAP["source_get_range"],
        )


class SpecialValueDefinition(DataPoolBaseTable):
    __table_name = "special_value_definition"

    @property
    def rows(self):
        return self._rows(self.__table_name)

    @property
    def columns(self):
        return self._columns(self.__table_name)

    def all(self, to_dataframe=True, show_query=False):
        return self._all(self.__table_name, to_dataframe, show_query)

    def from_source_type(self, source_type, to_dataframe=True, show_query=False):
        """
        Parameters
        ----------
        source_type:          str with name of source_type
        to_dataframe:         bool, specifying whether the query output should be formatted as dataframe
        show_query:           bool, specifying whether to print the query

        Return
        ------
        pd.DataFrame or dict

        Example
        -------
        dp = DataPool()
        source_type = "Hach_Flo-Dar"
        dp.special_value_definition.from_source_type(source_type)

        """

        query_str = dedent(
            f"""
            SELECT special_value_definition.numerical_value, special_value_definition.description
            FROM source_type
            LEFT JOIN special_value_definition ON source_type.source_type_id = special_value_definition.source_type_id
            WHERE source_type.name='{source_type}'
            """
        )
        return self._query(
            query_str,
            None,
            to_dataframe,
            show_query,
            False,
            COLUMN_MAP["special_value_definition_from_source_type"],
        )


class SourceType(DataPoolBaseTable):
    __table_name = "source_type"

    @property
    def rows(self):
        return self._rows(self.__table_name)

    @property
    def columns(self):
        return self._columns(self.__table_name)

    def all(self, to_dataframe=True, show_query=False):
        return self._all(self.__table_name, to_dataframe, show_query)

    def from_source(self, source_name, to_dataframe=True, show_query=False):
        """
        Parameters
        ----------
        source_name:          str with name of source
        to_dataframe:         bool, specifying whether the query output should be formatted as dataframe
        show_query:           bool, specifying whether to print the query

        Return
        ------
        pd.DataFrame or Tuples with name of source type

        Example
        -------
        dp = DataPool()
        source_name = "bf_f04_23_bahnhofstr"
        dp.source_type.from_source_name(source_name)

        """

        query_str = dedent(
            f"""
            SELECT source_type.name
            FROM source
            LEFT JOIN source_type ON source.source_type_id = source_type.source_type_id
            WHERE source.name='{source_name}'
            """
        )
        res = self._query(
            query_str,
            None,
            to_dataframe,
            show_query,
            False,
            COLUMN_MAP["source_type_from_source"],
        )

        if isinstance(res, _DataFrame):
            res = res.iloc[0, 0]

        return res


class Signal(DataPoolBaseTable):
    __table_name = "signal"

    @staticmethod
    def _all():
        raise PermissionError("This method is not available for the signal table!")

    @property
    def rows(self):
        return self._rows(self.__table_name)

    @property
    def columns(self):
        return self._columns(self.__table_name)

    def get_id(
            self,
            source_name,
            parameter_name,
            start="1900-01-01 00:00:00",
            end=None,
            to_dataframe=True,
            show_query=False,
    ):
        """
        Parameters
        ----------
        source_name:          str with name of the source
        parameter_name:       str with name of the parameter
        start:                str, specifying a datetime ideally in the format yyyy-mm-dd HH:MM:SS
        end:                  str, specifying a datetime ideally in the format yyyy-mm-dd HH:MM:SS
        to_dataframe:         bool, specifying whether the query output should be formatted as dataframe
        show_query:           bool, specifying whether to print the query

        Return
        ------
        pd.DataFrame or Tuples with Timestamp and ID

        Example
        -------
        dp = DataPool()
        source_name = "bf_f04_23_bahnhofstr"
        parameter_name =
        start = "2016-06-22 12"        # zeros are automatically added

        # if end is not specified, all data up to now will be returned
        dp.signal.get_id(source_name,parameter_name,start)

        """

        if end is None:
            end = _dt.now().strftime("%Y-%m-%d %H:%M:%S")

        st, en = parse_dates(start, end)

        query_str = dedent(
            f"""
            SELECT signal.timestamp, signal_id
            FROM signal 
            INNER JOIN parameter ON signal.parameter_id = parameter.parameter_id
            INNER JOIN source ON signal.source_id = source.source_id
            WHERE parameter.name = '{parameter_name}' AND
            source.name = '{source_name}' AND
            '{st}'::timestamp <= signal.timestamp AND
            signal.timestamp <= '{en}'::timestamp
            ORDER BY signal.timestamp ASC
            """
        )
        return self._query(
            query_str,
            None,
            to_dataframe,
            show_query,
            False,
            COLUMN_MAP["signal_get_if"],
        )

    def get(
            self,
            *,
            source_name=None,
            site_name=None,
            parameter_name=None,
            source_type_name=None,
            start="1900-01-01 00:00:00",
            end=None,
            without_flags=True,
            minimal=False,
            to_dataframe=True,
            show_query=False,
    ):
        """Arguments must be provided with keywords!

        Parameters
        ----------
        source_name:          str, of the source name
        site_name:            str, of the site name
        parameter_name:       str, of the parameter name
        source_type_name:     str, of the source type name
        start:                str, specifying a datetime ideally in the format yyyy-mm-dd HH:MM:SS
        end:                  str, specifying a datetime ideally in the format yyyy-mm-dd HH:MM:SS
        without_flags:        bool, specifying if quality data should be retrieved. if fast, it will not be.
        minimal:              bool, if True, output of "source.name", "source.serial", "source_type.name", "site.name"will be skipped.
        to_dataframe:         bool, specifying whether the query output should be formatted as dataframe
        show_query:           bool, specifying whether to print the query

        Return
        ------
        pd.DataFrame or Tuples containing the signals

        Example
        -------
        from datapool_client import DataPool

        dp = DataPool() # this only works when a default connection has been set!
        df = dp.signal.get(source_name = "bn_r03_rub_morg", parameter_name="bucket content",
                           source_type_name="OttPluvioII",site_name="school_chatzenrainstr",start = "2019-11-28")
        """
        if (
                (source_name is None)
                and (site_name is None)
                and (parameter_name is None)
                and (source_type_name is None)
        ):
            raise ValueError(
                "Please pass a filter! Choose at least one of those 'source_name', 'site_name', "
                "'source_type_name', 'parameter_name'."
            )

        if end is None:
            end = _dt.now().strftime("%Y-%m-%d %H:%M:%S")

        st, en = parse_dates(start, end)

        source_with = "\n"
        source_filter = "\n"
        site_with = "\n"
        site_filter = "\n"
        parameter_with = "\n"
        parameter_filter = "\n"

        if source_type_name is None:

            if source_name is not None:
                source_with = (
                    "source_ids AS (SELECT source_id::integer FROM source "
                    f"WHERE source.name = '{source_name}')"
                )
                source_filter = "signal.source_id = ANY(ARRAY(SELECT source_id::integer FROM source_ids))"

        else:

            if source_type_name is not None:
                source_with = (
                    "source_ids AS ("
                    "WITH source_type_ids AS (SELECT source_type_id::integer FROM source_type "
                    f"WHERE source_type.name = '{source_type_name}')"
                    "SELECT source_id::integer FROM source WHERE source_type_id ="
                    "ANY(ARRAY(SELECT source_type_id::integer FROM source_type_ids))"
                    ")"
                )

                source_filter = "signal.source_id = ANY(ARRAY(SELECT source_id::integer FROM source_ids))"

        if site_name is not None:
            site_with = (
                "site_ids AS (SELECT site_id::integer FROM site "
                f"WHERE site.name = '{site_name}')"
            )
            site_filter = (
                "signal.site_id = ANY(ARRAY(SELECT site_id::integer FROM site_ids))"
            )

        if parameter_name is not None:
            if not isinstance(parameter_name, list):
                parameter_name = [parameter_name]

            parameters = f"""'{{"{'","'.join(parameter_name)}"}}'"""

            parameter_with = (
                "parameter_ids AS (SELECT parameter_id::integer FROM parameter "
                f"WHERE parameter.name = ANY({parameters}))"
            )
            parameter_filter = "signal.parameter_id = ANY(ARRAY(SELECT parameter_id::integer FROM parameter_ids))"

        with_statement = ",\n".join(
            [
                statement
                for statement in [source_with, site_with, parameter_with]
                if statement != "\n"
            ]
        )
        filter_statement = "AND\n".join(
            [
                statement
                for statement in [source_filter, site_filter, parameter_filter]
                if statement != "\n"
            ]
        )

        query_str = dedent(
            f"""
            WITH 
            {with_statement}
            SELECT signal.timestamp, value, parameter.unit, parameter.name, source.name, source.serial, source_type.name, site.name, quality.method, quality.flag  
                FROM signal
                INNER JOIN site ON signal.site_id = site.site_id
                INNER JOIN parameter ON signal.parameter_id = parameter.parameter_id
                INNER JOIN source ON signal.source_id = source.source_id
                INNER JOIN source_type ON source.source_type_id = source_type.source_type_id
                LEFT JOIN signals_signal_quality_association ON signals_signal_quality_association.signal_id = signal.signal_id
                LEFT JOIN signal_quality ON signals_signal_quality_association.signal_quality_id = signal_quality.signal_quality_id
                LEFT JOIN quality ON quality.quality_id = signal_quality.quality_id
                WHERE 
                {filter_statement}
                AND '{st}'::timestamp <= signal.timestamp
                AND signal.timestamp <= '{en}'::timestamp 

                ORDER BY signal.timestamp ASC
            """
        )

        columns = COLUMN_MAP["signal_get_with_quality"]

        if without_flags and not minimal:
            to_replace = {
                ", quality.method, quality.flag": "",
                "LEFT JOIN signals_signal_quality_association ON signals_signal_quality_association.signal_id = signal.signal_id": "",
                "LEFT JOIN signal_quality ON signals_signal_quality_association.signal_quality_id = signal_quality.signal_quality_id": "",
                "LEFT JOIN quality ON quality.quality_id = signal_quality.quality_id": "",
            }

            query_str = dedent(replace_in_query(query_str, to_replace))
            columns = COLUMN_MAP["signal_get_without_quality"]

        elif minimal and not without_flags:
            to_replace = {", source.name, source.serial, source_type.name, site.name": ""}
            query_str = dedent(replace_in_query(query_str, to_replace))

            columns = COLUMN_MAP["signal_get_minimal"]

        elif minimal and without_flags:
            to_replace = {
                ", quality.method, quality.flag": "",
                ", source.name, source.serial, source_type.name, site.name": "",
                "LEFT JOIN signals_signal_quality_association ON signals_signal_quality_association.signal_id = signal.signal_id": "",
                "LEFT JOIN signal_quality ON signals_signal_quality_association.signal_quality_id = signal_quality.signal_quality_id": "",
                "LEFT JOIN quality ON quality.quality_id = signal_quality.quality_id": "",
            }
            query_str = dedent(replace_in_query(query_str, to_replace))
            columns = COLUMN_MAP["signal_get_without_quality_and_minimal"]

        return self._query(query_str, None, to_dataframe, show_query, False, columns)

    def newest(self, n, to_dataframe=True, show_query=False):
        """
        Parameters
        ----------
        n:                    int, specifying number of signals
        to_dataframe:         bool, specifying whether the query output should be formatted as dataframe
        show_query:           bool, specifying whether to print the query

        Return
        ------
        pd.DataFrame or Tuples containing the signals

        Example
        -------
        dp = DataPool()
        dp.signal.newest(4)
        """

        query_str = dedent(
            f"""
            SELECT  timestamp, 
                    value, 
                    unit, 
                    parameter_name, 
                    source_name, 
                    source_serial, 
                    source_type_name
            FROM     ( 
                                SELECT     signal.signal_id AS signal_id, 
                                        signal.timestamp, 
                                        value, 
                                        unit, 
                                        parameter.NAME                          AS parameter_name, 
                                        source.NAME                             AS source_name, 
                                        source.serial                           AS source_serial, 
                                        source_type.NAME                        AS source_type_name
                                FROM       signal 
                                INNER JOIN site 
                                ON         signal.site_id = site.site_id 
                                INNER JOIN parameter 
                                ON         signal.parameter_id = parameter.parameter_id 
                                INNER JOIN source 
                                ON         signal.source_id = source.source_id 
                                INNER JOIN source_type 
                                ON         source.source_type_id = source_type.source_type_id 
                                WHERE      timestamp >= (Now() - interval '1 week') 
                                ORDER BY   signal.timestamp DESC limit '{n}' ) s 
            GROUP BY s.timestamp, 
                    s.value, 
                    s.signal_id, 
                    s.parameter_name, 
                    s.unit, 
                    s.source_type_name, 
                    s.source_name, 
                    s.source_serial
            """
        )

        return self._query(
            query_str,
            None,
            to_dataframe,
            show_query,
            False,
            COLUMN_MAP["signal_newest"],
        )

    def last(self, to_dataframe=True, show_query=False):
        """
        Parameters
        ----------
        recent_on_top:        bool, specifying the temporal order (only if to_dataframe = True)
        to_dataframe:         bool, specifying whether the query output should be formatted as dataframe
        show_query:           bool, specifying whether to print the query

        Return
        ------
        pd.DataFrame or Tuples containing the signals

        Example
        -------
        dp = DataPool()
        dp.signal.last()

        """

        query_str = dedent(
            """
            SELECT source.name, MAX(signal.timestamp)
            FROM signal 
            INNER JOIN source ON signal.source_id = source.source_id
            GROUP BY source.name
            ORDER BY MAX(signal.timestamp) ASC"""
        )
        return self._query(
            query_str, None, to_dataframe, show_query, False, COLUMN_MAP["signal_last"]
        )

    def from_site_with_rangecheck(
            self,
            site_name,
            start="1900-01-01 00:00:00",
            end=None,
            to_dataframe=True,
            show_query=False,
    ):
        """
        Parameters
        ----------
        site_name:            str of the site name
        start:                str, specifying a datetime ideally in the format yyyy-mm-dd HH:MM:SS
        end:                  str, specifying a datetime ideally in the format yyyy-mm-dd HH:MM:SS
        to_dataframe:         bool, specifying whether the query output should be formatted as dataframe
        show_query:           bool, specifying whether to print the query

        Return
        ------
        pd.DataFrame or Tuples containing the signals

        Example
        -------
        site = "23_bahnhofstr"
        start = "2016-06-22 12"        # zeros are automatically added
        end = "2016-06-22 12:10:00"
        dp = DataPool()
        dp.signal.from_site_with_rangecheck(site, start, end)
        """

        if end is None:
            end = _dt.now().strftime("%Y-%m-%d %H:%M:%S")

        st, en = parse_dates(start, end)

        query_str = dedent(
            f"""
            WITH signals AS 
            ( 
                    SELECT     signal_id::INTEGER 
                    FROM       signal 
                    inner join site 
                    ON         signal.site_id = site.site_id 
                    WHERE      site.name = '{site_name}' 
                    AND        '{st}' :: timestamp <= signal.timestamp 
                    AND        signal.timestamp <= '{en}' :: timestamp
            ), 
    
            signals_with_quality_1 AS 
            ( 
                SELECT signal_id 
                FROM   signals_signal_quality_association 
                join   signal_quality 
                ON     signals_signal_quality_association.signal_quality_id = signal_quality.signal_quality_id
                WHERE  quality_id = 1 
                AND    signals_signal_quality_association.signal_id = ANY( array(select signal_id::INTEGER FROM signals))
            ) 
            SELECT   timestamp, 
                    value, 
                    unit, 
                    parameter.name, 
                    source_type.name, 
                    source.name 
            FROM     signal 
            join     parameter 
            ON       signal.parameter_id = parameter.parameter_id 
            join     source 
            ON       signal.source_id = source.source_id 
            join     source_type 
            ON       source.source_type_id = source_type.source_type_id 
            WHERE    signal_id = ANY(ARRAY(SELECT signal_id::INTEGER FROM signals_with_quality_1)) 
    
            ORDER BY timestamp ASC
            """
        )
        return self._query(
            query_str,
            None,
            to_dataframe,
            show_query,
            False,
            COLUMN_MAP["signal_from_site_with_rangecheck"],
        )


class Quality(DataPoolBaseTable):
    __table_name = "quality"

    def __init__(
            self,
            host=None,
            port=None,
            database=None,
            user=None,
            password=None,
            instance=None,
            check=True,
            to_replace={},
            verbose=True,
    ):
        super().__init__(
            host, port, database, user, password, instance, check, to_replace, verbose
        )

        self.__signal = Signal(
            host, port, database, user, password, instance, False, to_replace, verbose
        )

    @property
    def rows(self):
        return self._rows(self.__table_name)

    @property
    def columns(self):
        return self._columns(self.__table_name)

    def all(self, to_dataframe=True, show_query=False):
        return self._all(self.__table_name, to_dataframe, show_query)

    @property
    def methods(self):
        query_str = "SELECT DISTINCT method FROM quality"
        res = self._query(query_str, None, True, False, False, ["method"])
        return res.method.tolist()

    def get_flags(self, to_dataframe=True, show_query=False):
        """
        Parameters
        ----------
        to_dataframe:         bool, specifying whether the query output should be formatted as dataframe
        show_query:           bool, specifying whether to print the query

        Return
        ------
        pd.DataFrame or Tuples of all flags and methods

        Example
        -------
        dp = DataPool()
        dp.signal.get_flags()

        """

        query_str = "SELECT DISTINCT method, flag FROM quality"
        return self._query(
            query_str, None, to_dataframe, show_query, False, ["method", "flag"]
        )

    def __get_quality_id(self):

        query = dedent(
            f"""
            SELECT quality_id, flag
            FROM quality 
            WHERE method = '{self.method}' 
            """
        )

        self.quality_id = self._query(query)

        if self.quality_id.empty:
            raise ValueError("No matching quality found!")

        self.flagset = set(self.flags)
        dbflagset = set(self.quality_id.flag)

        if (self.flagset - dbflagset) != set():
            raise ValueError(
                "Uninitialized flag found! Associate flag with method by applying the add.quality.method() function."
            )

        return

    def __get_signal_quality_id(self):

        self.timestamp = _datetime.now().strftime("%Y-%m-%d")

        query = dedent(
            f"""SELECT signal_quality_id, signal_quality.quality_id, timestamp, author
            FROM signal_quality
            INNER JOIN quality ON signal_quality.quality_id = quality.quality_id                 
            WHERE timestamp = '{self.timestamp}' AND
            author = '{self.author}' AND
            method = '{self.method}'"""
        )

        self.signal_quality_id = self._query(query)

        if self.signal_quality_id.empty:

            self.add_signal_quality_id = True

            max_id = self._query("SELECT MAX(signal_quality_id) FROM signal_quality")
            if max_id.empty:
                max_id = 0

            else:
                max_id = max_id.iloc[0, 0]

            self.signal_quality_id = _DataFrame()
            self.signal_quality_id["signal_quality_id"] = _arange(
                max_id + 1, max_id + self.quality_id.flag.shape[0] + 1
            )
            self.signal_quality_id["quality_id"] = self.quality_id.quality_id.astype(
                int
            )
            self.signal_quality_id["timestamp"] = self.timestamp
            self.signal_quality_id["author"] = self.author

    def get_signal_quality_id(self, method_name):
        query = dedent(
            f"""SELECT signal_quality_id, signal_quality.quality_id, timestamp, author
            FROM signal_quality
            INNER JOIN quality ON signal_quality.quality_id = quality.quality_id                 
            WHERE method = '{method_name}'"""
        )
        return self.query(query)

    def __match_id_2_flag(self):

        match_quality_id_2_flag = _zeros(len(self.flags))

        sig_qual_id = self.signal_quality_id.signal_quality_id.values

        for flag in self.flagset:
            match_quality_id_2_flag[self.flags == flag] = sig_qual_id[
                self.quality_id.flag == flag
                ]

        signal_df = self.__signal.get_id(
            self.source, self.parameter, self.start, self.end
        )

        self.signals_signal_quality_association = _DataFrame(
            {
                "signal_id": signal_df.id.values.astype(int),
                "signal_quality_id": match_quality_id_2_flag.astype(int),
            }
        )

        return

    def __add_2_db(self):

        answer = "y"
        if self.ask:
            answer = input("Add signal quality? Add flag quality? [Y/n] ")
            answer = "y" if answer == "" else answer

        if answer.lower() in ["y", "yes"]:

            self._connect()
            con = self._conn

            if self.add_signal_quality_id:
                self.signal_quality_id.to_sql(
                    "signal_quality", con, if_exists="append", index=False
                )
                print("signal quality added")

            try:
                self.signals_signal_quality_association.to_sql(
                    "signals_signal_quality_association",
                    con,
                    if_exists="append",
                    index=False,
                )
                print("flag added")
            except Exception as e:
                print("flags weren't added!")
                print(e)

            con.close()

        else:

            print("aborted.")

        return

    def add_quality_flag(
            self, source, parameter, method, flags, start, end, author, ask=True
    ):
        """

        Parameters
        ----------
        source : str
            specifying the source your adding flags for

        parameter: str
            specifying the parameter your adding flags for

        method : str
            specifying the quality method your adding the flags for

        flags : list or array
            containing the flags you want to add

        start : str
            specifying when to start to add the flags

        end : str
            specifying when to stop to add the flags

        author : str
            stating who is adding the flags

        ask : bool
            specifying whether a user input is necessary

        Returns
        -------

        """

        self.source = source
        self.parameter = parameter
        self.method = method
        self.flags = _array(flags)
        self.start, self.end = parse_dates(start, end)
        self.author = author
        self.ask = ask

        self.add_signal_quality_id = False

        self.__get_quality_id()
        self.__get_signal_quality_id()
        self.__match_id_2_flag()
        self.__add_2_db()

        return

    def add_quality_method(self, method, flags, ask=True, add_flag_only=False):
        """

        Parameters
        ----------
        method : str
            specifying the quality method your adding (the flags for)

        flags : list
            containing the flags you want to add

        ask : bool
            specifying whether a user input is necessary

        add_flag_only : bool
            switches to the adding flags to a existing method mode

        Returns
        -------
        None

        """

        quality_df = self._query("SELECT flag, method FROM quality")

        if not add_flag_only:
            if not "green" in flags:
                raise ValueError("global default flag 'green' missing!")

        if len(set(flags)) != len(flags):
            raise ValueError("set unique quality flags!")

        max_id = self._query("SELECT MAX(quality_id) FROM quality")
        if max_id.empty:
            max_id = 0
        else:
            max_id = max_id.iloc[0, 0]

        self.quality_df = _DataFrame()
        self.quality_df["quality_id"] = _arange(max_id + 1, max_id + len(flags) + 1)
        self.quality_df["flag"] = flags
        self.quality_df["method"] = method

        if not _merge(
                self.quality_df[["flag", "method"]],
                quality_df[["flag", "method"]],
                how="inner",
        ).empty:
            raise ValueError("This combination on method and flag already exists!")

        answer = "y"
        if ask:
            answer = input("Add quality {flags} to {method}? [Y/n] ".format(**locals()))
            answer = "y" if answer == "" else answer

        if answer.lower() in ["y", "yes"]:

            self._connect()
            con = self._conn

            self.quality_df.to_sql("quality", con, if_exists="append", index=False)

            con.close()

        else:
            print("aborted")

        return

    def _delete_from_quality_table(self, id_list):
        """

        Parameters
        ----------
        id_list : list
            a list containing the id's of the lines to remove from the table

        Returns
        -------
        None

        """

        for i in id_list:
            query = dedent(
                f"""
                DELETE FROM quality
                WHERE quality.quality_id = '{i}'
                """
            )
            self.query(query, allow_modifications=True)

    def delete_quality_flags(self, method, source, parameter, start, end):
        """
        This function deletes the signals_signal_quality_association entry of signal_id and signal_quality_id.
        :param method str with method name
        :param source str with source name
        :param parameter str with parameter name
        :param start datetime with start date
        :param end datetime with end date
        :return:
        """

        s_ids = tuple(
            self.__signal.get_id(
                source_name=source, parameter_name=parameter, start=start, end=end
            )["id"].to_list()
        )

        if s_ids == ():

            print("Nothing to delete")

        else:

            query = """SELECT signal_quality.signal_quality_id, signals_signal_quality_association.signal_id FROM signals_signal_quality_association
                    INNER JOIN signal_quality ON signals_signal_quality_association.signal_quality_id = signal_quality.signal_quality_id
                    INNER JOIN quality ON signal_quality.quality_id = quality.quality_id
                    WHERE quality.method = '{}' AND signal_id IN ({})""".format(
                method, ",".join([str(i) for i in s_ids])
            )

            candidates_list = list(self.query(query, allow_modifications=True))[1]

            if candidates_list == []:

                print("Nothing to delete")

            else:
                for i in range(len(candidates_list)):
                    query_delete = """DELETE FROM signals_signal_quality_association WHERE signal_quality_id = '{}' AND signal_id = '{}'""".format(
                        candidates_list[i][0], candidates_list[i][1]
                    )
                    self.query(query_delete, allow_modifications=True)

                print("Entries deleted")

        return

    def get_unflagged_signals(self, source_name, parameter_name):
        """Creates list of unflagged signal ids.

            Args:
              source_name: name of the source to look for gaps in flagging.
        parameter_name: name of the parameter to look for gaps in flagging.

            Returns:
              List of signal id without any flag.
        """

        query = dedent(
            f"""
            SELECT 
                signal.signal_id, 
                signal.timestamp, 
                signal.value, 
                quality.method, 
                quality.flag 
            FROM signal
            
            INNER JOIN parameter 
            ON parameter.parameter_id = signal.parameter_id
            INNER JOIN source 
            ON source.source_id = signal.source_id
            FULL OUTER JOIN signals_signal_quality_association 
            ON signals_signal_quality_association.signal_id = signal.signal_id
            FULL OUTER JOIN signal_quality 
            ON signals_signal_quality_association.signal_quality_id = signal_quality.signal_quality_id
            FULL OUTER JOIN quality 
            ON quality.quality_id = signal_quality.quality_id
            
            WHERE source.name = '{source_name}'
            AND parameter.name = '{parameter_name}'
            AND quality.flag IS NULL
            
            ORDER BY signal.timestamp ASC;
            """
        )
        return self.query_df(query)
