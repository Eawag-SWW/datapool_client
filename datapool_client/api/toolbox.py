from datapool_client.core.abstractions import Connector


class ToolBox(Connector):
    def value_in_column_of_table(self, table, column, value, show_query=False):
        """
        Parameters
        ----------
        table:         str, table to check in
        column:        str, column of table to check in
        value:         str, value to search in column of table
        show_query:    bool, specifying whether to print the query

        Return
        ------
        bool
        """
        query = (
            f"SELECT exists (SELECT 1 FROM {table} WHERE {column} = '{value}' LIMIT 1);"
        )
        result = self._query(query, to_dataframe=False, show_query=show_query)
        return result["data"][0][0]

    def count_values_in_db_group_by_source_and_parameter(self, to_dataframe=True, show_query=False):
        """
        Parameters
        ----------
        to_dataframe:         bool, specifying whether the query output should be formatted as dataframe
        show_query:           bool, specifying whether to print the query

        Return
        ------
        pd.DataFrame or dict

        Example
        -------
        tb = Toolbox()
        tb.count_values_in_db_group_by_source_and_parameter()
        """
        query = """
        WITH count_table AS (
            SELECT
                count(parameter_id),
                parameter_id,
                source_id,
                date_trunc('week', timestamp)
            FROM signal
            GROUP BY
                date_trunc('week', timestamp),
                parameter_id,
                source_id
        )
        SELECT
            count_table.count AS value_count,
            parameter.name AS parameter_name,
            source.name AS source_name,
            count_table.date_trunc AS date_trunc
        FROM count_table
        INNER JOIN parameter ON parameter.parameter_id = count_table.parameter_id
        INNER JOIN source ON source.source_id = count_table.source_id
        order by date_trunc desc;
        """
        return self._query(
            query,
            to_dataframe=to_dataframe,
            show_query=show_query
        )
