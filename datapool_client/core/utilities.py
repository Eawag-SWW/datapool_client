from datetime import datetime as _dt

from datapool_client import CONFIG_PATH
from datapool_client.core.column_map import COLUMN_MAP
from datapool_client.core.config import _import_defaults


def choose_arguments_connection_arguments(filepath=CONFIG_PATH, **kwargs):
    instance = kwargs.pop("instance")
    conn_args = [arg is None for arg in kwargs.values()]

    if all(conn_args) and instance is None:
        # read default args
        conn_details = _import_defaults(filepath=filepath)

    elif any(conn_args):
        if instance is None:
            raise ValueError(
                "Please pass all connection details or set a default connection!"
            )
        else:
            conn_details = _import_defaults(instance, filepath=filepath)
    else:
        conn_details = kwargs

    return conn_details


def parse_date_formats(
    date_str,
    possible_formats=("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d %H", "%Y-%m-%d"),
):
    for form in possible_formats:
        try:
            datetime = _dt.strptime(date_str, form)
            return datetime
        except ValueError:
            pass


def parse_dates(start, end):
    """
    uses the __handle_formats function to check input dates...
    """

    st = parse_date_formats(start)
    en = parse_date_formats(end)

    assert st < en, "The start datetime must be smaller than the end datetime."
    return st.strftime("%Y-%m-%d %H:%M:%S"), en.strftime("%Y-%m-%d %H:%M:%S")


def replace_in_query(query, items_to_replace):
    for old_term, new_term in items_to_replace.items():
        query = query.replace(old_term, new_term)
    return query


def clean_query_string(query_str, to_replace):
    query_str = query_str.rstrip()

    if not query_str.endswith(";"):
        query_str = query_str + ";"

    return replace_in_query(query_str, to_replace)


def determine_additional_meta_info_columns_of_meta_data_history(meta_columns):
    meta_cols = COLUMN_MAP["meta_data_history_get"]
    return list(set(meta_columns) - set(meta_cols))
