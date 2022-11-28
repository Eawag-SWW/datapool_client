import json
from typing import Union

import pandas as pd


def regroup_list_of_dicts(additional_info: Union[dict, list]):
    info = additional_info.copy()
    m = 0
    while m < len(info):
        n = 1
        while n < len(info):
            if not (set(info[m]) & set(info[n])):
                info[m].update(info[n])
                info.pop(n)
                n = 1

            n += 1
        m += 1
    return info


def _format_meta_data_dict(meta_data_dict: dict):
    """dict from dataframe record style (list of row dicts)"""
    new = []
    for i, _row in enumerate(meta_data_dict):
        row = _row.copy()
        ami = row.pop("additional_meta_info")
        if pd.isna(ami):
            new.append(row)
        else:
            if isinstance(ami, str):
                ami = json.loads(ami)

            if isinstance(ami, dict):
                row.update(ami)
                new.append(row)
            else:
                if len(ami) > 1:
                    ami = regroup_list_of_dicts(ami)
                for entry in ami:
                    row2 = row.copy()
                    row2.update(entry)
                    new.append(row2)

    return new


def format_meta_data(meta_data_dataframe: pd.DataFrame):
    formatted = _format_meta_data_dict(meta_data_dataframe.to_dict(orient="records"))
    return pd.DataFrame(formatted)


def reshape(dataframe: pd.DataFrame, only_values=True):
    """Reshapes data returned by DataPool.signal.get query.

    Parameters
    ----------
    dataframe:
        pd.DataFrame, to be reshaped.
    only_values:
        bool, omitting the quality information on reshape.

    Return
    ------
    pd.DataFrame
    """
    dataframe.copy()
    if only_values:
        df = pd.pivot_table(
            dataframe, values=["value"], index="timestamp", columns="variable"
        )
        df.columns = df.columns.droplevel()
        df.columns.name = ""
        return df
    else:
        values = pd.pivot_table(
            dataframe, values="value", index="timestamp", columns="variable"
        )

        flags = pd.pivot_table(
            dataframe,
            values="quality_flag",
            index="timestamp",
            columns="quality_method",
            aggfunc=lambda x: " ".join(str(v) for v in x),
        )
        res = pd.concat([values, flags], axis=1)
        return res


def reshape_full_site_query(data):

    data = pd.pivot_table(
        data,
        values=["site_field_value"],
        index=["name", "description"],
        columns="site_field",
        aggfunc=lambda x: x,
    )
    data.columns = data.columns.droplevel()
    data.columns.name = ""
    return data.reset_index()
