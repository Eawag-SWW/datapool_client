import pandas as pd
import pytest


def test_dp_query(setup_postgres, dp):
    data = dp.query("Select * from source;")
    assert isinstance(data, dict)

    with pytest.raises(AttributeError):
        dp.signal.query("Select * from source;")


def test_dp_query_df(setup_postgres, dp):
    data = dp.query_df("Select * from source;")
    isinstance(data, pd.DataFrame)

    with pytest.raises(AttributeError):
        dp.signal.query_df("Select * from source;")
