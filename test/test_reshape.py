from datapool_client import reshape


def test_reshape(setup_postgres, dp):
    df = dp.signal.get(source_name="source_1_2")
    reshape(df)


def test_reshape_with_flags(setup_postgres, dp):
    df = dp.signal.get(source_name="source_1_2", fast=False)
    reshape(df, only_values=False)