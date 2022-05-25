import matplotlib
import pytest

matplotlib.use("Agg")


def test_all_fails(setup_postgres, dp):
    with pytest.raises(PermissionError):
        dp.signal._all()


def test_from_site_with_rangecheck(setup_postgres, dp):
    dp.signal.from_site_with_rangecheck(site_name="site_1")


def test_last(setup_postgres, dp):
    dp.signal.last()


def test_newest(setup_postgres, dp):
    dp.signal.newest(2)


def test_get(setup_postgres, dp):
    dp.signal.get(source_name="source_1_1")
    dp.signal.get(parameter_name="parameter_1")
    dp.signal.get(site_name="site_1")
    dp.signal.get(source_type_name="source_type_1")
    dp.signal.get(source_name="source_1_1", parameter_name="parameter_1")
    dp.signal.get(
        source_name="source_1_1", parameter_name="parameter_1", site_name="site_1"
    )
    dp.signal.get(
        source_name="source_1_1",
        parameter_name="parameter_1",
        site_name="site_1",
        source_type_name="source_type_1",
    )


def test_get_fast(setup_postgres, dp):
    ans1 = dp.signal.get(source_name="source_1_1", without_flags=False, minimal=False)
    ans2 = dp.signal.get(source_name="source_1_1", without_flags=False, minimal=True)
    ans3 = dp.signal.get(source_name="source_1_1", without_flags=True, minimal=True)
    ans4 = dp.signal.get(source_name="source_1_1", without_flags=True, minimal=False)



def test_get_raw(setup_postgres, dp):
    dp.signal.get(source_name="source_1_1", to_dataframe=False)
    dp.signal.get(parameter_name="parameter_1", to_dataframe=False)
    dp.signal.get(site_name="site_1", to_dataframe=False)
    dp.signal.get(source_type_name="source_type_1", to_dataframe=False)
    dp.signal.get(
        source_name="source_1_1", parameter_name="parameter_1", to_dataframe=False
    )
    dp.signal.get(
        source_name="source_1_1",
        parameter_name="parameter_1",
        site_name="site_1",
        to_dataframe=False,
    )
    dp.signal.get(
        source_name="source_1_1",
        parameter_name="parameter_1",
        site_name="site_1",
        source_type_name="source_type_1",
        to_dataframe=False,
    )


def test_get_id(setup_postgres, dp):
    with pytest.raises(TypeError):
        dp.signal.get_id(parameter_name="parameter_1")
        dp.signal.get_id(source_name="source_1_1")
        dp.signal.get_id(start="2021-01-01")
        dp.signal.get_id(start="2021-01-01", end="2021-12-01")
    dp.signal.get_id(source_name="source_1_1", parameter_name="parameter_1")
    dp.signal.get_id(
        source_name="source_1_1",
        parameter_name="parameter_1",
        start="2021-01-01",
        end="2021-12-01",
    )
