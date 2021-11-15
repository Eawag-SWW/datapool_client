def test_meta_action_type_all(setup_postgres, dp):
    dp.meta_action_type.all()


def test_meta_log_type_all(setup_postgres, dp):
    dp.meta_log_type.all()


def test_meta_flag_all(setup_postgres, dp):
    dp.meta_flag.all()


def test_meta_data_all(setup_postgres, dp):
    dp.meta_data.all()


def test_meta_data_history_all(setup_postgres, dp):
    dp.meta_data_history.all()


def test_meta_data_get(setup_postgres, dp):
    dp.meta_data.get(source_name="source_1_1")
    dp.meta_data.get(site_name="site_1")
    dp.meta_data.get(source_name="source_1_1", site_name="site_1")


def test_meta_data_history_get(setup_postgres, dp):
    dp.meta_data_history.get(source_name="source_1_1")
    dp.meta_data_history.get(site_name="site_1")
    dp.meta_data_history.get(source_name="source_1_1", site_name="site_1")
    dp.meta_data_history.get(
        source_name="source_1_1", site_name="site_1", start="2021-01-01"
    )
    dp.meta_data_history.get(
        source_name="source_1_1", site_name="site_1", end="2021-12-01"
    )
    dp.meta_data_history.get(
        source_name="source_1_1",
        site_name="site_1",
        start="2021-01-01",
        end="2021-12-01",
    )
