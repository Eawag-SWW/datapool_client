def test_all(setup_postgres, dp):
    dp.source_type.all()


def test_from_source_name(setup_postgres, dp):
    dp.source_type.from_source("source_1_1")
