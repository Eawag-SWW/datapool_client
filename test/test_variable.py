def test_all(setup_postgres, dp):
    dp.variable.all()


def test_from_source_type(setup_postgres, dp):
    dp.variable.from_source_type("source_type_1")


def test_from_all_sources(setup_postgres, dp):
    dp.variable.from_all_sources()
