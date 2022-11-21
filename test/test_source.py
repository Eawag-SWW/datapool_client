def test_all(setup_postgres, dp):
    dp.source.all()


def test_get_range(setup_postgres, dp):
    dp.source.get_range("source_1_1")


def test_from_variable(setup_postgres, dp):
    dp.source.from_variable("variable_1")


def test_from_project(setup_postgres, dp):
    dp.source.from_project("project_2")
