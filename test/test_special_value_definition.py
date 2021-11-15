def test_all(setup_postgres, dp):
    dp.special_value_definition.all()


def test_from_source_type(setup_postgres, dp):
    dp.special_value_definition.from_source_type("source_type_1")
