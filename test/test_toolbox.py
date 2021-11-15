def test_value_in_column_of_table(setup_postgres, toolbox):
    assert toolbox.value_in_column_of_table("source", "name", "source_1_1")


def test_count_values_in_db_group_by_source_and_parameter(setup_postgres, toolbox):
    toolbox.count_values_in_db_group_by_source_and_parameter()