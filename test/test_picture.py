def test_all(setup_postgres, dp):
    dp.picture.all()


def test_all_with_data_column(setup_postgres, dp):
    dp.picture.all_with_data_column()
