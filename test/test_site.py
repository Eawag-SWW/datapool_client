def test_all(setup_postgres, dp):
    dp.site.all()


def test_img_count(setup_postgres, dp):
    dp.site.get_img_count()


def test_from_source(setup_postgres, dp):
    dp.site.from_source("source_1_1")
