import pytest

from datapool_client.core.config import read_config, set_defaults


def test_set_defaults_raises(tmp_path):
    config_file = tmp_path / "config.ini"

    with pytest.raises(ConnectionError):
        set_defaults(
            host="", port="", user="", database="", password="", filepath=config_file
        )


def test_set_defaults_and_overwrite(tmp_path):
    config_file = tmp_path / "config.ini"

    conn_details = dict(
        host="host",
        port="port",
        user="user",
        database="dbname",
        password="password",
    )

    # initial setting defaults
    set_defaults(**conn_details, filepath=config_file, test_conn=False)

    assert sorted(dict(read_config(config_file)["DEFAULT"])) == sorted(conn_details)

    # no overwrite password
    with pytest.raises(ValueError):
        set_defaults(**conn_details, filepath=config_file, test_conn=False)

    # overwrite defaults
    set_defaults(**conn_details, filepath=config_file, test_conn=False, overwrite=True)

    # save another instance
    set_defaults(
        **conn_details,
        instance="OTHER",
        filepath=config_file,
        test_conn=False,
    )

    assert sorted(dict(read_config(config_file)["OTHER"])) == sorted(conn_details)

    with pytest.raises(ValueError):
        set_defaults(
            **conn_details,
            instance="OTHER",
            filepath=config_file,
            test_conn=False,
        )


def test_set_defaults_too_few_args(tmp_path):
    config_file = tmp_path / "config.ini"

    with pytest.raises(TypeError):
        set_defaults(host="", port="", filepath=config_file)
