import configparser as _configparser
import os as _os

from sqlalchemy import create_engine

from datapool_client import CONFIG_PATH


DEFAULT_INSTANCE_NAME = "DEFAULT"


def read_config(config_file_path):
    config = _configparser.ConfigParser()
    config.read(config_file_path)
    return config


def write_config(config_file_path, config):
    with open(config_file_path, "w") as ncf:
        config.write(ncf)
    return


def _import_defaults(instance=None, filepath=CONFIG_PATH):

    if not _os.path.exists(filepath):
        raise ValueError(
            "Please pass all connection details or set a default connection!"
        )

    config = read_config(filepath)

    if instance is None:
        return dict(config.defaults())

    else:
        instances = config.sections()
        if instance not in instances:
            raise ValueError(
                "The instance you have passed is not specified '{}' as a default "
                "connection '{}'!".format(instance, ", ".join(instances))
            )

        return dict(config[instance])


def test_connection(host, port, user, password, database):
    try:
        engine = create_engine(
            f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
        )
        with engine.connect() as _:
            pass
    except Exception as error:
        raise ConnectionError(
            f"{error}\nNo connection to the database could be established. "
            "Please check your input variables. "
            "Are you in the right network?"
        )


def set_defaults(
        *,
        host,
        port,
        database,
        user,
        password,
        instance=DEFAULT_INSTANCE_NAME,
        overwrite=False,
        test_conn=True,
        filepath=CONFIG_PATH
):
    """
    Description
    -----------

    Funktion sets a default connection.

    Parameter
    ---------

    host:       str, specifying the database server's address
    port        str, specifying the port on which the database runs
    database:   str, stating the database name
    user:       str, specifying the database user
    password:   str, specifying the database user's password
    instance:   str, specifying the name of the datapool_client instance. This name must be unique in the config.
    overwrite:  bool, needs to be set True to overwrite existing connection details of existing instance
    test_conn:  bool, test connection or not
    filepath:   str, set filepath for config file for testing purposes
    """

    connection_details = {
        "host": host,
        "user": user,
        "port": port,
        "database": database,
        "password": password,
    }

    if _os.path.exists(filepath):

        config = read_config(filepath)
        if overwrite:
            config[instance] = connection_details
        else:
            message = (
                "You have already set some defaults! "
                "If you want to overwrite the current settings pass ovewrite as True. "
                "If you do want to set defaults for another instance please "
                "pass a instance name that is not yet present in the ini file."
                "Available instances are: "
            )

            if instance == DEFAULT_INSTANCE_NAME and config.defaults():
                message += DEFAULT_INSTANCE_NAME
                raise ValueError(message)

            elif instance in config.sections() + [DEFAULT_INSTANCE_NAME]:
                message += ', '.join(config.sections())
                if config.defaults():
                    message += f", {DEFAULT_INSTANCE_NAME}"
                raise ValueError(message)

            else:
                config[instance] = connection_details
    else:
        config = _configparser.ConfigParser()
        config[instance] = connection_details

    if test_conn:
        test_connection(
           **connection_details
        )

    write_config(filepath, config)
