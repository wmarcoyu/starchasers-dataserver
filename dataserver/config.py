"""Data server configuration."""
import json
import pathlib
import logging
from logging.handlers import TimedRotatingFileHandler
from psycopg2 import pool
APPLICATION_ROOT = "/"
SERVER_ROOT = pathlib.Path(__file__).resolve().parent.parent
DOWNLOAD_FOLDER = SERVER_ROOT/"var"
SCORE_TABLE_PATH = SERVER_ROOT/"data"/"score_table.npy"
PARKS_DATABASE_NAME = "parks"
USERS_DATABASE_NAME = "users"


def setup_logger(log_file_name):
    """Initialize a logger that writes to log_file_name."""
    logger_instance = logging.getLogger(__name__)
    logger_instance.setLevel(logging.DEBUG)

    handler = TimedRotatingFileHandler(
        filename=log_file_name, when="D", interval=1, backupCount=14
    )
    handler.setLevel(logging.DEBUG)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    logger_instance.addHandler(handler)
    logger_instance.addHandler(console_handler)

    return logger_instance


logger = setup_logger("logs/download.log")


# TODO: adjust this in production.
DATABASE_USER = "ec2-user"

with open(
        SERVER_ROOT/"credentials"/"config.json", "r", encoding="utf-8"
) as file:
    config = json.load(file)

# Create a connection pool for database `parks`.
PARKS_DATABASE = {
    "database": PARKS_DATABASE_NAME,
    "user": DATABASE_USER,
    "password": config["PARKS_DATABASE_PASSWORD"],
    "host": "localhost",
    "port": 5432,
}

# TODO: adjust min and max accordingly.
parks_connection_pool = pool.ThreadedConnectionPool(1, 30, **PARKS_DATABASE)

# Create a connection pool for database `users`.
USERS_DATABASE = {
    "database": USERS_DATABASE_NAME,
    "user": DATABASE_USER,
    "password": config["PARKS_DATABASE_PASSWORD"],
    "host": "localhost",
    "port": 5432,
}

users_connection_pool = pool.ThreadedConnectionPool(1, 5, **USERS_DATABASE)
