"""Data server configuration."""
import json
import pathlib
from psycopg2 import pool
APPLICATION_ROOT = "/"
SERVER_ROOT = pathlib.Path(__file__).resolve().parent.parent
DOWNLOAD_FOLDER = SERVER_ROOT/"var"
SCORE_TABLE_PATH = SERVER_ROOT/"data"/"score_table.npy"
PARKS_DATABASE_NAME = "parks"
USERS_DATABASE_NAME = "users"
LIGHT_POLLUTION_DATABASE_NAME = "light_pollution"


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
    "password": config["DATABASE_PASSWORD"],
    "host": "localhost",
    "port": 5432,
}

# TODO: adjust min and max accordingly.
parks_connection_pool = pool.ThreadedConnectionPool(1, 10, **PARKS_DATABASE)

# Create a connection pool for database `users`.
USERS_DATABASE = {
    "database": USERS_DATABASE_NAME,
    "user": DATABASE_USER,
    "password": config["DATABASE_PASSWORD"],
    "host": "localhost",
    "port": 5432,
}

users_connection_pool = pool.ThreadedConnectionPool(1, 10, **USERS_DATABASE)

LIGHT_POLLUTION_DATABASE = {
    "database": LIGHT_POLLUTION_DATABASE_NAME,
    "user": DATABASE_USER,
    "password": config["DATABASE_PASSWORD"],
    "host": "localhost",
    "port": 5432,
}

light_pollution_connection_pool = pool.ThreadedConnectionPool(
    1, 10, **LIGHT_POLLUTION_DATABASE
)

print("Data server configuration complete.")
