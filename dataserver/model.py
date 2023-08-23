"""Utility functions for the world model."""
import flask
import numpy as np
import dataserver
from dataserver.config import parks_connection_pool, users_connection_pool


def get_parks_db():
    """Get a connection from the connection pool.

    Do not confuse it with `get_parks_db` in the app server.

    Similarity: they both use the same database `parks`, containing a table
    also named `parks` that contains information about listed parks. Code to
    open and close connections is also identical.

    Difference: this function is used by back-end to get forecasts and scores
    while the other one is used by front-end for searching purposes.

    Main reason to have the same function is to off-load the data server
    a bit. Data server primarily focues on forecast data retreival and
    processing, whereas the app server is responsible for searching.
    """
    if "parks_db" not in flask.g:
        flask.g.parks_db = parks_connection_pool.getconn()
    return flask.g.parks_db  # returns a connection


@dataserver.app.teardown_appcontext
def close_parks_db(error=None):
    """Release the database connection back to pool."""
    assert error or not error  # avoid unused parameter error
    database_connection = flask.g.pop("parks_db", None)
    if database_connection is not None:
        database_connection.commit()
        parks_connection_pool.putconn(database_connection)


def get_users_db():
    """Get a connection to `users` database."""
    if "users_db" not in flask.g:
        flask.g.users_db = users_connection_pool.getconn()
    return flask.g.users_db


@dataserver.app.teardown_appcontext
def close_users_db(error=None):
    """Release the database connection back to pool."""
    assert error or not error  # avoid unused parameter error
    database_connection = flask.g.pop("users_db", None)
    if database_connection is not None:
        database_connection.commit()
        users_connection_pool.putconn(database_connection)


def get_lat_idx(target_lat):
    """Return the matrix index of target latitude."""
    if target_lat > 90 or target_lat < -90:
        raise ValueError("Latitude out of range. -90 <= lat <= 90")
    return np.abs(dataserver.LATS[:, 0] - target_lat).argmin()


def get_lng_idx(target_lng):
    """Return the matrix index of target longitude."""
    if target_lng > 180 or target_lng < -180:
        raise ValueError("Longitude out of range. -180 <= lng <= 180")
    # Values in LNGS range from 0 to 360 - offset target_lng by 180.
    target_lng += 180
    return np.abs(dataserver.LNGS - target_lng).argmin()


def get_cloud_humidity_index(input_percentage):
    """Return cloud/humidity index based on cloud/humidity percentage.

    [0, 20) - low - 0
    [20, 40) - moderate - 1
    [40, 100] - high - 2

    We use the same function for both cloud cover and humidity because
    they have the same quantitave standard.
    """
    if input_percentage < 0 or input_percentage > 100:
        raise ValueError(
            f"Invalid cloud/humidity input {input_percentage}. "
            "Should be between 0 and 100."
        )
    if input_percentage < 20:
        return 0
    if input_percentage < 40:
        return 1
    return 2


def get_aerosol_index(aerosol):
    """Return aersol index based on total aerosol concentration.

    [0, 0.1) - low - 0
    [0.1, 0.3) - moderate - 1
    [0.3, inf) - high - 2
    """
    if aerosol < 0.1:
        return 0
    if aerosol < 0.3:
        return 1
    return 2
