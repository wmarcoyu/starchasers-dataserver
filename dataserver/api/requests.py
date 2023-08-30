"""Endpoints that server app server requests."""
import flask
from flask import request
import dataserver
from dataserver.logger import logger


@dataserver.app.route("/api/get-park-name/")
def get_park_name():
    """Return park fullname based on parameter `park_id`."""
    park_id = request.args.get("park_id")
    if park_id is None:
        raise ValueError("park_id cannot be None.")
    connection = dataserver.model.get_parks_db()
    cursor = connection.cursor()
    cursor.execute(
        "SELECT * FROM parks WHERE id = %s", (park_id, )
    )
    data = cursor.fetchall()
    if len(data) == 0:
        raise ValueError(f"No matches for park_id {park_id} in the database.")
    # Shouldn't really happen.
    if len(data) > 1:
        logger.warning(
            "In details.py more than 1 match found in parks database."
        )

    # (id, lat, lng, park_name, admin_name, country, light_pollution)
    park_info = data[0]
    park_name = park_info[3]
    admin_name = park_info[4]
    country = park_info[5]

    fullname = f"{park_name}, {admin_name}, {country}"
    context = {"fullname": fullname}
    return flask.jsonify(**context), 200
