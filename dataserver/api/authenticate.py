"""Authenticate an API user."""
import flask
from dataserver.model import get_users_db


def authenticate(request_object):
    """Return 403 if input token is incorrect."""
    username = request_object.headers.get("Username")
    input_token = request_object.headers.get("Token")
    if username is None or input_token is None:
        flask.abort(400, description="Check username and input token.")

    # Check if token is correct.
    connection = get_users_db()
    cursor = connection.cursor()
    cursor.execute(
        "SELECT token FROM users WHERE username = %s", (username, )
    )
    results = cursor.fetchall()

    if len(results) != 1:
        flask.abort(400, description="Non-unique tokens found.")

    stored_token = results[0][0]

    if input_token != stored_token:
        flask.abort(403)

    # Authentication complete.
