"""Render the welcome page."""
import flask
import dataserver


@dataserver.app.route("/")
def render_index_page():
    """Render the index page."""
    context = {}
    return flask.render_template("index.html", **context)
