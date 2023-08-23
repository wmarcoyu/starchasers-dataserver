"""Starchaser data server package init."""
import flask
from flask_cors import CORS
import numpy as np
app = flask.Flask(__name__)
# Allow cross origin resource sharing.
CORS(app)
app.config.from_object("dataserver.config")
import dataserver.model  # noqa: E402  pylint: disable=wrong-import-position
import dataserver.api  # noqa: E402  pylint: disable=wrong-import-position
import dataserver.controllers  # noqa: E402  pylint: disable=wrong-import-position


LATS = np.load("data/0p25_lats.npy")
LNGS = np.load("data/0p25_lngs.npy")
