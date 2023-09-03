"""API endpoint for forecasts data (cloud, humidity, and aerosol)."""
import os
from datetime import datetime, timedelta
import flask
from flask import request
import numpy as np
import pytz
import dataserver
from dataserver.api.authenticate import authenticate
from dataserver.config import SCORE_TABLE_PATH
from dataserver.logger import logger
from dataserver.api.utilities import get_directory, get_timezone, \
    get_forecast_type_and_hour, get_coordinates, get_lat_lng_idx, round_time, \
    get_light_pollution_score, get_milky_way_max_angle, \
    compute_hour_score, compute_final_score, get_object_activity
from dataserver.model import get_cloud_humidity_index, get_aerosol_index


HOURS_OF_PREDICTION = 72
# Get forecasts for 5 days and keep results for 4 days.
MOON_FORECAST_DAYS = 5
MOON_RESULT_DAYS = 4
CONVERSION_TABLE_PATH = "data/sky_transparency_table.npy"


@dataserver.app.route("/api/transparency-forecast/")
def get_transparency_forecast():
    """Return sky transparency forecast.

    Combine the three aspects of transparency and return a JSON
    with `transparency`, `cloud`, `humidity`, and `aerosol`.
    """
    authenticate(request)

    # Get the three aspects of transparency.
    cloud_humidity_dict = get_forecasts(request, "gfs")
    aerosol_dict = get_forecasts(request, "gefs")

    # Store the existing three aspects into context.
    context = {}
    context["cloud"] = cloud_humidity_dict["cloud"]
    context["humidity"] = cloud_humidity_dict["humidity"]
    context["aerosol"] = aerosol_dict["aerosol"]

    # Store timestamp into context.
    if cloud_humidity_dict["timestamp"] != aerosol_dict["timestamp"]:
        t_gfs = cloud_humidity_dict["timestamp"]
        t_gefs = aerosol_dict["timestamp"]
        logger.error(
            "GFS and GEFS data have different timestamps: %s %s", t_gfs, t_gefs
        )
    context["timestamp"] = aerosol_dict["timestamp"]
    context["lat"] = aerosol_dict["lat"]
    context["lng"] = aerosol_dict["lng"]

    # Compute transparency for each hour.
    context["transparency"] = {}
    conversion_table = np.load(CONVERSION_TABLE_PATH)
    for each_hour in range(HOURS_OF_PREDICTION):
        cloud = context["cloud"][each_hour]
        humidity = context["humidity"][each_hour]
        aerosol = context["aerosol"][each_hour]

        # Get transparency score.
        cloud_index = get_cloud_humidity_index(cloud)
        humidity_index = get_cloud_humidity_index(humidity)
        aerosol_index = get_aerosol_index(aerosol)
        transparency_score = \
            conversion_table[cloud_index][humidity_index][aerosol_index]
        context["transparency"][each_hour] = int(transparency_score)

    context = prettify_context_by_date(context, request)

    return flask.jsonify(**context), 200


def prettify_context_by_date(context, request_object):
    """Split data in context by date.

    Raw data in `context` are forecasts for the continuous future 72 hours.
    This function separates the data by date according to local timezone.
    """
    # Find local time of data update.
    local_timezone = get_timezone(context["lat"], context["lng"])
    utc_date_time = datetime.strptime(context["timestamp"], "%Y%m%d%H")
    utc_date_time = utc_date_time.replace(tzinfo=pytz.UTC)
    local_time = utc_date_time.astimezone(pytz.timezone(local_timezone))

    prettified_context = {}
    prettified_context["data"] = {}

    for each_hour in range(HOURS_OF_PREDICTION):
        current_date_time = local_time + timedelta(hours=each_hour)
        current_day_str = current_date_time.strftime("%Y/%m/%d")
        # Add date to dict if it does not exist.
        if current_day_str not in prettified_context["data"]:
            prettified_context["data"][current_day_str] = {}

        # Add data of current hour to dict under current date.
        current_hour_str = current_date_time.strftime("%H")  # 0 t0 23
        prettified_context["data"][current_day_str][current_hour_str] = {
            "transparency": context["transparency"][each_hour],
            "cloud": context["cloud"][each_hour],
            "humidity": context["humidity"][each_hour],
            "aerosol": context["aerosol"][each_hour]
        }

    prettified_context["timestamp"] = context["timestamp"]
    prettified_context["timezone"] = local_timezone
    prettified_context["lat"] = context["lat"]
    prettified_context["lng"] = context["lng"]

    get_dark_hours(prettified_context)
    get_moon_activity(prettified_context)
    get_milky_way_activity(prettified_context)
    compute_score_forecast(prettified_context, request_object)

    return prettified_context


def get_forecasts(request_object, data_type):
    """Get forecast data for coordinate and store them in context.

    Parameters:
    request_object - flask.request object specific to a url.
    data_type - gfs (cloud and humidity) or gefs (aerosol).

    Return:
    A dictionary context for JSON rendering.
    """
    lat, lng = get_coordinates(request_object)
    lat_idx, lng_idx = get_lat_lng_idx(lat, lng)
    context = {}
    context["lat"] = lat
    context["lng"] = lng
    if data_type == "gfs":
        context["cloud"], context["humidity"] = {}, {}
    elif data_type == "gefs":
        context["aerosol"] = {}
    data_directory = "/"

    # If the function is running for testing, choose a future(debug) date.
    if request_object.args.get("test") is not None:
        data_directory, timestamp = get_directory(
            data_type, "30770617"
        )
    # Otherwise use current date.
    else:
        data_directory, timestamp = get_directory(data_type)

    all_filenames = os.listdir(data_directory)
    for filename in all_filenames:
        data = np.load(f"{data_directory}/{filename}")
        forecast_type, forecast_hour = get_forecast_type_and_hour(filename)
        context[forecast_type][forecast_hour] = data[lat_idx][lng_idx]

    # Special case for gefs: aerosol data update every three hours instead
    # of every hour, so we 'fill in the gaps' with most recent available data.
    if data_type == "gefs":
        for each_hour in range(72):
            context["aerosol"][each_hour] = \
                context["aerosol"][(each_hour//3) * 3]

    context["timestamp"] = timestamp
    context["lat"] = lat
    context["lng"] = lng
    return context


def get_dark_hours(context):
    """Add dark hours and moon activity to context."""
    context["dark_hours"] = []
    get_object_activity(context, "sun")


def get_moon_activity(context):
    """Add moon set and rise times to context."""
    context["moon_activity"] = []
    get_object_activity(context, "moon")


def get_milky_way_activity(context):
    """Add Milky Way activity into context."""
    context["milky_way"] = {}
    get_milky_way_max_angle(
        context["milky_way"], context["lat"], context["lng"]
    )
    context["milky_way"]["activity"] = []
    get_object_activity(context, "milky_way")


def compute_score_forecast(context, request_object):
    """Compute stargazing score for forecast page.

    If `park_id` is not None, use it to determine light pollution score.
    Otherwise, use `lat` and `lng`.
    At this step, `context` already contains `lat` and `lng`, which are
    determined earlier by `get_coordinates` function. If they were not
    provided, an exception would have been thrown earlier.
    """
    park_id = request_object.args.get("park_id")
    if park_id is not None:
        light_pollution_score = get_light_pollution_score(park_id, None)
    else:
        light_pollution_score = get_light_pollution_score(
            None, (context["lat"], context["lng"])
        )
    score_table = np.load(SCORE_TABLE_PATH)
    scores = []
    for dark_hour_object in context["dark_hours"]:
        sunset_dt = datetime.strptime(
            dark_hour_object["set"], "%Y/%m/%d %H:%M"
        )
        sunrise_dt = datetime.strptime(
            dark_hour_object["rise"], "%Y/%m/%d %H:%M"
        )
        # Round up sunset hour and round down sunrise hour.
        sunset_dt = round_time(sunset_dt, "up")
        sunrise_dt = round_time(sunrise_dt, "down")
        hour_dt = sunset_dt
        while hour_dt != sunrise_dt:
            try:
                scores.append(
                    compute_hour_score(context, hour_dt,
                                       light_pollution_score, score_table)
                )
            except ValueError as error:
                logger.error(
                    "Ignored error: %s", error
                )
            hour_dt += timedelta(hours=1)
    try:
        context["score"] = compute_final_score(scores)
    except ValueError as error:
        logger.warning(error)
        context["score"] = "No available score"
