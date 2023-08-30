"""API endpoint for forecasts data (cloud, humidity, and aerosol)."""
import os
from datetime import datetime, timedelta
import flask
from flask import request
import numpy as np
import ephem
import pytz
import dataserver
from dataserver.api.authenticate import authenticate
from dataserver.config import SCORE_TABLE_PATH
from dataserver.logger import logger
from dataserver.api.utilities import get_directory, get_timezone, \
    get_forecast_type_and_hour, get_coordinates, get_lat_lng_idx, round_time, \
    get_light_pollution_score, get_milky_way_max_angle, \
    compute_hour_score, compute_final_score
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
    dates = [key for (key, value) in context["data"].items()]
    # lat and lng have to be strings for ephem.
    lat, lng = str(context["lat"]), str(context["lng"])
    context["dark_hours"] = []
    timezone = pytz.timezone(context["timezone"])

    observer = ephem.Observer()
    sun = ephem.Sun()

    observer.lat = lat
    observer.lon = lng

    for date in dates:
        observer.date = date

        # Calculate rising and setting times.
        next_sunrise = observer.next_rising(sun)
        next_sunset = observer.next_setting(sun)

        # Convert ephem times to datetime objects.
        next_sunrise_dt = ephem.localtime(next_sunrise)
        next_sunset_dt = ephem.localtime(next_sunset)

        # Convert times from UTC to local timezone.
        next_sunrise = next_sunrise_dt.astimezone(timezone)
        next_sunset = next_sunset_dt.astimezone(timezone)

        context["dark_hours"].append({
            "sunset": next_sunset.strftime("%Y/%m/%d %H:%M"),
            "sunrise": next_sunrise.strftime("%Y/%m/%d %H:%M")
        })


def get_moon_activity(context):
    """Add moon set and rise times to context."""
    context["moon_activity"] = []
    dates = [key for (key, value) in context["data"].items()]
    # lat and lng have to be strings for ephem.
    observer = ephem.Observer()
    observer.lat = str(context["lat"])
    observer.lon = str(context["lng"])
    moon = ephem.Moon()
    timezone = pytz.timezone(context["timezone"])

    setting_times, rising_times = [], []

    for each_day in range(MOON_FORECAST_DAYS):
        current_date_dt = datetime.strptime(min(dates), "%Y/%m/%d") + \
            timedelta(days=each_day)
        current_date_str = current_date_dt.strftime("%Y/%m/%d")

        observer.date = current_date_str

        next_setting = observer.next_setting(moon)
        next_rising = observer.next_rising(moon)
        next_setting_str = ephem.localtime(next_setting).astimezone(timezone).\
            strftime("%Y/%m/%d %H:%M:%S")
        next_rising_str = ephem.localtime(next_rising).astimezone(timezone).\
            strftime("%Y/%m/%d %H:%M:%S")

        # Maintain two sorted lists containing setting times and
        # rising times, respectively.
        if len(setting_times) == 0 or setting_times[-1] < next_setting_str:
            setting_times.append(next_setting_str)
        if len(rising_times) == 0 or rising_times[-1] < next_rising_str:
            rising_times.append(next_rising_str)

    # Always start from a setting. If there is a rising time that precedes
    # the first setting, remove the rising time from the list.
    if setting_times[0] > rising_times[0]:
        del rising_times[0]

    # Get 4 (setting, rising) pairs. Moon always has the pattern
    # (set, rise, set, rise, ...), i.e., setting and rising have to occur
    # alternately, although they may not happen on the same day.
    while len(context["moon_activity"]) < MOON_RESULT_DAYS:
        next_setting = datetime.strptime(
            setting_times[0], "%Y/%m/%d %H:%M:%S"
        ).strftime("%Y/%m/%d %H:%M")
        next_rising = datetime.strptime(
            rising_times[0], "%Y/%m/%d %H:%M:%S"
        ).strftime("%Y/%m/%d %H:%M")
        context["moon_activity"].append({
            "moonset": next_setting,
            "moonrise": next_rising
        })
        del setting_times[0]
        del rising_times[0]


def get_milky_way_activity(context):
    """Add Milky Way activity into context."""
    context["milky_way"] = {}
    dates = [key for (key, value) in context["data"].items()]
    # lat and lng have to be strings for ephem.
    observer = ephem.Observer()
    observer.lat = str(context["lat"])
    observer.lon = str(context["lng"])

    # Create a Sagittarius instance, which represents Milky Way center.
    sagittarius = ephem.readdb("Sgr,f|C|F7,17:58:03.470,-26:06:04.6,1.00,2000")

    get_milky_way_max_angle(context["milky_way"], observer)

    # Calculate Sagittarius rising, setting, and transit times.
    context["milky_way"]["activity"] = []
    timezone = pytz.timezone(context["timezone"])
    for date in dates:
        observer.date = date

        rising = observer.next_rising(sagittarius)
        setting = observer.next_setting(sagittarius)
        transit = observer.next_transit(sagittarius)

        # Convert to datetime format.
        rising_dt = ephem.localtime(rising)
        setting_dt = ephem.localtime(setting)
        transit_dt = ephem.localtime(transit)

        # Convert to local timezone and add to `context`.
        rising_time = rising_dt.astimezone(timezone)
        setting_time = setting_dt.astimezone(timezone)
        transit_time = transit_dt.astimezone(timezone)
        context["milky_way"]["activity"].append({
            "rise": rising_time.strftime("%Y/%m/%d %H:%M"),
            "set": setting_time.strftime("%Y/%m/%d %H:%M"),
            "transit": transit_time.strftime("%H:%M")
        })


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
            dark_hour_object["sunset"], "%Y/%m/%d %H:%M"
        )
        sunrise_dt = datetime.strptime(
            dark_hour_object["sunrise"], "%Y/%m/%d %H:%M"
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
