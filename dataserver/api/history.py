"""API endpoint for retrieving average/historical transparency data."""
import json
from datetime import datetime, timedelta
import flask
import numpy as np
import ephem
import pytz
import dataserver
from dataserver.api.authenticate import authenticate
from dataserver.api.utilities import get_lat_lng_idx, get_coordinates, \
    get_timezone, get_milky_way_max_angle, get_bortle_class
from dataserver.logger import logger


@dataserver.app.route("/api/historical-transparency/")
def get_historical_transparency():
    """Return the historical/average transparency data for a given location.

    Location is stored as arguments in url.
    Return a JSON with overall `transparency` data, along with separate
    `cloud`, `humidity`, and `aerosol` data. Each category consists of data
    for 12 months.
    """
    authenticate(flask.request)

    lat, lng = get_coordinates(flask.request)
    lat_idx, lng_idx = get_lat_lng_idx(lat, lng)  # this function checks errors
    history_data_path = str(
        dataserver.app.config["SERVER_ROOT"]/"history_data"
    )
    context = {}
    context["lat"] = lat
    context["lng"] = lng
    print("Input lat: ", lat)
    # Get monthly data for transparency, cloud cover, humidity, and aerosol.
    context["transparency"], context["cloud"] = {}, {}
    context["humidity"], context["aerosol"] = {}, {}
    for month in range(1, 13):
        transparency = np.load(f"{history_data_path}/{month}/transparency.npy")
        cloud = np.load(f"{history_data_path}/{month}/cloud.npy")
        humidity = np.load(f"{history_data_path}/{month}/humidity.npy")
        aerosol = np.load(f"{history_data_path}/{month}/aerosol.npy")

        context["transparency"][month] = int(transparency[lat_idx][lng_idx])
        context["cloud"][month] = cloud[lat_idx][lng_idx]
        context["humidity"][month] = humidity[lat_idx][lng_idx]
        context["aerosol"][month] = aerosol[lat_idx][lng_idx]

    context["timezone"] = get_timezone(lat, lng)
    context["year"] = str(datetime.now().year)

    get_milky_way_season(context)
    get_new_moon_dates(context)
    get_light_pollution(context, flask.request)
    get_next_meteor_shower(context)

    return flask.jsonify(**context), 200


def get_milky_way_season(context):
    """Add Milky Way season to context.

    Milky Way season is defined as following. It starts on the date when Milky
    Way center first rises above horizon before midnight (12:00 AM), and it
    ends on the date when Milky Way center first sets below horizon before
    midnight (12:00 AM).
    """
    context["milky_way_season"] = {}

    observer = ephem.Observer()
    observer.lat = str(context["lat"])
    observer.lon = str(context["lng"])
    get_milky_way_max_angle(context["milky_way_season"], observer)
    sagittarius = ephem.readdb("Sgr,f|C|F7,17:58:03.470,-26:06:04.6,1.00,2000")

    # Determine start date: {current_year}/01/01.
    current_year = context["year"]
    start_date = f"{current_year}/01/01"
    start_date = datetime.strptime(start_date, "%Y/%m/%d")
    end_date = datetime.strptime(f"{current_year}/12/31", "%Y/%m/%d")
    timezone = pytz.timezone(context["timezone"])

    start_is_found = False
    date = start_date
    while date <= end_date:
        observer.date = date

        # Find season start.
        if not start_is_found:
            next_rising = observer.next_rising(sagittarius)
            next_rising_time = ephem.localtime(next_rising).astimezone(
                timezone
            )

            # E.g. 165543 for 16:55:43
            hour_minute_second = next_rising_time.strftime("%H%M%S")

            # Milky Way season start found.
            if "180000" < hour_minute_second <= "235959":
                context["milky_way_season"]["start"] = \
                    next_rising_time.strftime("%B %d")
                start_is_found = True

        # Find season end.
        else:
            next_setting = observer.next_setting(sagittarius)
            next_setting_time = ephem.localtime(next_setting).astimezone(
                timezone
            )

            hour_minute_second = next_setting_time.strftime("%H:%M:%S")

            # Milky Way season end found.
            if "180000" < hour_minute_second <= "235959":
                context["milky_way_season"]["end"] = \
                    next_setting_time.strftime("%B %d")
                return

        # Increment date.
        date += timedelta(days=1)

    logger.error("Milky Way season not found for (%s, %s).",
                 context["lat"], context["lng"])
    raise ValueError("Milky Way season not found for "
                     f"({context['lat']}, {context['lng']}).")


def get_new_moon_dates(context):
    """Get all new moon dates in current year."""
    current_year = int(context["year"])
    timezone = pytz.timezone(context["timezone"])
    start_date = datetime.strptime(
        f"{current_year}/01/01", "%Y/%m/%d"
    )

    # Earliest new moon date of ephem format.
    new_moon_date_ep = ephem.previous_new_moon(start_date)

    all_dates = []
    while new_moon_date_ep.datetime().year < current_year + 1:
        # Convert it to datetime object and add UTC timezone info.
        new_moon_date_utc = new_moon_date_ep.datetime().replace(
            tzinfo=pytz.UTC
        )
        new_moon_date_local = new_moon_date_utc.astimezone(timezone)

        # Add it to list if it is within current_year.
        if new_moon_date_local.year == current_year:
            all_dates.append(
                new_moon_date_local.strftime("%b %d")
            )

        # Update to next date.
        new_moon_date_ep = ephem.next_new_moon(new_moon_date_ep)

    context["new_moon_dates"] = all_dates


def get_light_pollution(context, request_object):
    """Add Bortle class to context."""
    # If park_id is not None, pass park_id into get_bortle_class.
    park_id = request_object.args.get("park_id")
    if park_id is not None:
        context["bortle"] = get_bortle_class(park_id=str(park_id),
                                             coordinates=None)
        return

    # park_id is None, read lat and lng from context.
    # lat and lng exist for sure otherwise an exception will be raised earlier
    # in `get_coordinates` function.
    lat, lng = context["lat"], context["lng"]
    context["bortle"] = get_bortle_class(park_id=None, coordinates=(lat, lng))
    return


def get_next_meteor_shower(context):
    """Find next meteor shower and add it to context.

    Meteor showers in data/meteor_shower_table.json are already sorted in
    ascending order by `max_date`.
    """
    table_path = "data/meteor_shower_table.json"
    with open(table_path, encoding="utf-8") as file:
        table = json.load(file)

    # Find the meteor shower whose `max_date` is no less than (>=) `today`.
    today = datetime.now().strftime("%Y/%m/%d")
    for meteor_shower in table["data"]:
        max_date = meteor_shower["max_date"]
        if max_date >= today:
            context["next_meteor_shower"] = meteor_shower
            return

    # NOTE: the code should never reach here.
    logger.error("Cannot find next meteor shower for %s.", today)
    raise ValueError(f"Cannot find next meteor shower for {today}.")
