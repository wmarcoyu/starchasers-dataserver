"""Utility functions for dataserver/api."""
import os
import math
from datetime import datetime, timedelta
import ephem
import pytz
from timezonefinder import TimezoneFinder
import dataserver
from dataserver.config import logger
from preprocessing.light_pollution.bortle import get_bortle


def get_directory(data_type, current_date_str=None):
    """Return the directory path to the most up-to-date data.

    Parameter current_date_str is purely for debugging purposes.
    Return:
    a tuple (data_directory, timestamp) where timestamp is of format
    `YYYYMMDDHH`, e.g. 2023071100 (UTC).
    """
    if data_type not in ("gfs", "gefs"):
        raise ValueError(
            f"Invalid data_type: {data_type}. Expected \'gfs\' or \'gefs\'"
        )
    # string type
    if current_date_str is None:
        current_date_str = datetime.now().strftime("%Y%m%d")
    data_times = ["18", "12", "06", "00"]
    data_retention_time = 3
    completion_flag = "complete.flag"
    # Find the most recent data starting from present up to 3 days.
    for day in range(data_retention_time):
        # datetime type
        current_date_dt = datetime.strptime(current_date_str, "%Y%m%d")
        date_dt = current_date_dt - timedelta(days=day)
        date_str = date_dt.strftime("%Y%m%d")
        for each_time in data_times:
            dir_path = f"data/{date_str}/{each_time}"
            if os.path.exists(dir_path) and \
               os.path.exists(f"{dir_path}/{completion_flag}"):
                return (f"{dir_path}/{data_type}", f"{date_str}{each_time}")
    raise FileNotFoundError("Cannot find processed NOAA datasets.")


def get_forecast_type_and_hour(filename):
    """Extract forecast type and hour from filename.

    Parameter:
    filename - type.fXXX.npy, where type is the type of stored data (cloud/
    humidity/aerosol). XXX is the forecast hour.

    Return:
    A tuple of forecast type and hour. E.g. cloud.f007.npy -> (cloud, 7)
    """
    forecast_type = filename.split(".")[0]
    forecast_hour_string = filename.split(".")[1]
    assert len(forecast_hour_string) == 4
    forecast_hour_int = int(forecast_hour_string[1:])
    return (forecast_type, forecast_hour_int)


def get_coordinates(request_object):
    """Return (latitude, longitude) of the request.

    If `park_id` is provided, coordinates will be determined by searching
    in the parks database.

    If `park_id` is not provided or database query comes back with no results,
    then coordinates will be determined by checking query parameters.

    If query parameters do not contain coordinates, throw an error.
    """
    def read_coordinates_from_request(request_object):
        """Directly obtain coordinates from request parameters."""
        lat, lng = request_object.args.get("lat"), \
            request_object.args.get("lng")
        # Case 3: no park_id and no/invalid coordinates parameters.
        if lat is None or lng is None:
            raise ValueError(
                "No coordinates found. "
                "Check the validity of lat, lng and/or park_id."
            )
        return (lat, lng)

    park_id = request_object.args.get("park_id")
    if park_id is not None:
        connection = dataserver.model.get_parks_db()
        cursor = connection.cursor()
        cursor.execute(
            "SELECT lat, lng FROM parks WHERE id = %s", (park_id, )
        )
        results = cursor.fetchall()
        if len(results) == 0:
            # Case 2: park_id provided but no results in database.
            return read_coordinates_from_request(request_object)
        if len(results) > 1:
            logger.error(
                "More than one set of coordinates obtained from database. "
                "park_id: %s", park_id
            )
        # Case 1: determine coordinates by park_id.
        return results[0]  # already a tuple of (lat, lng)
    # Case 2: no park_id provided.
    return read_coordinates_from_request(request_object)


def get_lat_lng_idx(lat, lng):
    """Check the validity of input latitude and longitude.

    Return a tuple of (lat_idx, lng_idx) if both are valid.
    """
    if lat is None or lng is None:
        raise ValueError("Latitude and longitude cannot be empty.")
    # Error raised by get_lat_idx or get_lng_idx would be automatically
    # propagated upwards.
    lat_idx, lng_idx = dataserver.model.get_lat_idx(float(lat)), \
        dataserver.model.get_lng_idx(float(lng))
    # Both are valid, return a tuple.
    return (lat_idx, lng_idx)


def get_bortle_class(park_id=None, coordinates=None):
    """Get Bortle class of the given location.

    Exactly one of `park_id` and `coordinates` is not None and the other is
    None. If `park_id` is not None, Bortle class will be determined by
    searching in the database. If `coordinates` is not None, Bortle class
    will be determined by directly reading from the light pollution map.

    coordinates should be in (lat, lng) format.
    """
    # Both are None.
    if park_id is None and coordinates is None:
        raise ValueError("Both park_id and coordinates are None.")
    # Both are not None.
    if park_id is not None and coordinates is not None:
        raise ValueError("Both park_id and coordinates are not None.")
    if park_id is not None:
        # Get Bortle class computed for the park.
        connection = dataserver.model.get_parks_db()
        cursor = connection.cursor()
        cursor.execute(
            "SELECT light_pollution from parks WHERE id = %s", (park_id, )
        )
        result = cursor.fetchall()
        if len(result) == 0:
            raise ValueError(
                f"No light pollution data fetched for park_id {park_id}."
            )
        if len(result) > 1:
            logger.warning(
                "More than one light pollution data fetched for park_id %s.",
                park_id
            )
        return int(result[0][0])

    # The other case - find Bortle class by coordinates.
    lat, lng = coordinates  # both strings and doubles are fine
    return get_bortle(lng, lat)


def get_light_pollution_score(park_id=None, coordinates=None):
    """Fetch Bortle class and return light pollution score.

    Return:
    0 - excellent (Bortle 1)
    1 - good (Bortle 2 - 4)
    2 - average (Bortle 5)
    3 - poor (Bortle 6 and above)
    """
    bortle_class = get_bortle_class(park_id, coordinates)
    if bortle_class == 1:
        return 0
    if bortle_class <= 4:
        return 1
    if bortle_class == 5:
        return 2
    return 3


def get_transparency_score(transparency):
    """Return transparency score based on transparency value.

    Raw transparency value is on the scale of 1 to 5, with 1 being poor and 5
    being excellent. In score_table.npy, however, transparency is on the
    scale of 0 to 4, with 0 being excellent and 4 being poor.
    """
    transparency = int(transparency)
    if transparency == 5:
        return 0
    if transparency == 4:
        return 1
    if transparency == 3:
        return 2
    if transparency == 2:
        return 3
    if transparency == 1:
        return 4
    raise ValueError(f"Invalid input transparency value {transparency}.")


def get_milky_way_max_angle(context, observer):
    """Add Milky Way center max angle to context.

    Milky Way center max angle is independent of time and only depends
    on location. It is invisible in winter (in Northern Hemisphere) only
    because it rises in the morning and sets in the afternoon.

    Requirements:
    latitude and longitude should already be stored into observer.
    """
    if observer.lat is None or observer.lon is None:
        raise ValueError(
            "Observer is missing coordinates info. Check observer.lat and "
            "observer.lon."
        )
    # Create a Sagittarius instance, which represents Milky Way center.
    sagittarius = ephem.readdb("Sgr,f|C|F7,17:58:03.470,-26:06:04.6,1.00,2000")

    # Compute max angle and add to dict.
    transit = observer.next_transit(sagittarius)
    observer.date = transit
    sagittarius.compute(observer)
    context["max_angle"] = \
        f"{math.degrees(sagittarius.alt):.2f}\u00b0"


def round_time(time_dt, direction):
    """Round a datetime object to the closest previous or next hour.

    Parameters:
    time_dt - input time in datetime format.
    direction - 'up' or 'down' to define the rounding direction.
    """
    if direction == 'up':
        if time_dt.minute > 0 or time_dt.second > 0 or time_dt.microsecond > 0:
            time_dt = time_dt + timedelta(hours=1)
        return time_dt.replace(minute=0, second=0, microsecond=0)
    if direction == 'down':
        return time_dt.replace(minute=0, second=0, microsecond=0)
    raise ValueError("Direction must be 'up' or 'down'")


def is_moon_free(lat, lng, start_time_dt):
    """Return if the moon is invisible in current hour.

    Return:
    True - if the moon is invisible.
    False - if the moon is visible.
    NOTE: start_time_dt should be in UTC.
    """
    if start_time_dt.tzinfo != pytz.UTC:
        raise ValueError(
            "Input time should be in UTC timezone."
        )
    if start_time_dt.minute != 0 or start_time_dt.second != 0:
        raise ValueError(
            "Input time should be rounded to the nearest hour."
        )
    observer = ephem.Observer()
    # lat and lng should be strings.
    observer.lat = str(lat)
    observer.lon = str(lng)
    observer.date = start_time_dt
    moon = ephem.Moon()
    moon.compute(observer)

    # Check if moon is below horizon at start hour.
    if moon.alt > 0:
        return False

    # If moon is indeed below horizon at start hour, continue to check if
    # it is also below horizon at end hour.
    end_time_dt = start_time_dt + timedelta(hours=1)
    observer.date = end_time_dt
    moon.compute(observer)

    return moon.alt <= 0


def compute_hour_score(context, start_time_dt,
                       light_pollution_score, score_table):
    """Compute overall stargazing score for an hour given start time.

    Score table dimensions: [light_pollution][transparency]
    Return:
    4 - excellent
    3 - good
    2 - average
    1 - poor
    """
    lat, lng = str(context["lat"]), str(context["lng"])
    # Moon is above horizon in current hour - return poor.
    local_timezone = pytz.timezone(context["timezone"])
    local_time_dt = local_timezone.localize(start_time_dt)
    utc_time = local_time_dt.astimezone(pytz.UTC)
    if not is_moon_free(lat, lng, utc_time):
        return 1
    # No moon visible - consider light pollution and sky transparency.
    date = start_time_dt.strftime("%Y/%m/%d")
    hour = start_time_dt.strftime("%H")
    if date in context["data"] and hour in context["data"][date]:
        transparency = int(context["data"][date][hour]["transparency"])
        transparency_score = get_transparency_score(transparency)
        return int(score_table[light_pollution_score][transparency_score])
    raise ValueError(
        f"No available data for {start_time_dt}."
    )


def compute_final_score(scores):
    """Compute final score given a list of hourly scores.

    Return:
    S - if at least three hourly scores are 4 - excellent.
    A - if at least three hourly scores are 3 - good and above.
    B - if at least five hourly scores are 2 - average and above.
    C - if none of the above.
    """
    if len(scores) < 5:
        raise ValueError("Insufficient hourly scores.")
    scores_count = {i: 0 for i in range(1, 5)}
    for score in scores:
        scores_count[score] += 1
    if scores_count[4] >= 3:
        return "S"
    if scores_count[4] + scores_count[3] >= 3:
        return "A"
    if scores_count[4] + scores_count[3] + scores_count[2] >= 5:
        return "B"
    return "C"


def get_timezone(lat, lng):
    """Compute local timezone at location (lat, lng)."""
    tf_instance = TimezoneFinder()
    local_timezone = tf_instance.certain_timezone_at(
        lat=float(lat), lng=float(lng)
    )
    return local_timezone
