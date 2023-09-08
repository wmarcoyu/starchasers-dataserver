"""Utility functions for dataserver/api."""
import os
import math
from datetime import datetime, timedelta
import ephem
import pytz
from timezonefinder import TimezoneFinder
import dataserver
from dataserver.logger import logger
from preprocessing.light_pollution.bortle import get_bortle


DAYS_OF_ACTIVITY = 4


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

    If query parameters do not contain coordinates information, throw an error.
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


def get_setting_rising_pairs(
        start_date, result_dict, astro_object, observer, timezone
):
    """Obtain FOUR setting and rising pairs.

    A setting and rising pair records the setting and rising times of an
    astronomical object, e.g. Sun, on a given date. For example:
    {
      "sunset": xxxx, "sunrise": xxxx
    }

    object_type is either `sun` or `moon` in our case.

    The goal of this function is to ensure that for any pair, setting time
    PRECEDES rising time, since we want Sun-free and Moon-free hours.
    """
    current_date = timezone.localize(datetime.strptime(start_date, "%Y/%m/%d"))
    current_date = current_date.astimezone(pytz.UTC)  # in UTC

    while len(result_dict) < DAYS_OF_ACTIVITY:
        observer.date = current_date

        # Compute current setting time.
        setting = observer.next_setting(astro_object)
        setting_dt = ephem.localtime(setting).astimezone(pytz.UTC)  # UTC

        # Compute next rising time.
        rising = observer.next_rising(astro_object)
        rising_dt = ephem.localtime(rising).astimezone(pytz.UTC)  # UTC

        # Check if rising is after setting. If not, compute one more rising
        # with date incremented by 1.
        if rising_dt < setting_dt:
            observer.date = current_date + timedelta(days=1)

            rising = observer.next_rising(astro_object)
            rising_dt = ephem.localtime(rising).astimezone(pytz.UTC)  # UTC

            # If second rising time is still less than setting time,
            # raise a ValueError.
            if rising_dt < setting_dt:
                raise ValueError(
                    f"Error finding rising time following {setting_dt}."
                )

        # Convert back to local timezone before adding to result.
        setting_dt = setting_dt.astimezone(timezone)
        rising_dt = rising_dt.astimezone(timezone)

        result_dict.append({
            "set": setting_dt.strftime("%Y/%m/%d %H:%M"),
            "rise": rising_dt.strftime("%Y/%m/%d %H:%M")
        })

        # Update current_date.
        current_date = current_date + timedelta(days=1)


def get_transit(observer, astro_object, rising_dt, current_date):
    """Compute transit time following the current rising time."""
    def compute_next_transit(observer, astro_object, current_date):
        """Compute next transit time based on current_date."""
        observer.date = current_date
        transit = observer.next_transit(astro_object)
        return ephem.localtime(transit).astimezone(pytz.UTC)  # UTC

    transit_times = []

    # Find transit time based on current_date.
    transit_times.append(
        compute_next_transit(observer, astro_object, current_date)
    )

    # Decrement current_date by half a day and compute another transit time.
    transit_times.append(
        compute_next_transit(
            observer, astro_object, current_date - timedelta(days=0.5)
        )
    )

    # Increment current_date by half a day and compute another transit time.
    transit_times.append(
        compute_next_transit(
            observer, astro_object, current_date + timedelta(days=0.5)
        )
    )

    # Find the earliest transit time that is after rising_dt.
    result = current_date + timedelta(days=10)  # effectively the largest
    for each_transit_time in transit_times:
        # Both smaller than result and larger than rising_dt.
        if rising_dt < each_transit_time < result:
            result = each_transit_time

    if result == current_date + timedelta(days=10):
        raise ValueError(
            f"Error finding transit time following {rising_dt}."
        )

    return result  # UTC


def get_rising_setting_pairs(
        start_date, result_dict, astro_object, observer, timezone
):
    """Get FOUR rising and setting pairs.

    The goal of this function is to ensure that for any pair, rising time
    PRECEDES setting time.
    """
    current_date = timezone.localize(datetime.strptime(start_date, "%Y/%m/%d"))
    current_date = current_date.astimezone(pytz.UTC)  # in UTC

    while len(result_dict) < DAYS_OF_ACTIVITY:
        observer.date = current_date

        # Compute current rising time.
        rising = observer.next_rising(astro_object)
        rising_dt = ephem.localtime(rising).astimezone(pytz.UTC)  # UTC

        # Compute next setting time.
        setting = observer.next_setting(astro_object)
        setting_dt = ephem.localtime(setting).astimezone(pytz.UTC)  # UTC

        # Compute next transit, time the object reaches its max angle.
        transit_dt = get_transit(
            observer, astro_object, rising_dt, current_date
        )  # UTC

        # Check if setting is after rising. If not, compute one more setting
        # with date incremented by 1.
        if setting_dt < rising_dt:
            current_date_plus_one = current_date + timedelta(days=1)
            observer.date = current_date_plus_one  # UTC

            setting = observer.next_setting(astro_object)
            setting_dt = ephem.localtime(setting).astimezone(pytz.UTC)  # UTC

            # If second setting time is still less than rising time,
            # raise a ValueError.
            if setting_dt < rising_dt:
                raise ValueError(
                    f"Error finding setting time following {rising_dt}."
                )

        # Just to be safe, check that next transit is no later than next set.
        if transit_dt > setting_dt:
            raise ValueError(
                f"Next transit {transit_dt} is after next set {setting_dt}."
            )

        # Convert to local times before adding to result.
        rising_dt = rising_dt.astimezone(timezone)
        setting_dt = setting_dt.astimezone(timezone)
        transit_dt = transit_dt.astimezone(timezone)

        result_dict.append({
            "set": setting_dt.strftime("%Y/%m/%d %H:%M"),
            "rise": rising_dt.strftime("%Y/%m/%d %H:%M"),
            "transit": transit_dt.strftime("%Y/%m/%d %H:%M")
        })

        # Update current_date.
        current_date = current_date + timedelta(days=1)  # still in UTC


def get_object_activity(context, object_type):
    """Get the activity of an astronomical object.

    See details of `get_setting_rising_pairs` and `get_rising_setting_pairs`.
    """
    # Set up observer geolocation information.
    observer = ephem.Observer()
    observer.lat = str(context["lat"])
    observer.lon = str(context["lng"])
    timezone = pytz.timezone(context["timezone"])

    # Find start date.
    all_dates = [key for (key, value) in context["data"].items()]
    start_date = sorted(all_dates)[0]  # earliest of all

    # Determine the astronomical object and the location to store the results
    # based on object type.
    if object_type == "sun":
        astro_object = ephem.Sun()
        result_dict = context["dark_hours"]

        get_setting_rising_pairs(
            start_date, result_dict, astro_object, observer, timezone
        )
    elif object_type == "moon":
        astro_object = ephem.Moon()
        result_dict = context["moon_activity"]

        get_setting_rising_pairs(
            start_date, result_dict, astro_object, observer, timezone
        )
    elif object_type == "milky_way":
        astro_object = ephem.readdb(
            "Sgr,f|C|F7,17:58:03.470,-26:06:04.6,1.00,2000"
        )  # Sagittarius, constellation at Milky Way center
        result_dict = context["milky_way"]["activity"]

        get_rising_setting_pairs(
            start_date, result_dict, astro_object, observer, timezone
        )
    else:
        raise TypeError(f"Invalid object type: {object_type}")


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


def get_milky_way_max_angle(result_dict, lat, lng):
    """Add Milky Way center max angle to context.

    Milky Way center max angle is independent of time and only depends
    on location. It is invisible in winter (in Northern Hemisphere) only
    because it rises in the morning and sets in the afternoon.

    Requirements:
    latitude and longitude should already be stored into observer.
    """
    observer = ephem.Observer()
    observer.lat = str(lat)
    observer.lon = str(lng)

    # Create a Sagittarius instance, which represents Milky Way center.
    sagittarius = ephem.readdb("Sgr,f|C|F7,17:58:03.470,-26:06:04.6,1.00,2000")

    # Compute max angle and add to dict.
    transit = observer.next_transit(sagittarius)
    observer.date = transit
    sagittarius.compute(observer)
    result_dict["max_angle"] = \
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
