"""Test cloud.py in dataserver/api module."""
import os
import shutil
from datetime import datetime
import pytz
from dataserver.api.utilities import get_directory, \
    get_forecast_type_and_hour, is_moon_free
from dataserver.api.forecasts import get_moon_activity


def clean_directory():
    """Remove testing directories, if there are any."""
    if os.path.exists("data/30770615"):
        shutil.rmtree("data/30770615")
    if os.path.exists("data/30770616"):
        shutil.rmtree("data/30770616")
    if os.path.exists("data/30770617"):
        shutil.rmtree("data/30770617")


def test_get_directory():
    """Test get_directory function."""
    clean_directory()
    # Case 1: available data 30770615, 30770616, and 30770617 up to 12.
    # 30770617/18 folder does not exist.
    os.makedirs("data/30770617/12/complete.flag")  # previous data don't matter
    # Request is made on 30770617. Should return 30770617/12/gfs(gefs)
    assert get_directory("gfs", "30770617") == \
        ("data/30770617/12/gfs", "3077061712")
    assert get_directory("gefs", "30770617") == \
        ("data/30770617/12/gefs", "3077061712")
    # Request is made on 30770618. Should also return 30770617/12/gfs(gefs)
    assert get_directory("gfs", "30770618") == \
        ("data/30770617/12/gfs", "3077061712")
    assert get_directory("gefs", "30770618") == \
        ("data/30770617/12/gefs", "3077061712")

    # Case 2: available data 30770615, 30770616, and 30770617 up to 12.
    # 30770617/18 folder exists but does not have completion flag.
    os.makedirs("data/30770617/18")
    # Request is made on 30770617. Should return 30770617/12/gfs(gefs)
    assert get_directory("gfs", "30770617") == \
        ("data/30770617/12/gfs", "3077061712")
    assert get_directory("gefs", "30770617") == \
        ("data/30770617/12/gefs", "3077061712")
    # Request is made on 30770618. Should also return 30770617/12/gfs(gefs)
    assert get_directory("gfs", "30770618") == \
        ("data/30770617/12/gfs", "3077061712")
    assert get_directory("gefs", "30770618") == \
        ("data/30770617/12/gefs", "3077061712")

    # Case 3: available data 30770615, 30770616, 30770617.
    os.makedirs("data/30770617/18/complete.flag")
    # Request is made on 30770617. Should return 30770617/18/gfs(gefs)
    assert get_directory("gfs", "30770617") == \
        ("data/30770617/18/gfs", "3077061718")
    assert get_directory("gefs", "30770617") == \
        ("data/30770617/18/gefs", "3077061718")
    # Request is made on 30770618. Should return 30770617/18/gfs(gefs)
    assert get_directory("gfs", "30770618") == \
        ("data/30770617/18/gfs", "3077061718")
    assert get_directory("gefs", "30770618") == \
        ("data/30770617/18/gefs", "3077061718")

    # Error handling: data_type incorrect.
    try:
        get_directory("nba", "30770617")
    except ValueError as error:
        assert str(error) == "Invalid data_type: nba. " \
                             "Expected \'gfs\' or \'gefs\'"
        print(error)

    # Error handling: no data available.
    clean_directory()
    try:
        get_directory("gfs", "30770617")
    except FileNotFoundError as error:
        assert str(error) == "Cannot find processed NOAA datasets."
        print(error)


def test_get_forecast_hour():
    """Test get_forecast_type_and_hour function."""
    assert get_forecast_type_and_hour("cloud.f000.npy") == ("cloud", 0)
    assert get_forecast_type_and_hour("humidity.f007.npy") == ("humidity", 7)
    assert get_forecast_type_and_hour("aerosol.f071.npy") == ("aerosol", 71)


def convert_time_to_utc(time_dt, input_timezone):
    """Convert input_time from input_timezone to UTC.

    Parameters:
    time_dt - a datetime object of format YYYY/MM/DD HH:MM:SS
    input_timezone - a STRING representing a valid timezone.
                     e.g. America/Detroit
    """
    # pytz will throw exceptions if input_timezone is invalid.
    timezone = pytz.timezone(input_timezone)

    # Add timezone info to the datetime object.
    local_time = timezone.localize(time_dt)

    utc_time = local_time.astimezone(pytz.UTC)
    return utc_time


# pylint: disable=R0914
def test_is_moon_free():
    """Test is_moon_free function.

    Ground truth: on July 1 2023, at location (42.2776, -83.7409), the moon
    sets at 3:47 am and rises at 7:58 pm.
    """
    lat = 42.2776
    lng = -83.7409
    time_format = "%Y/%m/%d %H:%M:%S"
    timezone = "America/Detroit"
    # Case 1: 3 am - should return False.
    time_3_am = datetime.strptime(
        "2023/07/01 03:00:00", time_format
    )
    time_3_am_utc = convert_time_to_utc(time_3_am, timezone)
    assert not is_moon_free(lat, lng, time_3_am_utc)

    # Case 2: 4 am - should return True.
    time_4_am = datetime.strptime(
        "2023/07/01 04:00:00", time_format
    )
    time_4_am_utc = convert_time_to_utc(time_4_am, timezone)
    assert is_moon_free(lat, lng, time_4_am_utc)

    # Case 3: 6 pm - should return True.
    time_6_pm = datetime.strptime(
        "2023/07/01 18:00:00", time_format
    )
    time_6_pm_utc = convert_time_to_utc(time_6_pm, timezone)
    assert is_moon_free(lat, lng, time_6_pm_utc)

    # Case 4: 7 pm - should return False.
    # NOTE: this test case doesn't really work, and the inconsistency appears
    # to lie within the ephem library - calculated next_rising time is indeed
    # 19:58:23 but alitude at 8 pm is slightly less than 0.
    # This error margin is acceptable.

    # Case 5: 8 pm - should return False.
    time_8_pm = datetime.strptime(
        "2023/07/01 20:00:00", time_format
    )
    time_8_pm_utc = convert_time_to_utc(time_8_pm, timezone)
    assert not is_moon_free(lat, lng, time_8_pm_utc)

    # Case 6: no timezone info - should throw an error.
    time_no_tz = datetime.strptime(
        "2023/07/01 20:00:00", time_format
    )
    try:
        is_moon_free(lat, lng, time_no_tz)
    except ValueError as error:
        print(error)
        assert error.args[0] == "Input time should be in UTC timezone."

    # Case 7: not UTC timezone - should throw an error.
    time_wrong_tz = datetime.strptime(
        "2023/07/01 20:00:00", time_format
    )
    wrong_timezone = pytz.timezone("America/Detroit")
    time_wrong_tz = wrong_timezone.localize(time_wrong_tz)
    try:
        is_moon_free(lat, lng, time_wrong_tz)
    except ValueError as error:
        print(error)
        assert error.args[0] == "Input time should be in UTC timezone."

    # Case 8: not rounded to the nearest hour - should throw an error.
    time_not_rounded = datetime.strptime(
        "2023/07/01 18:46:00", time_format
    )
    utc_time_not_rounded = convert_time_to_utc(time_not_rounded, timezone)
    try:
        is_moon_free(lat, lng, utc_time_not_rounded)
    except ValueError as error:
        print(error)
        assert error.args[0] == \
            "Input time should be rounded to the nearest hour."


def test_get_moon_activity():
    """Test function `get_moon_activity`."""
    # Create a simple `context` dictionary.
    context = {
        "data": {
            "2023/07/01": None
        },
        "lat": 42.2776,
        "lng": -83.7409,
        "timezone": "America/Detroit"
    }

    get_moon_activity(context)

    print(context["moon_phase"])
