"""Functions that process downloaded datasets."""
import os
import shutil
from datetime import datetime, timedelta
import numpy as np
import pygrib
from dataserver.logger import logger


MAX_WORKERS = 1
INT_MAX = 2**32 - 1
DATA_RETENTION_DAYS = 2


def process(current_date, current_time):
    """Process downloaded data.

    It extracts information (TCDC, RH, aerosol) from downloaded datasets,
    deletes raw data, and deletes stale data from 3 days ago.
    """
    logger.info(
        "%s/%s data processing starts.", current_date, current_time
    )

    # Mark the processing as complete.
    completion_flag = f"data/{current_date}/{current_time}/complete.flag"
    with open(completion_flag, "w", encoding="utf-8") as file:
        timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        file.write(
            f"Complete at {timestamp} UTC."
        )

    if current_time == "18":
        delete_stale_data(current_date)

    logger.info(
        "%s/%s data processing complete.", current_date, current_time
    )


def process_gfs_file(file_path):
    """Process one GFS file.

    This function:
        - extracts TCDC (total cloud cover) and RH (relative humidity).
        - deletes original (large) file.
    """
    gribs = pygrib.open(file_path)
    selected_cloud_gribs = gribs.select(
        name="Total Cloud Cover", typeOfLevel="atmosphere"
    )
    selected_humidity_gribs = gribs.select(
        name="Relative humidity", typeOfLevel="atmosphereSingleLayer"
    )

    path_segments = file_path.split("/")
    filename = path_segments[-1]
    name_segments = filename.split(".")
    forecast_hour = name_segments[-1]  # gfs.f{hour:03d}
    directory_path = os.path.dirname(file_path)
    if len(directory_path) == 0:
        directory_path = "."

    process_selected_gribs(
        selected_cloud_gribs, "cloud", directory_path, forecast_hour
    )
    process_selected_gribs(
        selected_humidity_gribs, "humidity", directory_path, forecast_hour
    )

    os.remove(file_path)


def process_gefs_file(file_path):
    """Process GEFS datasets.

    This function:
        - extracts aerosol.
        - deletes raw data.
    """
    gribs = pygrib.open(file_path)

    # INFO: parameterName 102: AOTK - aerosol optical thickness
    # INFO: aerosolType 62000: total aerosol
    # INFO: Each AOTK data has a range, from `scaledValueOfFirstWavelength` to
    # INFO: `scaledValueOfSecondWavelength` in nanometers (nm).

    # First select all AOTK data in visible light range.
    selected_gribs = gribs.select(
        parameterName="102", aerosolType=62000,
        scaledValueOfFirstWavelength=lambda x: 400 <= x <= 700,
        scaledValueOfSecondWavelength=lambda x: 400 <= x <= 700
    )
    if len(selected_gribs) == 0:
        raise ValueError(f"No available aerosol data: {file_path}")

    # Select data that corresponds to 555 nm, which affects sky transparency
    # the most. If 555 nm data is not available, select the first one
    # in visible light range. Note that `filtered_grib` is a list.
    filtered_grib = [grib for grib in selected_gribs if
                     grib.scaledValueOfFirstWavelength == 545]
    if len(filtered_grib) == 0:
        filtered_grib.append(selected_gribs[0])

    # Process file path.
    path_segments = file_path.split("/")
    filename = path_segments[-1]
    name_segments = filename.split(".")
    forecast_hour = name_segments[-1]  # gefs.f{hour:03d}
    directory_path = os.path.dirname(file_path)
    if len(directory_path) == 0:
        directory_path = "."

    # Note that the first parameter should be a list.
    process_selected_gribs(
        filtered_grib, "aerosol", directory_path, forecast_hour
    )

    os.remove(file_path)


def process_selected_gribs(gribs, gribs_type, directory_path, forecast_hour):
    """Select the grib with the least number of NaN entries.

    Parameters:
    gribs - a list of selected gribs that may contain more than one grib.
        Select the grib with the least number of NaN entires.

    directory_path - path to the directory where the processed data should
        reside in. e.g. data/20770101/06/gfs

    gribs_type - cloud, humidity, or aersol

    forecast_hour - a string that represents the hour of forecast in 3-digit
        format. The entire path of the processed data looks like:
        data/20770101/06/gfs/cloud.006.npy if gribs_type == cloud and
        forecast_hour == f006.
    """
    if len(gribs) == 0:
        raise ValueError(
            f"No available data: {directory_path}/{gribs_type}.{forecast_hour}"
        )
    min_nan_entries = INT_MAX
    selected_data = None
    for grib in gribs:
        num_nan_entries = np.sum(np.isnan(grib.values))
        if num_nan_entries < min_nan_entries:
            min_nan_entries = num_nan_entries
            selected_data = grib.values
    height, width = np.shape(selected_data)
    if min_nan_entries == height * width:
        raise ValueError(
            f"All entries in {directory_path}/{gribs_type}.{forecast_hour} "
            "are NaN."
        )
    np.save(
        f"{directory_path}/{gribs_type}.{forecast_hour}.npy", selected_data
    )


def delete_stale_data(current_date):
    """Delete stale data (entire directory) from 3 days ago.

    Parameter:
    current_date - a STRING that represents current date in YYYYMMDD format.
    """
    given_date = datetime.strptime(current_date, "%Y%m%d")
    target_date = given_date - timedelta(days=DATA_RETENTION_DAYS)
    target_date = target_date.strftime("%Y%m%d")
    logger.info("Start deleting data for %s.", target_date)
    try:
        shutil.rmtree(f"data/{target_date}")
    except FileNotFoundError as _:
        logger.warning("Directory not found: %s", target_date)
    logger.info("%s data deletion complete.", target_date)
