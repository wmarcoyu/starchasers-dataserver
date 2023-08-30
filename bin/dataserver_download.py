"""Script that automatically downloads data from NOAA four times a day.

NOAA GFS: https://registry.opendata.aws/noaa-gfs-bdp-pds/
NOAA GEFS: https://registry.opendata.aws/noaa-gefs/
"""
import concurrent.futures
from datetime import datetime
import time
import os
import subprocess
import schedule
from ratelimiter import RateLimiter
from dataserver.logger import logger
from dataserver.process import process, process_gfs_file, process_gefs_file


MAX_WORKERS = 1
REQUEST_INTERVAL = 6
SCHEDULE_CHECK_INTERVAL = 300
RETRY_TIME = 1800
MAX_RETRY = 10
MAX_PREDICTION_HOURS = 72
rate_limiter = RateLimiter(max_calls=MAX_WORKERS, period=REQUEST_INTERVAL)


def get_current_time(current_date):
    """Return the latest time of data upload based on current date.

    Result is one of the following: 00/06/12/18.

    If the folder for current_date, e.g. 20770101, does not exist, then
    latest time is 00. Otherwise, it returns the first non-existent time in
    the folder.

    Before it returns the result, it creates the resulting directories.

    Parameter:
    current_date - YYYYMMDD.
    """
    working_directory = os.getcwd()
    directory = f"{working_directory}/data/{current_date}"
    # Folder for current_date doesn't exist, time is 00.
    if not os.path.exists(directory):
        os.mkdir(directory)
        os.mkdir(f"{directory}/00")
        return "00"
    # Return the first non-existent time.
    times = ["06", "12", "18"]
    for timestamp in times:
        next_directory = f"{directory}/{timestamp}"
        if not os.path.exists(next_directory):
            os.mkdir(next_directory)
            return timestamp
    raise ValueError("Current directory has all four times.")


def get_urls(current_date, current_time):
    """Format target database urls given current date and time."""
    gfs_url = f"s3://noaa-gfs-bdp-pds/gfs.{current_date}/{current_time}/atmos"
    gefs_url = \
        f"s3://noaa-gefs-pds/gefs.{current_date}/{current_time}/chem/pgrb2ap25"
    urls = {"gfs": gfs_url, "gefs": gefs_url}
    return urls


def downloader(file_transfer_info, file_list):
    """Download one dataset.

    This is a worker thread that downloads only one file. Upon success, it
    extracts the information from the file, removes the large raw file, and
    removes the file info from the list of all files.

    Parameters:
    file_transfer_info - (src_url, dest_dir) a tuple of source url and
        destination directory path.
    file_list - a SET of file_transfer_info. Current info is removed after
        download is successful.
    """
    with rate_limiter:
        src_url, dest_dir = file_transfer_info
        src_url = str(src_url)
        dest_dir = str(dest_dir)
        try:
            subprocess.check_output(
                ["aws", "s3", "cp", "--no-sign-request", src_url, dest_dir],
                stderr=subprocess.STDOUT
            )
            # NOTE: process the file right after download to save storage space
            # pylint: disable=C0207
            filename = dest_dir.split("/")[-1]  # gfs.fxxx or gefs.fxxx
            file_type = filename.split(".")[0]
            if file_type == "gfs":
                process_gfs_file(str(dest_dir))
            else:
                assert file_type == "gefs"
                process_gefs_file(str(dest_dir))
            # Current file is finished downloading and processing.
            file_list.remove(file_transfer_info)
        except subprocess.CalledProcessError as error:
            logger.exception(
                f"Error occurred: {error.output.decode('utf-8')}\n"
                f"Source file: {src_url}"
            )
        except Exception as error:
            logger.exception(
                f"Unexpected error: {error}\n"
                f"Source file: {src_url}"
            )


def download_data():
    """Download all target files.

    Locate directories that contain the latest data we need. Download hourly
    prediction data up to 72 hours in the future.
    """
    current_date = datetime.now().date().strftime("%Y%m%d")
    current_time = get_current_time(current_date)

    logger.info(
        f"Start downloading data for {current_date}/{current_time}"
    )

    os.mkdir(f"data/{current_date}/{current_time}/gfs")
    os.mkdir(f"data/{current_date}/{current_time}/gefs")

    urls = get_urls(current_date, current_time)
    gfs_url, gefs_url = urls["gfs"], urls["gefs"]

    # GFS data are hourly predictions.
    gfs_file_transfer_info = \
        [(f"{gfs_url}/gfs.t{current_time}z.pgrb2.0p25.f{hour:03d}",
          f"data/{current_date}/{current_time}/gfs/gfs.f{hour:03d}")
         for hour in range(MAX_PREDICTION_HOURS)]
    # GEFS data have 3-hour intervals.
    gefs_file_transfer_info = \
        [(f"{gefs_url}/gefs.chem.t{current_time}z.a2d_0p25.f{hour:03d}."
          "grib2",
          f"data/{current_date}/{current_time}/gefs/gefs.f{hour:03d}")
         for hour in range(0, MAX_PREDICTION_HOURS, 3)]

    file_list = set(gfs_file_transfer_info) | set(gefs_file_transfer_info)

    retry_counter = 0
    while len(file_list) > 0 and retry_counter < MAX_RETRY:
        # Copy file list for enumeration to avoid undefined behavior.
        file_list_copy = file_list.copy()
        with concurrent.futures.ThreadPoolExecutor(
          max_workers=MAX_WORKERS) as executor:
            # Each future inside list _ is guaranteed success since exceptions
            # are handled inside downloader function.
            _ = [executor.submit(
                downloader, file_transfer_info, file_list
            ) for file_transfer_info in file_list_copy]
        if len(file_list) == 0:
            logger.info(
                f"{current_date}/{current_time} download complete."
            )
            process(current_date, current_time)
            return
        retry_counter += 1
        logger.warning(
            f"{current_date}/{current_time} retry # {retry_counter}"
        )
        time.sleep(RETRY_TIME)
    logger.critical(
        f"{current_date}/{current_time} download failed."
    )


if __name__ == "__main__":
    # WARNING: Data for 00:00, 06:00, 12:00, 18:00 UTC, which are
    # uploaded around 4 hours later. Default timezone used by AWS EC2
    # is UTC, whereas local machines have default timezones corresponding
    # to their local timezones.
    for job_time in ["04:00", "10:00", "16:00", "22:00"]:
        schedule.every().day.at(job_time).do(download_data)

    while True:
        schedule.run_pending()
        time.sleep(SCHEDULE_CHECK_INTERVAL)  # check for job every 5 minutes
