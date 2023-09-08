"""Get historical data to analyze average sky transparency.

Get historical data exactly once before deployment.
"""
import concurrent.futures
import os
import subprocess
from datetime import datetime, timedelta
import numpy as np
from dataserver.config import SERVER_ROOT
from dataserver import model
from bin.dataserver_download import downloader


# Cloud cover and relative humidity
GFS_PATH = "s3://noaa-gfs-bdp-pds"
GFS_FILENAME = "gfs.t12z.pgrb2.0p25.f000"
# Total aerosol
GEFS_PATH = "s3://noaa-gefs-pds"
GEFS_FILENAME = "gefs.chem.t12z.a2d_0p25.f000.grib2"
# Time span: 12 years (right exclusive)
DATA_TIME = 12
# Maximum number of `downloader` threads
MAX_WORKERS = 1


# pylint: disable=too-few-public-methods
class HistoryTransparency:
    """Get average sky transparency data for a given month."""

    def __init__(self, start_year, end_year, input_month):
        """Initialize the class.

        Note that [start_year, end_year) is left-inclusive and right-exclusive.
        """
        # Check input years.
        check_start_end_years(start_year, end_year)
        self.start_year = start_year
        self.end_year = end_year
        # Check input month.
        check_input_month(input_month)
        self.month = input_month

        self.gfs_urls, self.gefs_urls = [], []

    def start(self):
        """Start downloading and processing files."""
        # Prepare urls for files to download.
        self._prepare_urls("GFS")
        self._prepare_urls("GEFS")
        # Download (and preprocess) files.
        self._download_files()
        # Process downloaded files.
        self._process_files()
        # Compute transparency quality.
        self._get_transparency()
        print(f"Month {self.month} data processing complete.")

    def _prepare_urls(self, url_type):
        """Prepare urls for files to download.

        url_type is either GFS or GEFS.
        """
        print(f"Start preparing {url_type} urls.")
        # Get all days in the month which are used to construct urls.
        all_days_in_month = []
        for year in range(self.start_year, self.end_year):
            all_days_in_month += get_days_in_month(year, self.month)
        urls = []
        # Construct urls based on url_type and add them to a list.
        if url_type == "GFS":
            for day in all_days_in_month:
                url = f"{GFS_PATH}/gfs.{day}/{DATA_TIME}/atmos/{GFS_FILENAME}"
                urls.append(url)
        elif url_type == "GEFS":
            for day in all_days_in_month:
                url = f"{GEFS_PATH}/gefs.{day}/{DATA_TIME}/"\
                    f"chem/pgrb2ap25/{GEFS_FILENAME}"
                urls.append(url)
        else:
            raise ValueError(
                f"Invalid url_type: {url_type}. "
                "url_type is either GFS or GEFS."
            )
        # Check if each url exists. NOTE: it is only possible that urls
        # at the front and the end of the list are missing.
        # (urls at the front are missing because they are too old to be kept
        # in the database; urls at the end are missing because they are
        # in the distant future and do not exist yet.)
        # The contiguous chunk of urls in the middle is guaranteed to exist.
        for url in urls:
            # False if current url exists and is NOT removed.
            url_is_removed = remove_nonexistence_file(url, urls)
            if not url_is_removed:
                # We've found the first existing url in the list.
                break

        for url in reversed(urls):
            url_is_removed = remove_nonexistence_file(url, urls)
            if not url_is_removed:
                # We've found the last existing url in the list.
                break

        if url_type == "GFS":
            self.gfs_urls = urls
        else:
            self.gefs_urls = urls
        print(f"Finish preparing {url_type} urls.")

    def _download_files(self):
        """Download GFS and GEFS data.

        Call function `downloader` in bin/dataserver_download.py.
        """
        print(f"Start downloading files for month {self.month}.")
        # Construct GFS file_transfer_info.
        gfs_file_transfer_info = []
        for idx, url in enumerate(self.gfs_urls):
            # Since all files have the same name `gfs.t12z.pgrb2.0p25.f000`
            # we rename them as `gfs.f{idx}`. Values of `idx` don't matter.
            dest_dir = SERVER_ROOT / "history_data" / f"{self.month}" / \
                "gfs" / f"gfs.f{idx:03d}"
            gfs_file_transfer_info.append((url, dest_dir))
        # Construct GEFS file_transfer_info.
        gefs_file_transfer_info = []
        for idx, url in enumerate(self.gefs_urls):
            dest_dir = SERVER_ROOT / "history_data" / f"{self.month}" / \
                "gefs" / f"gefs.f{idx:03d}"
            gefs_file_transfer_info.append((url, dest_dir))
        # Join the two lists into a set.
        file_list = set(gfs_file_transfer_info) | \
            set(gefs_file_transfer_info)
        file_list_copy = file_list.copy()
        # Download files with `downloader` threads.
        with concurrent.futures.ThreadPoolExecutor(
                max_workers=MAX_WORKERS) as executor:
            _ = [executor.submit(
                downloader, file_transfer_info, file_list
            ) for file_transfer_info in file_list_copy]

    def _process_files(self):
        """Download and preprocess one file.

        Call function `process_data_directory` in dataserver/process.py.
        """
        # Get cloud and humidity file full paths.
        gfs_filenames = os.listdir(
            SERVER_ROOT/"history_data"/f"{self.month}/gfs"
        )
        cloud_filenames = [filename for filename in gfs_filenames
                           if filename[0] == "c"]
        humidity_filenames = [filename for filename in gfs_filenames
                              if filename[0] == "h"]
        cloud_file_paths = [
            SERVER_ROOT/"history_data"/f"{self.month}"/"gfs"/f"{filename}"
            for filename in cloud_filenames
        ]
        humidity_file_paths = [
            SERVER_ROOT/"history_data"/f"{self.month}"/"gfs"/f"{filename}"
            for filename in humidity_filenames
        ]

        # Get aerosol file full paths.
        aerosol_filenames = os.listdir(
            SERVER_ROOT/"history_data"/f"{self.month}"/"gefs"
        )
        aerosol_file_paths = [
            SERVER_ROOT/"history_data"/f"{self.month}"/"gefs"/f"{filename}"
            for filename in aerosol_filenames
        ]
        # Get the median arrays.
        cloud_median = get_median_array(cloud_file_paths)
        humidity_median = get_median_array(humidity_file_paths)
        aerosol_median = get_median_array(aerosol_file_paths)
        # Save the results.
        directory = str(SERVER_ROOT/"history_data"/f"{self.month}")
        np.save(f"{directory}/cloud.npy", cloud_median)
        np.save(f"{directory}/humidity.npy", humidity_median)
        np.save(f"{directory}/aerosol.npy", aerosol_median)

    def _get_transparency(self):
        """Compute transparency based on cloud, humidity and aerosol."""
        # Load the conversion table.
        table = np.load(SERVER_ROOT/"data"/"sky_transparency_table.npy")
        # Load cloud, humidity, and aerosol data.
        current_month_directory = SERVER_ROOT/"history_data"/f"{self.month}"
        cloud = np.load(current_month_directory/"cloud.npy")
        humidity = np.load(current_month_directory/"humidity.npy")
        aerosol = np.load(current_month_directory/"aerosol.npy")
        # Compute the results.
        height, width = np.shape(cloud)
        result = np.zeros(shape=((height, width)))
        for row in range(height):
            for col in range(width):
                cloud_idx = model.get_cloud_humidity_index(cloud[row][col])
                humidity_idx = model.get_cloud_humidity_index(
                    humidity[row][col]
                )
                aerosol_idx = model.get_aerosol_index(aerosol[row][col])
                # Look up the conversion table.
                result[row][col] = table[cloud_idx][humidity_idx][aerosol_idx]
        # Save the result as int type.
        result = result.astype(int)
        np.save(current_month_directory/"transparency.npy", result)


# ========================= helper functions below ============================
def get_days_in_month(year, month):
    """List all days in the given month.

    year and month should be integers.
    """
    start_date = datetime(year, month, 1)

    # Calculate the number of days in the month
    if month == 12:
        end_date = datetime(year + 1, 1, 1)
    else:
        end_date = datetime(year, month + 1, 1)

    # Subtract one day to get the last day of the current month
    end_date -= timedelta(days=1)

    # Generate a list of all the days in the month in YYYYMMDD format.
    all_days = []
    current_date = start_date
    while current_date <= end_date:
        all_days.append(current_date.date().strftime("%Y%m%d"))
        current_date += timedelta(days=1)

    return all_days


def check_input_month(input_month):
    """Check the validity of input_month.

    input_month has to be an integer from 1 to 12.
    """
    if not isinstance(input_month, int) or input_month < 1 or input_month > 12:
        raise ValueError(
            f"Invalid input_month: {input_month}. "
            "Has to be an integer from 1 to 12."
        )


def check_input_year(input_year):
    """Check the validity of input_year.

    input_year has to be an integer that is at least 2021.
    """
    if not isinstance(input_year, int) or input_year < 2021:
        raise ValueError(
            f"Invalid input year: {input_year}. "
            "Has to be an integer at least 2021."
        )


def check_start_end_years(start_year, end_year):
    """Check the validity of start_year and end_year.

    Both have to be valid and additionally end_year > start_year.
    """
    check_input_year(start_year)
    check_input_year(end_year)
    if start_year >= end_year:
        raise ValueError(
            "end_year has to be greater than start_year."
        )


def remove_nonexistence_file(file_url, all_urls):
    """Check if file_url exists and remove it from all_urls if not.

    Return True if a non-existent url is removed and return False otherwise.
    """
    result = subprocess.run(
                f"aws s3 ls --no-sign-request {file_url}",
                shell=True, capture_output=True, text=True, check=True
            )
    # Remove the url if it does not exist.
    output = result.stdout
    if len(output) == 0:
        all_urls.remove(file_url)
        # A file has indeed been removed.
        return True
    # The file exists and is not removed.
    return False


def get_median_array(all_file_paths):
    """Load all numpy arrays and return the median array.

    Parameter:
    all_file_paths - a list of file full paths.
    """
    array_list = [np.load(file_path) for file_path in all_file_paths]
    stacked_array = np.stack(array_list, axis=0)
    return np.median(stacked_array, axis=0)
