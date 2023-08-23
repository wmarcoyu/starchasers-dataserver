"""Test automatic download script."""
import concurrent.futures
import os
import subprocess
import time
import requests
import pytest
from ratelimiter import RateLimiter
import bin.dataserver_download as dld


def test_get_current_time():
    """Test get_current_time function.

    The function should find the correct time(00/06/12/18) based on input date,
    create the directory, and return the time.
    """
    working_directory = os.getcwd()
    date = "20770101"
    assert not os.path.exists(f"{working_directory}/data/{date}")
    # create directories and return correct times
    assert dld.get_current_time(date) == "00"
    assert os.path.exists(f"{working_directory}/data/{date}/00")
    assert dld.get_current_time(date) == "06"
    assert os.path.exists(f"{working_directory}/data/{date}/06")
    assert dld.get_current_time(date) == "12"
    assert os.path.exists(f"{working_directory}/data/{date}/12")
    assert dld.get_current_time(date) == "18"
    assert os.path.exists(f"{working_directory}/data/{date}/18")

    try:
        dld.get_current_time(date)
    except ValueError as error:
        assert str(error) == "Current directory has all four times."

    # remove the directory
    os.system("rm -r data/20770101")


def test_get_urls():
    """Test get_urls.

    This function should take a few seconds.
    """
    current_date = 20230608
    current_time = 12
    urls = dld.get_urls(current_date, current_time)
    gfs, gefs = urls["gfs"], urls["gefs"]
    # Trailing slashes are required after the urls.
    os.system(f"aws s3 ls --no-sign-request {gfs}/ > tmp/gfs_content.txt")
    os.system(f"aws s3 ls --no-sign-request {gefs}/ > tmp/gefs_content.txt")

    assert os.path.getsize("tmp/gfs_content.txt") > 0
    assert os.path.getsize("tmp/gefs_content.txt") > 0

    # Show content and remove files afterwards.
    os.system("head -n 10 tmp/gfs_content.txt; rm tmp/gfs_content.txt")
    os.system("head -n 10 tmp/gefs_content.txt; rm tmp/gefs_content.txt")


# ----------------------------------------------------------------------------
# Test downloader and download_data with localhost and simplified code.
# ----------------------------------------------------------------------------

rate_limiter = RateLimiter(max_calls=5, period=5)


def downloader_simple(num, file_list):
    """Download a dataset from src_url to dest_dir.

    This is a worker thread that downloads only one file.
    """
    with rate_limiter:
        try:
            response = requests.get("http://localhost:8000", timeout=3)
            print(
                f"Downloader {num} compelete. Status {response.status_code}"
            )
            file_list.remove(num)
        except subprocess.CalledProcessError as error:
            print(f"Error occurred: {error.output.decode('utf-8')}")
        except Exception as error:
            print(f"Unexpected error: {error}")


def download_data_simple(file_list):
    """Download all datasets needed."""
    retry_counter = 0
    while len(file_list) > 0 and retry_counter < 3:
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=5
        ) as executor:
            _ = [executor.submit(
                downloader_simple, num, file_list
            ) for num in file_list]
        retry_counter += 1
        if len(file_list) == 0:
            print("All work done. Exiting...")
            break
        print("Sleep before retry.")
        time.sleep(5)
    if len(file_list) > 0:
        assert retry_counter == 3
        raise ValueError("Failed to download after maximum retry.")


@pytest.mark.skip
def test_download_success():
    """Test successful download.

    Keep the server running.
    """
    num_list = [range(20)]
    file_list = set(num_list)
    download_data_simple(file_list)
    assert len(file_list) == 0


@pytest.mark.skip
def test_download_retry_success():
    """Test successful download with retry.

    Stop the server during download and restart.
    """
    num_list = [range(20)]
    file_list = set(num_list)
    download_data_simple(file_list)
    assert len(file_list) == 0


@pytest.mark.skip
def test_download_failed():
    """Test unsuccessful downloads.

    Stop the server in the middle of downloads.
    """
    num_list = [range(20)]
    file_list = set(num_list)
    try:
        download_data_simple(file_list)
    except Exception as error:
        assert str(error) == "Failed to download after maximum retry."
    assert len(file_list) > 0
