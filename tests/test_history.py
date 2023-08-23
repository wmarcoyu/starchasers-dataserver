"""Test history.py which calculates historical/average sky transparency."""
import os
import shutil
import subprocess
from datetime import datetime, timedelta
import numpy as np
from dataserver import history


# pylint: disable=C0103
def test_get_median_array():
    """Test function `get_median_array`."""
    arr_A = np.array([
        [1, 2],
        [2, 1]
    ])
    arr_B = np.array([
        [2, 1],
        [2, 2]
    ])
    arr_C = np.array([
        [0, 3],
        [2, 3]
    ])
    directory = "tmp/median"
    os.makedirs(directory)
    np.save(f"{directory}/arrA.npy", arr_A)
    np.save(f"{directory}/arrB.npy", arr_B)
    np.save(f"{directory}/arrC.npy", arr_C)
    full_paths = ["tmp/median/arrA.npy",
                  "tmp/median/arrB.npy",
                  "tmp/median/arrC.npy"]
    result = history.get_median_array(full_paths)
    median = np.array([
        [1, 2],
        [2, 2]
    ])
    assert np.array_equal(result, median)
    shutil.rmtree("tmp/median")


# pylint: disable=W0212
def test_prepare_urls():
    """Test class method `_prepare_urls`."""
    # Initiate a HistoryTransparenncy instance, which executes __init__.
    instance = history.HistoryTransparency(2021, 2024, 6)
    instance._prepare_urls("GFS")
    instance._prepare_urls("GEFS")
    # First and last urls in GFS and GEFS urls should exist. If so, urls in
    # between are guaranteed to exist as well.
    first_gfs, last_gfs = instance.gfs_urls[0], instance.gfs_urls[-1]
    first_gefs, last_gefs = instance.gefs_urls[0], instance.gefs_urls[-1]
    urls = [first_gfs, last_gfs, first_gefs, last_gefs]
    print(urls)
    # Check that they exist.
    for url in urls:
        result = subprocess.run(
            f"aws s3 ls --no-sign-request {url}",
            shell=True, capture_output=True, text=True, check=True
        )
        output = result.stdout
        assert len(output) > 0


def test_download_and_process_files():
    """Test class methods `_download_files` and `_process_files`.

    This function effectively tests the whole functionality of
    class HistoryTransparency with a small sample of data.
    """
    # Manually prepare one GFS file and two GEFS files.
    instance = history.HistoryTransparency(2021, 2024, 6)
    today = datetime.now()
    t_minus_one = today - timedelta(days=1)
    t_minus_one = t_minus_one.strftime("%Y%m%d")
    instance.gfs_urls = [
        f"s3://noaa-gfs-bdp-pds/gfs.{t_minus_one}/12/atmos/"
        "gfs.t12z.pgrb2.0p25.f000"
    ]
    instance.gefs_urls = [
        f"s3://noaa-gefs-pds/gefs.{t_minus_one}/12/chem/pgrb2ap25/"
        "gefs.chem.t12z.a2d_0p25.f000.grib2",
        f"s3://noaa-gefs-pds/gefs.{t_minus_one}/18/chem/pgrb2ap25/"
        "gefs.chem.t18z.a2d_0p25.f000.grib2"
    ]
    # Remove June data and recreate the directory.
    try:
        shutil.rmtree("history_data/6")
    except:
        pass
    os.makedirs("history_data/6/gfs")
    os.makedirs("history_data/6/gefs")
    # Download the files.
    instance._download_files()
    # Check the existence of files.
    assert os.path.exists("history_data/6/gfs/cloud.f000.npy")
    assert os.path.exists("history_data/6/gfs/humidity.f000.npy")
    assert os.path.exists("history_data/6/gefs/fine.f000.npy")
    assert os.path.exists("history_data/6/gefs/fine.f001.npy")
    # Process the files.
    instance._process_files()
    assert os.path.exists("history_data/6/cloud.npy")
    assert os.path.exists("history_data/6/humidity.npy")
    assert os.path.exists("history_data/6/aerosol.npy")
    # Check data value.
    cloud_raw = np.load("history_data/6/gfs/cloud.f000.npy")
    cloud_res = np.load("history_data/6/cloud.npy")
    humidity_raw = np.load("history_data/6/gfs/humidity.f000.npy")
    humidity_res = np.load("history_data/6/humidity.npy")
    aerosol_one = np.load("history_data/6/gefs/fine.f000.npy")
    aerosol_two = np.load("history_data/6/gefs/fine.f001.npy")
    aerosol_res = np.load("history_data/6/aerosol.npy")
    tolerance = 1e-5
    assert np.allclose(cloud_raw, cloud_res, tolerance)
    assert np.allclose(humidity_raw, humidity_res, tolerance)
    assert np.allclose((aerosol_one + aerosol_two) / 2, aerosol_res, tolerance)
    # Clean up the directory
    shutil.rmtree("history_data/6")
    os.makedirs("history_data/6/gfs")
    os.makedirs("history_data/6/gefs")


def test_get_transparency():
    """Test function `_get_transparency`."""
    for i in range(1, 13):
        instance = history.HistoryTransparency(2021, 2024, i)
        instance._get_transparency()
