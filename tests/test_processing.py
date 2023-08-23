"""Test data processing functions."""
import os
import shutil
import numpy as np
from dataserver.process import process_gfs_file, process_gefs_file, \
    delete_stale_data


HEIGHT, WIDTH = 721, 1440


def clean_tmp_directory():
    """Remove a directory."""
    if os.path.exists("tmp/20770101"):
        shutil.rmtree("tmp/20770101")


def test_process_gfs_file():
    """Test process_gfs_file which processes one GFS file."""
    # Create a temporary directory that stores a GFS file.
    clean_tmp_directory()

    os.makedirs("tmp/20770101/12/gfs")
    # Copy the file into the directory.
    os.system("cp data/20230613/* tmp/20770101/12/gfs/")

    file_path = "tmp/20770101/12/gfs/gfs.t12z.pgrb2.0p25.f004"
    assert os.path.exists(file_path)
    try:
        process_gfs_file(file_path)
    except:
        assert False  # should be successful

    # The directory should have a cloud file and a humidity file.
    assert os.path.exists("tmp/20770101/12/gfs/cloud.f004.npy")
    assert os.path.exists("tmp/20770101/12/gfs/humidity.f004.npy")
    # The raw file should be gone.
    assert not os.path.exists(file_path)

    # Cloud and humidity data should be valid - not all NaN.
    cloud_data = np.load("tmp/20770101/12/gfs/cloud.f004.npy")
    humidity_data = np.load("tmp/20770101/12/gfs/humidity.f004.npy")
    assert np.shape(cloud_data) == (HEIGHT, WIDTH)
    assert np.shape(humidity_data) == (HEIGHT, WIDTH)
    assert np.sum(np.isnan(cloud_data)) < HEIGHT * WIDTH
    assert np.sum(np.isnan(humidity_data)) < HEIGHT * WIDTH

    # Remove the temporary directory.
    shutil.rmtree("tmp/20770101")


def test_process_gefs_file():
    """Test the function that processes one GEFS file."""
    clean_tmp_directory()
    tmp_path = "tmp/20770101/12/gefs"
    filename = "gefs.chem.t12z.a2d_0p25.f000.grib2"
    os.makedirs(tmp_path)
    # Copy GEFS files to tmp_path.
    os.system(
        f"cp data/20230613/{filename} {tmp_path}"
    )
    assert os.path.exists(f"{tmp_path}/{filename}")
    process_gefs_file(f"{tmp_path}/{filename}")
    # Raw file should be gone.
    assert not os.path.exists(f"{tmp_path}/{filename}")
    # New files should have been created.
    assert os.path.exists(f"{tmp_path}/coarse.f000.npy")
    assert os.path.exists(f"{tmp_path}/fine.f000.npy")
    coarse_data = np.load(f"{tmp_path}/coarse.f000.npy")
    fine_data = np.load(f"{tmp_path}/fine.f000.npy")
    assert np.shape(fine_data) == (HEIGHT, WIDTH)
    assert np.shape(coarse_data) == (HEIGHT, WIDTH)
    assert np.sum(np.isnan(fine_data)) < HEIGHT * WIDTH
    assert np.sum(np.isnan(coarse_data)) < HEIGHT * WIDTH

    shutil.rmtree("tmp/20770101")


def count_file_lines(filename):
    """Count number of lines a file has."""
    line_count = 0
    with open(filename, "r", encoding="utf-8") as file:
        for _ in file:
            line_count += 1
    return line_count


def test_delete_stale_data():
    """Test delete_stale_data function."""
    os.mkdir("data/30770101")
    os.mkdir("data/30770102")
    os.mkdir("data/30770103")
    os.mkdir("data/30770104")
    assert os.path.exists("data/30770101")
    delete_stale_data("30770104")
    assert not os.path.exists("data/30770101")
    assert os.path.exists("data/30770102")
    assert os.path.exists("data/30770103")
    assert os.path.exists("data/30770102")
    os.rmdir("data/30770102")
    os.rmdir("data/30770103")
    os.rmdir("data/30770104")


if __name__ == "__main__":
    pass
