"""Python script to create test files.

data/20230613 directory has two GFS files and two GEFS files for testing.
This script will load these files, duplicate them, and store duplicated files
in 30770617/12/gfs and 30770617/12/gefs, respectively.
"""
import os
import shutil
from dataserver.process import process_gfs_file, process_gefs_file


def copy_files_to_tmp():
    """Copy the GFS and GEFS files to tmp/.

    This ensures that original files won't get deleted.
    """
    # Create GFS and GEFS directories in tmp/.
    try:
        os.mkdir("tmp/gfs")
        os.mkdir("tmp/gefs")
    except FileExistsError as _:
        print("Directories already created.")

    # Copy files into correct directories.
    gfs_info = [
        ("data/20230613/gfs.t12z.pgrb2.0p25.f004", "tmp/gfs/gfs.f004"),
        ("data/20230613/gfs.t12z.pgrb2.0p25.f005", "tmp/gfs/gfs.f005")
    ]
    gefs_info = [
        ("data/20230613/gefs.chem.t12z.a2d_0p25.f000.grib2",
         "tmp/gefs/gefs.f000"),
        ("data/20230613/gefs.chem.t12z.a2d_0p25.f009.grib2",
         "tmp/gefs/gefs.f009")
    ]
    for (src, dst) in gfs_info:
        shutil.copy(src, dst)
    for (src, dst) in gefs_info:
        shutil.copy(src, dst)


def process_files():
    """Process files by calling functions in dataserver.process module."""
    def process_directory(directory_root, function):
        """Process a directory with input `function`."""
        filenames = os.listdir(directory_root)
        for each_file in filenames:
            full_path = f"{directory_root}/{each_file}"
            print(f"Processing file {full_path}.")
            function(full_path)
    process_directory("tmp/gfs", process_gfs_file)
    process_directory("tmp/gefs", process_gefs_file)


def duplicate_files():
    """Duplicate processed files according to their parity.

    E.g. cloud.f004.npy will be duplicated to cloud.f000.npy, cloud.f002.npy,
    ..., cloud.f070.npy, and cloud.f005.npy will be duplicated to
    cloud.f001.npy, cloud.f003.npy, ..., cloud.f071.npy.
    """
    try:
        os.makedirs("data/30770617/12/gfs")
        os.makedirs("data/30770617/12/gefs")
    except FileExistsError as _:
        print("Destination directories already created.")

    cloud_even_source = "tmp/gfs/cloud.f004.npy"
    cloud_odd_source = "tmp/gfs/cloud.f005.npy"
    humidity_even_source = "tmp/gfs/humidity.f004.npy"
    humidity_odd_source = "tmp/gfs/humidity.f005.npy"
    gfs_destination = "data/30770617/12/gfs"

    aerosol_even_source = "tmp/gefs/aerosol.f000.npy"
    aerosol_odd_source = "tmp/gefs/aerosol.f009.npy"
    gefs_destination = "data/30770617/12/gefs"

    for hour in range(72):
        file_suffix = f"f{hour:03d}.npy"
        if hour % 2 == 0:
            shutil.copy(
                cloud_even_source,
                f"{gfs_destination}/cloud.{file_suffix}"
            )
            shutil.copy(
                humidity_even_source,
                f"{gfs_destination}/humidity.{file_suffix}"
            )
            shutil.copy(
                aerosol_even_source,
                f"{gefs_destination}/aerosol.{file_suffix}"
            )
        else:
            shutil.copy(
                cloud_odd_source,
                f"{gfs_destination}/cloud.{file_suffix}"
            )
            shutil.copy(
                humidity_odd_source,
                f"{gfs_destination}/humidity.{file_suffix}"
            )
            shutil.copy(
                aerosol_odd_source,
                f"{gefs_destination}/aerosol.{file_suffix}"
            )


def write_complete_flag():
    """Mark the processing as complete."""
    with open("data/30770617/12/complete.flag", "w", encoding="utf-8") as file:
        file.write("Testing files processing complete.")


def clean_directory():
    """Remove processed files in directory tmp/ to start over."""
    try:
        shutil.rmtree("tmp/gfs")
        shutil.rmtree("tmp/gefs")
    except FileNotFoundError as _:
        print("Directories already removed. Ready to start processing.")


if __name__ == "__main__":
    """Run the procedure."""
    clean_directory()
    copy_files_to_tmp()
    process_files()
    duplicate_files()
    write_complete_flag()
