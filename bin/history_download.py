"""Driver to download and process history data."""
from dataserver.history import HistoryTransparency


START_YEAR = 2022
END_YEAR = 2023


def download_process_one_month(month):
    """Download and process data of month `month`.

    `month` has been checked to be valid.
    """
    month = int(month)
    # Just stop on exception.
    downloader = HistoryTransparency(START_YEAR, END_YEAR, month)
    downloader.start()


if __name__ == "__main__":
    input_month = input("Which month ([1-12/all(a)]): ")
    valid_months = [i for i in range(1, 13)]
    if input_month in ("all", "a"):
        for month in range(1, 13):
            download_process_one_month(month)  # month is an int
    elif int(input_month) in valid_months:
        download_process_one_month(int(input_month))
    else:
        raise ValueError(f"Invalid input: {input_month}")

    if input_month in ("all", "a"):
        input_month = "All"

    print(f"{input_month} history data downloaded and processed.")
