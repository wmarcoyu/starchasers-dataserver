"""Create a meteor shower table that contains major meteor showers in a year.

Only class I meteor showers are included due to their high hourly rates. Since
there is only a handful of class I meteor showers and they only need to be
recorded once every year, it is the simplest to manually hardcode them.

Source: https://www.amsmeteors.org/meteor-showers/2020-meteor-shower-list/
"""
import json
import ephem


OUTPUT_FILE_PATH = "data/meteor_shower_table.json"


def create_shower(name, period, max_date, max_time, max_hourly_rate):
    """Create a dictionary that represents a meteor shower.

    Parameters:
    name - string
    period - tuple of strings (start, end), both in yyyy/mm/dd format
    max_date - string yyyy/mm/dd
    max_time - string, e.g. 0400
    max_hourly_rate - string that represents a number
    """
    shower = {
        "name": name, "period": period, "max_date": max_date,
        "max_time": max_time, "max_hourly_rate": max_hourly_rate
    }

    # Compute moon phase as percentage full.
    date = ephem.Date(max_date)
    moon = ephem.Moon()
    moon.compute(date)

    shower["moon"] = str(int(moon.phase))

    return shower


if __name__ == "__main__":
    """
    Each meteor shower is a dictionary with keys being:
    name, period, max_date, max_time, max_hourly_rate.
    """
    table = {}
    table["data"] = []
    table["source"] = "https://www.amsmeteors.org/meteor-showers/"

    # TODO: update year.
    table["year"] = "2023"

    # TODO: update meteor showers.
    qua = create_shower(
        "Quadrantids", ("2022/12/26", "2023/01/16"),
        "2023/01/04", "0500", "120"
    )

    lyr = create_shower(
        "Lyrids", ("2023/04/15", "2023/04/29"),
        "2023/04/23", "0400", "18"
    )

    eta = create_shower(
        "eta Aquarids", ("2023/04/15", "2023/05/27"),
        "2023/05/06", "0400", "60"
    )

    sda = create_shower(
        "Southern delta Aquarids", ("2023/07/18", "2023/08/21"),
        "2023/07/31", "0300", "20"
    )

    per = create_shower(
        "Perseids", ("2023/07/14", "2023/09/01"),
        "2023/08/13", "0400", "100"
    )

    ori = create_shower(
        "Orionids", ("2023/09/26", "2023/11/22"),
        "2023/10/21", "0500", "23"
    )

    leo = create_shower(
        "Leonids", ("2023/11/03", "2023/12/02"),
        "2023/11/18", "0500", "15"
    )

    gem = create_shower(
        "Geminids", ("2023/11/19", "2023/12/24"),
        "2023/12/14", "0100", "120"
    )

    urs = create_shower(
        "Ursids", ("2023/12/13", "2023/12/24"),
        "2023/12/22", "0500", "10"
    )

    # NOTE: add the first meteor shower in next year to avoid gaps at the
    # end of current year.
    next_qua = create_shower(
        "Quadrantids", ("2023/12/26", "2024/01/16"),
        "2024/01/03", "0500", "120"
    )

    table["data"].append(qua)
    table["data"].append(lyr)
    table["data"].append(eta)
    table["data"].append(sda)
    table["data"].append(per)
    table["data"].append(ori)
    table["data"].append(leo)
    table["data"].append(gem)
    table["data"].append(urs)
    table["data"].append(next_qua)

    # Convert to JSON and write to file.
    json_string = json.dumps(table, indent=4)
    with open(OUTPUT_FILE_PATH, "w", encoding="utf-8") as json_file:
        json_file.write(json_string)

    print(f"Meteor shower table update complete. Year: {table['year']}")
