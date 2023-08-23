"""Fetch transparency data from dataserver."""
import subprocess


USERNAME = "admin"
with open("api_token.txt", encoding="utf-8") as file:
    token = file.readline().rstrip()


if __name__ == "__main__":
    request_type = input("Type of data to fetch: (forecast(f) | history(h)) ")
    lat = input("Latitude: ")
    lng = input("Longitude: ")
    park_id = input("park_id: ")

    url = "/"
    if request_type == "f" or request_type == "forecast":
        url = "http://localhost:9000/api/transparency-forecast/" \
            f"?lat={lat}&lng={lng}&park_id={park_id}&test=1"
    elif request_type == "h" or request_type == "history":
        url = "http://localhost:9000/api/historical-transparency/" \
            f"?lat={lat}&lng={lng}&park_id={park_id}&test=1"
    else:
        raise ValueError("Invalid type entered.")

    result = subprocess.run(
        ["http", url, f"Username:{USERNAME}", f"Token:{token}"],
        capture_output=True, text=True, check=True
    )

    print(result.stdout)
