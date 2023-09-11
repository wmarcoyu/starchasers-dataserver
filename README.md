# Starchasers Data Server

>**NOTE: The data server is currently not accepting public requests. Below is a developer-oriented overview of its structure.**

## Introduction

The main function of this data server is downloading weather forecasts data from NOAA four times a day, extracting relevant data (cloud cover, humidity, and aerosol optical depth), and serving them to the app server upon API requests
.

## Endpoints

There are two major endpoints that are directly related to the functionality of Starchasers. Here is a brief overview of their functions. See code for implementation details.

### `starchasers-data.com/api/transparency-forecast/`

Return 72-hour transparency forecasts, computed by the data server, along with the raw cloud, humidity, and aersol optical depth data. Auxiliary information returned includes dark hours, moon activity, Milky Way Activity, and a score for reference. This endpoint serves the *forecast* page of `starchasers.info/details/`.

<a href="docs/forecast_sample_response.json">Sample response</a>

### `starchasers-data.com/api/historical-transparency/`

Return monthly average transparency data pre-computed based on historical data, also accompanied by raw cloud, humidity, and aerosol optical depth data. Auxiliary information includes Bortle class, Milky Way season, new Moon dates, and next meteor shower. This endpoint serves the `trend` page of `starchasers.info/details/`.

<a href="docs/history_sample_response.json">Sample response</a>

## Development

To test and develop for the data server, clone this repository and start a development environment locally. NOAA is open data and does not require sign in to download data. See details here:

GFS: <a href="https://registry.opendata.aws/noaa-gfs-bdp-pds">https://registry.opendata.aws/noaa-gfs-bdp-pds</a>

GEFS: <a href="https://registry.opendata.aws/noaa-gefs">https://registry.opendata.aws/noaa-gefs</a>

## Acknowledgements

Written by <a href="https://github.com/wmarcoyu">Yu Wang</a>, summer 2023.

<a rel="license" href="http://creativecommons.org/licenses/by-nc/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by-nc/4.0/88x31.png" /></a><br />This work is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by-nc/4.0/">Creative Commons Attribution-NonCommercial 4.0 International License</a>.

## Attributions

NOAA Global Forecast System (GFS) was accessed from
<a href="https://registry.opendata.aws/noaa-gfs-bdp-pds">https://registry.opendata.aws/noaa-gfs-bdp-pds</a>

NOAA Global Ensemble Forecast System (GEFS) was accessed from
<a href="https://registry.opendata.aws/noaa-gefs">https://registry.opendata.aws/noaa-gefs</a>
