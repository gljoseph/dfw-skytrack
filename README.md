# dfw-skytrack
This real-time project integrates flight information from the AviationStack API for DFW Airport and weather data from the National Weather Service API, to provide the latest arrival, departure, and forecast details. 


# DFW SkyTrack ✈️

## Overview

This project fetches real-time flight arrival and departure information for Dallas/Fort Worth International Airport (DFW) from the AviationStack API. It also integrates hourly and weekly weather forecasts from the National Weather Service (NWS) API to provide comprehensive aviation and weather insights. The data is updated every minute for flights and every hour for weather. The scope of this project was a high-level look at DFW airport to make sure that it was running smoothly.

## Features

✅ Real-time flight tracking: Fetches up-to-the-minute arrival and departure details for DFW Airport.

✅ Weather forecasting: Pulls hourly and weekly weather forecasts from the NWS API.

✅ Data Cleaning & Processing: Ensures data integrity before storage.

✅ Unit Testing: Validates data accuracy before updating the Gold Table.

✅ Automated Pipeline: Runs continuously to maintain fresh and reliable data.


## Tech Stack

- Python 🐍 (Data fetching, processing, and transformation)

- Apache Spark (PySpark) ⚡ (Data handling and transformations)

- Databricks 📊 (Data processing, storage, orchestration)



## Data Sources

### 1. AviationStack API
- **About:** This API offers a simple, free way of accessing global flight tracking data in real-time. It provides an extensive set of aviation data, including real-time flight status, flight schedules, airline routes, airports, and aircrafts. It updates data every 30-60 seconds. This included information from 250+ countries and 13000+ airlines.
- **Need:** This API provided the base for the project and the rest of the project is built on top of this.
- **Data includes:** Airline IATA, Airline name, Flight number, Depart/Arrival location, Flight status, Flight delay amount, Scheduled time, Estimated time. AviationStack data divides into Departure and Arrival flight information for DFW airport.
- **Data problems:** There were limits to the free version of their API of 100 requests each month. The free version of the API would only return 100 rows of data at a time.

### 2. National Weather Service API
- **About:** The National Weather Service is a government entity that collects weather data. It provides critical forecast, alerts, observations. This API offers public access to a wide range of essential weather data. NWS refreshes every hour.
- **Need:** NWS provides hourly and weekly data for DFW airport managers to gauge the likeliness of airport delays due to rain, snow, wind, or other extreme weather.
- **Data includes:** temperature, wind speed, wind direction, short forecast, detailed forecast
- **Data problems:** The API documentation did not clearly define the rate limits.

### 3. Global Airport Database
- **About:** This Global Airport Database provided location information on 9300 large and small airports all around the world.
- **Need:** AviationStack API data did not include location information for flights. Another data source was needed to get detailed airport information.
- **Data includes:** ICAO code, IATA code, airport name, country, city, latitude, longitude, altitude
- **Data problems:** The website wasn’t clear on how frequently this data would be updated or how the data is maintained. It worked for this project, but long-term use would be affected when new airports are built.

## Fetch Data
- Calls AviationStack API for DFW flight arrivals & departures.
- Calls NWS API for hourly and weekly weather updates.
- Process & Clean Data
- Removes duplicates and fills missing values.
- Normalizes and structures data.
- Unit Testing
- Ensures key fields are non-null (e.g., flight_iata, arrival_time).
- Verifies data consistency before proceeding.
- Store & Update
- Overwrites clean data to Gold tables.
- Updates every minute for flights and every hour for weather.


## Installation & Setup

### Clone the repository:
git clone https://github.com/your-username/dfw-flight-weather-tracker.git
cd dfw-flight-weather-tracker

### Install dependencies:
pip install -r requirements.txt

### Set up your API keys (AviationStack & NWS).

### Update folder
Run the pipeline:
python main.py

## Visuals


## Contributing

We welcome contributions! Feel free to submit issues or pull requests.
