# DFW SkyTrack ✈️

## Overview

This project fetches real-time flight arrival and departure information for Dallas/Fort Worth International Airport (DFW) from the AviationStack API. It also integrates hourly and weekly weather forecasts from the National Weather Service (NWS) API to provide comprehensive aviation and weather insights. The data is updated every minute for flights and every hour for weather. The scope of this project was a high-level look at DFW airport to make sure that it runs smoothly. By leveraging flight and weather data, the project lets DFW airport operators know the flight, baggage, gate, and weather information that will affect passengers traveling through the airport.

The insights are presented through an interactive Databricks dashboard to show real-time departure and arrival flight updates and how weather will affect those flights.

## Problem Statement
- The airport requires an interactive dashboard that provides real-time insights into flight performance and the impact of weather on current and future operations.
- An automated data pipeline is essential to ensure continuous updates on flight schedules and weather conditions.

## Objectives
- Gather and analyze daily airport operations to assess overall performance and identify any emerging issues.
- Monitor upcoming weather conditions to anticipate potential disruptions to airport operations.
- Develop a user-friendly dashboard that provides quick and easy access to essential flight and weather information.

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

### 1. AviationStack API 🛬
- **About:** This API offers a simple, free way of accessing global flight tracking data in real-time. It provides an extensive set of aviation data, including real-time flight status, flight schedules, airline routes, airports, and aircrafts. It updates data every 30-60 seconds. This included information from 250+ countries and 13000+ airlines.
- **Need:** This API provided the base for the project and the rest of the project is built on top of this.
- **Data includes:** Airline IATA, Airline name, Flight number, Depart/Arrival location, Flight status, Flight delay amount, Scheduled time, Estimated time. AviationStack data divides into Departure and Arrival flight information for DFW airport.
- **Data problems:** There were limits to the free version of their API of 100 requests each month. The free version of the API would only return 100 rows of data at a time.

### 2. National Weather Service API ☀️
- **About:** The National Weather Service is a government entity that collects weather data. It provides critical forecast, alerts, observations. This API offers public access to a wide range of essential weather data. NWS refreshes every hour.
- **Need:** NWS provides hourly and weekly data for DFW airport managers to gauge the likeliness of airport delays due to rain, snow, wind, or other extreme weather.
- **Data includes:** temperature, wind speed, wind direction, short forecast, detailed forecast
- **Data problems:** The API documentation did not clearly define the rate limits.

### 3. Global Airport Database 📍
- **About:** This Global Airport Database provided location information on 9300 large and small airports all around the world.
- **Need:** AviationStack API data did not include location information for flights. Another data source was needed to get detailed airport information.
- **Data includes:** ICAO code, IATA code, airport name, country, city, latitude, longitude, altitude
- **Data problems:** The website wasn’t clear on how frequently this data would be updated or how the data is maintained. It worked for this project, but long-term use would be affected when new airports are built.

## Architecture and Methodology 📝
Medallion Architecture was implemented to enhance data quality, organization, and reliability throughout the data pipeline. 

In the bronze layer, raw data was ingested in its original form, serving as the foundational data source. Before transitioning to the silver layer, rigorous data processing techniques were applied, including data cleansing, transformation, deduplication, and filtering, to ensure consistency and accuracy. Finally, before promoting data to the gold layer, comprehensive unit tests were conducted to validate data integrity, preventing bad or incomplete data from reaching production-level tables. 

This structured approach ensures that only high-quality, reliable data is used for analysis and decision-making.

### Data Pipeline Architecture

### Data Model Design ⚙️

<img width="825" alt="Screenshot 2025-03-08 at 5 13 58 PM" src="https://github.com/user-attachments/assets/9e91e70d-72c5-4e6c-bc41-f96390a214fd" />

### Streaming Processing, Ingestion, & Storage 💾 
- Implemented Databricks Delta Live Tables to stream real-time departure and arrival data from the AviationStack API.
- Utilized Databricks Volumes for efficient storage of streaming data and processing with PySpark.
- Configured a Databricks workflow to run continuously, integrating a Delta Live pipeline for seamless data ingestion and processing.

### Batch Processing
- Configured a Databricks workflow to schedule hourly updates, ensuring the dashboard uses the latest National Weather Service data.
- The Global Airport Database lacked documentation on data refresh intervals, so no automated workflow was implemented for its updates.

### Data Quality 🔢
To maintain the integrity and reliability of the gold-level tables, unit tests were implemented to validate the data. These tests ensured:
- No null values were present in critical fields.
- All expected columns existed in the dataset.
- Duplicate records were identified and removed.

### Orchestration
- Used Databricks workflow to schedule continuous and hourly updates
- National Weather Service workflow was scheduled for 5 minutes after the hour

## Key Metrics & Business Value 📈
- **Flight Count:** This metric shows the amount of flights shown by departure and arrival status
- **Delayed:** This metric shows the amount of departure and arrival flights that are delayed.
- **Runway time:** This metrics shows the spread of flight time from when the airplane pushes back from the gate to when the flight takes off.
- **Arrival gate assignments:** this visual shows the arrival gates to show airport operators which gate may be too busy
- **Baggage claim assignments:** this visual shows the baggage claim assignments to show airport operators which gate may be too busy
- **Wind forecast:** this visual shows the wind forecast for the next week

## Visualizations 📊
Find the dashboard at this link: https://dbc-7b106152-caf3.cloud.databricks.com/dashboardsv3/01effc2f5f5b1e22858830eeb7df3abf/published?o=1352785079224954

Here are some of the visuals:

#### Departures page:
<img width="1310" alt="departures" src="https://github.com/user-attachments/assets/6c39ce54-705b-449c-a666-57f29a880941" />

#### Arrivals page:
<img width="1308" alt="arrivals" src="https://github.com/user-attachments/assets/b6ef6c5a-d539-4f4f-89c1-e2b630e552b0" />

#### Arrival gate:
<img width="1312" alt="arrival_gate" src="https://github.com/user-attachments/assets/5c24d100-493c-4297-885c-07d35beb6f21" />

#### Baggage Claim:
<img width="1318" alt="baggage_claim" src="https://github.com/user-attachments/assets/70debcd5-c05c-424e-8198-a262ce20c18e" />

#### Delayed flights page:
<img width="1315" alt="delayed_flights" src="https://github.com/user-attachments/assets/b0d56fbe-f593-4e26-809e-b7a7443837e8" />

## Future Enhancements
- Extend data coverage to include all airports for a more comprehensive analysis.  
- Enrich the dataset with additional insights relevant to airport operators, such as flight safety information and historical flight data.  
- Incorporate UV weather data to enhance weather-related decision-making.

## Installation & Setup ⚙️

1. Clone the repository: https://github.com/gljoseph/dfw-skytrack.git

2. Set up the repository on Databricks

3. Set up your API key from AviationStack

4. Install Python libraries: requests, time, pyspark, json

5. Update Volumes table locations

## Contributing

Contributions are always welcome! Feel free to submit issues or pull requests.
