# Overview

This is a project that uses Apache Airflow to automate the process of collecting data and sending it to data analysts

## Data Source

Data is taken from OpenweatherMap's API

link: https://openweathermap.org/api

**Preparation**: Airflow, Posgres Email STMP

**Step 1** : Collect the data from OpenWeatherMap with API key.

**Step 2** : Connect Airflow with Postgres, transform the data and save it to Postgres.

**Step 3** : Extract the necessary data from Postgres and save it to a file then send it via Email.
