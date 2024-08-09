from data_load_repository import BigQueryDataLoadingRepository
from data_load_repository import DataLoadingRepository
from google.cloud import bigquery
from typing import Callable
from dotenv import load_dotenv
import pandas as pd
import requests
import json
import os


def extract(api_key: str,
            base_url: str,
            make_get_request: Callable[[str], requests.Response] = requests.get
            ) -> dict:
    url = f"{base_url}flights?api_key={api_key}"
    response = make_get_request(url)
    if response.status_code == 200:
        return json.loads(response.text)["response"]
    return {"message": "Invalid response received from airlabs API"}


def transform(data: dict,
              PROJECT_ID: str,
              DATASET_ID: str,
              NEW_TABLE_ID: str,
              repository: DataLoadingRepository
              ) -> (pd.DataFrame, pd.DataFrame):
    # loads lookup tables into dataframes.
    # Airport_locations - Maps icao to lat/long co-ordinates.
    # Countries - Maps country codes to country names.
    # Pax - Maps aircraft model to passenger capacity.
    locations = pd.read_csv(os.path.join(os.getcwd(), "airport_locations.csv"))
    pax = pd.read_csv(os.path.join(os.getcwd(), "pax.csv"))
    countries = pd.read_csv(os.path.join(os.getcwd(), "countries.csv"))

    # Takes the Air Traffic data from the API, enriches it with data from the
    # above lookup tables, and processes it to match the BigQuery schema.
    df = pd.DataFrame(data) \
           .dropna(subset=['dep_icao', 'arr_icao']) \
           .merge(locations[["icao", "airport", "latitude", "longitude"]],
                  left_on='dep_icao', right_on='icao', how="left") \
           .rename(columns={'airport': 'dep_airport',
                   'latitude': 'dep_lat', 'longitude': 'dep_long'}) \
           .drop(columns=['icao']) \
           .merge(locations[["icao", "airport", "latitude", "longitude"]],
                  left_on='arr_icao', right_on='icao', how="left") \
           .rename(columns={'airport': 'arr_airport',
                   'latitude': 'arr_lat', 'longitude': 'arr_long'}) \
           .drop(columns=['icao']) \
           .dropna(subset=['dep_lat', 'dep_long', 'arr_lat', 'arr_long']) \
           .astype({'dep_lat': str, 'dep_long': str, 'arr_lat': str, 'arr_long': str}) \
           .merge(pax, on="aircraft_icao", how="inner")
    df = df.assign(dep_code=df["dep_icao"].str[:2]) \
           .assign(arr_code=df["arr_icao"].str[:2]) \
           .merge(countries, left_on='dep_code',
                  right_on='country_code', how='left') \
           .rename(columns={'country': 'dep_country'}) \
           .drop(columns=['country_code']) \
           .merge(countries, left_on='arr_code',
                  right_on='country_code', how='left') \
           .rename(columns={'country': 'arr_country'}) \
           .drop(columns=['country_code']) \
           .dropna(subset=['dep_country', 'arr_country']) \
           .rename(columns={'updated': 'timestamp'}) \
           [["hex", "dep_country", "dep_airport", "dep_lat", "dep_long",
             "arr_country", "arr_airport", "arr_lat", "arr_long", "Pax", "updated"]]

    # The data has no feasible primary key, so data consistency is achieved by
    # caching the previous insertion in a separate table. Here we are pulling
    # that cache to compare it to the data being loaded.
    try:
        # If the table exists, load it into a Pandas DataFrame
        query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{NEW_TABLE_ID}`"
        cached_data = repository.extract_data(query).to_dataframe()
        new_data = cached_data.merge(df[~df["hex"].isin(cached_data["hex"])],
                                     on="hex", how="left", indicator=True) \
                              .query('_merge == "left_only"') \
                              .drop(columns='_merge')
        return df, new_data
    except Exception as e:
        # If there is no cache, the data to be loaded into both tables is the same.
        if "Not found: Table" in str(e):
            return df, df
        else:
            return {"message": "An unexpected error occurred"}


def load(df: pd.DataFrame,
         new_data: pd.DataFrame,
         DATASET_ID: str,
         OLD_TABLE_ID: str,
         NEW_TABLE_ID: str,
         repository: DataLoadingRepository
         ) -> dict:

    # Overwrite the new-flight-data table with all the data from the API response
    repository.load_data(df, NEW_TABLE_ID, "WRITE_TRUNCATE")

    try:
        # Append the new data to the main table
        repository.load_data(new_data, OLD_TABLE_ID, "WRITE_APPEND")
    except Exception as e:
        if "Not found: Table" in str(e):
            # If the table doesn't exist, create it and insert the data
            repository.load_data(new_data, OLD_TABLE_ID, "WRITE_EMPTY")
        else:
            return {"message": "An unexpected error occurred"}
    return {"message": "Database updated successfully"}


def handle_request():

    load_dotenv()
    API_KEY = os.getenv("API_KEY")
    PROJECT_ID = os.getenv("PROJECT_ID")
    DATASET_ID = os.getenv("DATASET_ID")
    OLD_TABLE_ID = os.getenv("OLD_TABLE_ID")
    NEW_TABLE_ID = os.getenv("NEW_TABLE_ID")
    BASE_URL = "https://airlabs.co/api/v9/"
    client = bigquery.Client(project=PROJECT_ID)
    bigquery_repository = BigQueryDataLoadingRepository(client, DATASET_ID)

    data = extract(API_KEY, BASE_URL)
    df, new_data = transform(data, PROJECT_ID, DATASET_ID,
                             NEW_TABLE_ID, bigquery_repository)
    response = load(df, new_data, DATASET_ID, OLD_TABLE_ID,
                    NEW_TABLE_ID, bigquery_repository)

    # Return the result as a JSON response
    return response
