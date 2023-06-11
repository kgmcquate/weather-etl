import os, json, boto3
from pyspark.sql import SparkSession
import dataclasses
import datetime
from dataclasses import dataclass
from urllib.parse import urlencode
import requests
import pprint
# import database

api_url = "api.open-meteo.com/v1/"
DEFAULT_LOOKBACK_DAYS = 365
lakes_table_name = "lakes"
weather_by_day_table_name = "weather_by_day"

from .data_models import DailyWeather

features = [feature.name for feature in dataclasses.fields(DailyWeather) if feature.name not in ("date", "latitude", "longitude", "timezone")]

import logging

@dataclass
class WeatherRequest:
    start_date: datetime.date
    end_date:  datetime.date
    latitude: float
    longitude: float

    features = features
    api_url = api_url

    def __post_init__(self):

        pass

    def _get_request_params(self): # -> dict[str, str]
        return {
            "daily": ','.join(self.features),
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat(),
            "latitude": self.latitude,
            "longitude": self.longitude,
            "timezone": "auto" # The local timezone
        }

    def _get_request_url(self): #  -> str
        return f"https://{self.api_url}forecast?{urlencode(self._get_request_params())}"
    
    def _send_request(self):
        return requests.get(url=self._get_request_url())

    def get_weather_data(self): # -> list[DailyWeather]
        response = self._send_request().json()

        daily_data = response["daily"] # : dict[str, list]
        local_tz = response["timezone"] # : str 

        daily_weathers = [] # : list[DailyWeather] 
        for i, date in enumerate(daily_data["time"]):
            features_dict = {}
            for feature in features:
                features_dict[feature] = daily_data[feature][i]

            daily_weathers.append(
                DailyWeather(
                    date=date,
                    latitude=self.latitude,
                    longitude=self.longitude,
                    timezone=local_tz,
                    **features_dict
                )
            )

        return daily_weathers

    

def get_jdbc_options():
    from .database import db_endpoint, db_password, db_username
    jdbc_url = f"jdbc:postgresql://{db_endpoint}:5432/"

    # logger.debug(jdbc_url)

    return {
        "url": jdbc_url,
        "user": db_username,
        "password": db_password,
        "driver": "org.postgresql.Driver"
    }


def main(
        spark = SparkSession.builder.getOrCreate(),
        current_date: datetime.date = datetime.date.today(),
        lookback_days: int = DEFAULT_LOOKBACK_DAYS,

    ):
    from pyspark.sql.functions import col, lit, max, min

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    
    start_date = current_date - datetime.timedelta(days=lookback_days)
    weather_dates = [current_date - datetime.timedelta(days=x) for x in range(lookback_days) ]

    latlngs_df = (
        spark.read
        .option("dbtable", lakes_table_name)
        .options(**get_jdbc_options())
        .format("jdbc")
        .load()
        .select("latitude", "longitude")
        .distinct() # Some locations have the same lat/long
    )

    weather_days_df = (
        spark.read
        .option("dbtable", weather_by_day_table_name)
        .options(**get_jdbc_options())
        .format("jdbc")
        .load()
        .select("date", "latitude", "longitude")
        .filter(col("date") >= lit(start_date.isoformat()) )
    )

    if logger.isEnabledFor(logging.DEBUG):
        latlngs_df.cache().show()

    from pyspark.sql.types import StructType, StructField, DateType, StringType, FloatType

    needed_dates_df = (
        spark.createDataFrame(
            [[d] for d in weather_dates],
            schema=StructType().add(field="date", data_type=DateType())
        )
    )

    if logger.isEnabledFor(logging.DEBUG):
        needed_dates_df.cache().show()


    weathers_needed = latlngs_df.crossJoin(needed_dates_df)

    # Weathers that aren't already in the DB
    weathers_to_get = (
        weathers_needed
        .join(weather_days_df, ["latitude", "longitude", "date"], "leftanti")
    )

    
    weathers_rdd = (
        # Get the start and end times for each location, since the API takes a lat/lng and date range
        weathers_to_get.groupBy("latitude", "longitude").agg(
            min(col("date")).alias("start_date"),
            max(col("date")).alias("end_date")
        )
        # TODO remove limit for prod
        .limit(10)
        # Turn into API requests
        .rdd
        .map(lambda row: WeatherRequest(**row.asDict()))
        # Only run 5 requests concurrently
        .coalesce(5)
        # Send request to API
        # Each request turns a list of weathers, so flatMap
        .flatMap(lambda req: req.get_weather_data())
        .map(lambda x: x.__dict__)
    )

    if logger.isEnabledFor(logging.DEBUG):
        weathers_rdd.cache()
        print(weathers_rdd.take(10))

    # def get_pyspark_type(the_type):
    #     if the_type == float:
    #         return FloatType()
    #     elif the_type == str:
    #         return StringType()

    # daily_weather_schema = [StructField(name=name, dataType=) for name, python_type in DailyWeather.__annotations__.items()]

    new_weathers = spark.createDataFrame(weathers_rdd)

    new_weathers.show()


    # req = WeatherRequest(
    #     start_date=datetime.date.fromisoformat("2023-01-01"),
    #     end_date=datetime.date.fromisoformat("2023-01-07"),
    #     latitude=52.52,
    #     longitude=13.419998
    # )

    # pprint.pprint(
    #     req.get_weather_data()
    # )





# url = "https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&daily=
# temperature_2m_max,temperature_2m_min,sunrise,sunset,uv_index_max,uv_index_clear_sky_max,precipitation_sum,rain_sum,showers_sum,snowfall_sum,precipitation_hours,precipitation_probability_max,windspeed_10m_max,windgusts_10m_max,winddirection_10m_dominant,shortwave_radiation_sum,et0_fao_evapotranspiration
# &forecast_days=1&start_date=2023-05-28&end_date=2023-06-03&timezone=America%2FChicago"


