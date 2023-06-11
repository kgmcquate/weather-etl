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

from .data_models import DailyWeather

features = [feature.name for feature in dataclasses.fields(DailyWeather) if feature.name not in ("date", "latitude", "longitude", "timezone")]

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
    

def main(spark = SparkSession.builder.getOrCreate()):
    from .database import db_endpoint, db_password, db_username

    jdbc_url = f"jdbc:postgresql://{db_endpoint}"

    print(f"{jdbc_url=}")

    df = (
        spark.read
        .options(
            {"user": db_username, 
             "password": db_password,
             "driver": "org.postgresql.Driver"
             }
        )
        .format("jdbc")
        .load()
    )

    df.show()

    req = WeatherRequest(
        start_date=datetime.date.fromisoformat("2023-01-01"),
        end_date=datetime.date.fromisoformat("2023-01-07"),
        latitude=52.52,
        longitude=13.419998
    )

    pprint.pprint(
        req.get_weather_data()
    )





# url = "https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&daily=
# temperature_2m_max,temperature_2m_min,sunrise,sunset,uv_index_max,uv_index_clear_sky_max,precipitation_sum,rain_sum,showers_sum,snowfall_sum,precipitation_hours,precipitation_probability_max,windspeed_10m_max,windgusts_10m_max,winddirection_10m_dominant,shortwave_radiation_sum,et0_fao_evapotranspiration
# &forecast_days=1&start_date=2023-05-28&end_date=2023-06-03&timezone=America%2FChicago"


