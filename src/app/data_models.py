import datetime

from dataclasses import dataclass
import dataclasses
from urllib.parse import urlencode
import requests
import pprint
from urllib3.util import Retry
import time

from .config import api_url

@dataclass
class DailyWeather:
    date: str
    latitude: str
    longitude: str
    timezone: str
    temperature_2m_max: float
    temperature_2m_min: float
    sunrise: str
    sunset: str
    uv_index_max: float
    uv_index_clear_sky_max: float
    precipitation_sum: float
    rain_sum: float
    showers_sum: float
    snowfall_sum: float
    precipitation_hours: float
    precipitation_probability_max: int
    windspeed_10m_max: float
    windgusts_10m_max: float
    winddirection_10m_dominant: int
    shortwave_radiation_sum: float
    et0_fao_evapotranspiration: float

    def __post_init__(self):
        pass


@dataclass
class WeatherRequest:
    start_date: datetime.date
    end_date:  datetime.date
    latitude: str
    longitude: str

    features = [feature.name for feature in dataclasses.fields(DailyWeather) if feature.name not in ("date", "latitude", "longitude", "timezone")]
    api_url = api_url

    def _get_request_params(self): # -> dict[str, str]
        return {
            "daily": ','.join(self.features),
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat(),
            "latitude": float(self.latitude),
            "longitude": float(self.longitude),
            "timezone": "auto" # The local timezone
        }

    def _get_request_url(self): #  -> str
        return f"https://{self.api_url}forecast?{urlencode(self._get_request_params())}"
    
    def _send_request(self):
        # retries = Retry(10)
        # return http.request(
        #     "GET", "https://example.com/", retries=Retry(10))

        time.sleep(0.1)
        
        return requests.get(url=self._get_request_url())

    def get_weather_data(self): # -> list[DailyWeather]
        try:
            response = self._send_request().json()

            daily_data = response["daily"] # : dict[str, list]
            local_tz = response["timezone"] # : str 

            daily_weathers = [] # : list[DailyWeather] 
            for i, date in enumerate(daily_data["time"]):
                features_dict = {}
                for feature in self.features:
                    features_dict[feature] = daily_data[feature][i]

                # "daily":{"time":["2023-05-28",
                assert self.start_date <= datetime.date.fromisoformat(date) <= self.end_date

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
        except Exception as e:
            return Exception(f"Error getting weather data for {self}", e)