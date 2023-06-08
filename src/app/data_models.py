import datetime

from dataclasses import dataclass
    
@dataclass(slots=True)
class DailyWeather:
    date: datetime.date
    latitude: float
    longitude: float
    timezone: str
    temperature_2m_max: float
    temperature_2m_min: float
    sunrise: float
    sunset: float
    uv_index_max: float
    uv_index_clear_sky_max: float
    precipitation_sum: float
    rain_sum: float
    showers_sum: float
    snowfall_sum: float
    precipitation_hours: float
    precipitation_probability_max: float
    windspeed_10m_max: float
    windgusts_10m_max: float
    winddirection_10m_dominant: float
    shortwave_radiation_sum: float
    et0_fao_evapotranspiration: float