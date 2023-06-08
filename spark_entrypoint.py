
from pyspark.sql import SparkSession

from app.main import main


main()

# url = "https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&daily=
# temperature_2m_max,temperature_2m_min,sunrise,sunset,uv_index_max,uv_index_clear_sky_max,precipitation_sum,rain_sum,showers_sum,snowfall_sum,precipitation_hours,precipitation_probability_max,windspeed_10m_max,windgusts_10m_max,winddirection_10m_dominant,shortwave_radiation_sum,et0_fao_evapotranspiration
# &forecast_days=1&start_date=2023-05-28&end_date=2023-06-03&timezone=America%2FChicago"


