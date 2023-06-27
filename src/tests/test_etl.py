import datetime
import os
import unittest
from unittest.mock import MagicMock, patch
from chispa.dataframe_comparer import assert_df_equality

from pyspark.sql import SparkSession
# from your_module import main
from .app.main import transform


# def get_test_jdbc_options():
#     sqlite_path = "tests/test.db"
#     jdbc_url = f"jdbc:sqlite://{sqlite_path}"

#     logger.debug(jdbc_url)

#     return {
#         "dbtable": lakes_table_name,
#         "url": jdbc_url,
#         "driver": "org.sqlite.JDBC"
#     }

mock_response = {"latitude":52.52,"longitude":13.419998,"generationtime_ms":0.6489753723144531,"utc_offset_seconds":-18000,"timezone":"America/Chicago","timezone_abbreviation":"CDT","elevation":38.0,"daily_units":{"time":"iso8601","temperature_2m_max":"°C","temperature_2m_min":"°C","sunrise":"iso8601","sunset":"iso8601","uv_index_max":"","uv_index_clear_sky_max":"","precipitation_sum":"mm","rain_sum":"mm","showers_sum":"mm","snowfall_sum":"cm","precipitation_hours":"h","precipitation_probability_max":"%","windspeed_10m_max":"km/h","windgusts_10m_max":"km/h","winddirection_10m_dominant":"°","shortwave_radiation_sum":"MJ/m²","et0_fao_evapotranspiration":"mm"},"daily":{"time":["2023-05-28","2023-05-29","2023-05-30","2023-05-31","2023-06-01","2023-06-02","2023-06-03"],"temperature_2m_max":[23.8,21.5,22.3,25.1,21.9,19.1,20.0],"temperature_2m_min":[10.4,8.5,9.3,10.6,9.6,6.8,7.8],"sunrise":["2023-05-27T21:50","2023-05-28T21:49","2023-05-29T21:48","2023-05-30T21:48","2023-05-31T21:47","2023-06-01T21:46","2023-06-02T21:45"],"sunset":["2023-05-28T14:16","2023-05-29T14:17","2023-05-30T14:18","2023-05-31T14:20","2023-06-01T14:21","2023-06-02T14:22","2023-06-03T14:23"],"uv_index_max":[6.60,5.85,6.60,6.20,6.70,6.65,6.60],"uv_index_clear_sky_max":[6.60,6.60,6.60,6.70,6.70,6.65,6.65],"precipitation_sum":[0.00,0.00,0.00,0.00,0.00,0.00,0.00],"rain_sum":[0.00,0.00,0.00,0.00,0.00,0.00,0.00],"showers_sum":[0.00,0.00,0.00,0.00,0.00,0.00,0.00],"snowfall_sum":[0.00,0.00,0.00,0.00,0.00,0.00,0.00],"precipitation_hours":[0.0,0.0,0.0,0.0,0.0,0.0,0.0],"precipitation_probability_max":[0,3,0,0,0,0,0],"windspeed_10m_max":[14.5,13.0,11.7,10.5,14.1,16.7,12.6],"windgusts_10m_max":[31.7,32.0,33.1,25.2,34.2,39.2,34.2],"winddirection_10m_dominant":[55,343,22,308,303,37,73],"shortwave_radiation_sum":[28.32,24.56,27.81,26.50,26.36,23.46,28.04],"et0_fao_evapotranspiration":[5.23,4.76,5.20,5.16,4.67,4.60,4.99]}}


lakes_schema = """
    id INT,
    lake_name STRING,
    latitude STRING,
    longitude STRING,
    nearby_city_name STRING,
    state_or_province STRING,
    country STRING,
    nearby_city_latitude STRING,
    nearby_city_longitude STRING,
    max_depth_m DOUBLE,
    surface_area_m2 DOUBLE
"""

daily_weather_schema = """
    date date NOT NULL,
    latitude STRING NOT NULL,
    longitude STRING NOT NULL,
    timezone STRING NOT NULL,
    temperature_2m_max DOUBLE ,
    temperature_2m_min DOUBLE,
    sunrise DATE,
    sunset DATE,
    uv_index_max DOUBLE,
    uv_index_clear_sky_max DOUBLE,
    precipitation_sum DOUBLE,
    rain_sum DOUBLE,
    showers_sum DOUBLE,
    snowfall_sum DOUBLE,
    precipitation_hours DOUBLE,
    precipitation_probability_max INT,
    windspeed_10m_max DOUBLE,
    windgusts_10m_max DOUBLE,
    winddirection_10m_dominant INT ,
    shortwave_radiation_sum DOUBLE,
    et0_fao_evapotranspiration DOUBLE
"""


class TestMainFunction(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Create a SparkSession for testing
        cls.spark: SparkSession = SparkSession.builder \
            .appName("TestApp") \
            .master("local[1]") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        # Stop the SparkSession
        cls.spark.stop()

    @patch('app.data_models.requests.get')
    def test_transform_function(
        self,
        mock_get: MagicMock
    ) -> None:
        # Define test input parameters
        current_date: datetime.date = datetime.date(2023, 6, 25)
        lookback_days: int = 2

        # Mock API response
        mock_weather_api_response: MagicMock = MagicMock()
        mock_weather_api_response.json.return_value = {
            "daily": {
                "time": ["2023-06-23", "2023-06-24"],
                "temperature_2m_max": [25.0, 28.0],
                "temperature_2m_min": [18.0, 20.0],
                "sunrise": ["06:00:00", "05:59:00"],
                "sunset": ["20:00:00", "20:01:00"],
                "uv_index_max": [7.0, 8.0],
                "uv_index_clear_sky_max": [9.0, 10.0],
                "precipitation_sum": [0.5, 1.2],
                "rain_sum": [0.3, 0.8],
                "showers_sum": [0.1, 0.4],
                "snowfall_sum": [0.0, 0.0],
                "precipitation_hours": [5.0, 7.0],
                "precipitation_probability_max": [50, 60],
                "windspeed_10m_max": [15.0, 18.0],
                "windgusts_10m_max": [20.0, 22.0],
                "winddirection_10m_dominant": [180, 200],
                "shortwave_radiation_sum": [1000.0, 1200.0],
                "et0_fao_evapotranspiration": [4.0, 5.0]
            },
            "timezone": "America/New_York"
        }

        mock_get.return_value = mock_weather_api_response

        # mock_new_weathers_df = self.spark.createDataFrame()

        pyfile_path = os.path.dirname(os.path.realpath(__file__))

        mock_lakes_table_df = (
            self.spark.read
            .option("delimiter", "\t")
            .option("header", "true")
            .schema(lakes_schema)
            .csv(os.path.join(pyfile_path, "data_files/test_lakes.tsv"))
        )

        mock_daily_weather_table_df = (
            self.spark.read
            .option("delimiter", "\t")
            .option("header", "true")
            .schema(daily_weather_schema)
            .csv(os.path.join(pyfile_path,"data_files/test_daily_weather.tsv"))
        )

        # Call the main function
        new_weathers = transform(
            lakes_table_df=mock_lakes_table_df,
            daily_weather_table_df=mock_daily_weather_table_df,
            current_date=current_date,
            lookback_days=lookback_days
        )

        new_weathers.show()

        # assert_df_equality(new_weathers, mock_new_weathers_df, ignore_row_order=True)

        # # Assertions
        # # Verify that the API request was made with the expected URL
        # expected_url: str = "https://api.open-meteo.com/v1/forecast?daily=temperature_2m_max,temperature_2m_min&start_date=2023-06-23&end_date=2023-06-24&latitude=mock_lat&longitude=mock_lng&timezone=auto"
        # mock_get.assert_called_once_with(url=expected_url)

        # # Reset mock objects
        # mock_get.reset_mock()
        # self.spark.write.reset_mock()