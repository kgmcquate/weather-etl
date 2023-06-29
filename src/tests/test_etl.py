import datetime
from dataclasses import dataclass
import os
import unittest
from unittest.mock import MagicMock, patch
from chispa.dataframe_comparer import assert_df_equality
import json
from pyspark.sql import SparkSession, DataFrame


# def get_test_jdbc_options():
#     sqlite_path = "tests/test.db"
#     jdbc_url = f"jdbc:sqlite://{sqlite_path}"

#     logger.debug(jdbc_url)

#     return {
#         "dbtable": lakes_table_name,
#         "url": jdbc_url,
#         "driver": "org.sqlite.JDBC"
#     }

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
    sunrise TIMESTAMP,
    sunset TIMESTAMP,
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


class TestAPIRequest(unittest.TestCase):
    @patch("app.data_models.requests.get")
    def test_api_call_mock(self, mock_get: MagicMock) -> None:
        from app.data_models import WeatherRequest

        test_case_id = "2"
        test_data_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), f"data_files/case_{test_case_id}/")

        # Mock API response
        resp = json.loads(open(f"{test_data_path}/api_response.json").read())
        mock_weather_api_response: MagicMock = MagicMock()
        mock_weather_api_response.json.return_value = resp

        mock_get.return_value = mock_weather_api_response

        # "daily":{"time":["2023-05-28",

        w_req = WeatherRequest(
            start_date=datetime.date.fromisoformat("2023-05-28"),
            end_date=datetime.date.fromisoformat("2023-06-03"),
            latitude=52.52,
            longitude=13.419998
        )

        self.assertDictEqual(w_req._send_request().json(), resp)

        daily_weathers = w_req.get_weather_data()

        import pprint
        
        from app.data_models import DailyWeather

        expected_weathers = eval(open(f"{test_data_path}/daily_weathers.py").read())

        self.assertListEqual(
            daily_weathers,
            expected_weathers
        )



class TestSparkJob(unittest.TestCase):
    """Testing the spark job locally"""
    @classmethod
    def setUpClass(cls):
        # Create a SparkSession for testing
        cls.spark: SparkSession = (
            SparkSession.builder.master("local[1]").getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        # Stop the SparkSession
        cls.spark.stop()


    def test_transform_function(self) -> None:
        from app.main import transform

        test_case_id = "1"
        test_data_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), f"data_files/case_{test_case_id}/")

        # Define test input parameters
        current_date: datetime.date = datetime.date(2023, 6, 25)
        lookback_days: int = 2


        mock_lakes_table_df = (
            self.spark.read.option("delimiter", "\t")
            .option("header", "true")
            .schema(lakes_schema)
            .csv(f"{test_data_path}lakes.tsv")
        )

        mock_daily_weather_table_df = (
            self.spark.read.option("delimiter", "\t")
            .option("header", "true")
            .schema(daily_weather_schema)
            .csv(f"{test_data_path}daily_weather.tsv")
        )

        # Call the main function
        new_weathers = transform(
            lakes_table_df=mock_lakes_table_df,
            daily_weather_table_df=mock_daily_weather_table_df,
            current_date=current_date,
            lookback_days=lookback_days,
        )

        new_weathers.show()

        expected_output_df = self.spark.read.schema(daily_weather_schema).csv(f"{test_data_path}output_df.csv")


        assert_df_equality(
            new_weathers.orderBy("date", "latitude", "longitude").select("date", "latitude", "longitude"),
            expected_output_df.orderBy("date", "latitude", "longitude").select("date", "latitude", "longitude"),
            # ignore_row_order=True,
            # ignore_column_order=True
        )

        # # Assertions
        # # Verify that the API request was made with the expected URL
        # expected_url: str = "https://api.open-meteo.com/v1/forecast?daily=temperature_2m_max,temperature_2m_min&start_date=2023-06-23&end_date=2023-06-24&latitude=mock_lat&longitude=mock_lng&timezone=auto"
        # mock_get.assert_called_once_with(url=expected_url)

        # # Reset mock objects
        # mock_get.reset_mock()
        # self.spark.write.reset_mock()
