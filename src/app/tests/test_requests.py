


mock_response = {"latitude":52.52,"longitude":13.419998,"generationtime_ms":0.6489753723144531,"utc_offset_seconds":-18000,"timezone":"America/Chicago","timezone_abbreviation":"CDT","elevation":38.0,"daily_units":{"time":"iso8601","temperature_2m_max":"°C","temperature_2m_min":"°C","sunrise":"iso8601","sunset":"iso8601","uv_index_max":"","uv_index_clear_sky_max":"","precipitation_sum":"mm","rain_sum":"mm","showers_sum":"mm","snowfall_sum":"cm","precipitation_hours":"h","precipitation_probability_max":"%","windspeed_10m_max":"km/h","windgusts_10m_max":"km/h","winddirection_10m_dominant":"°","shortwave_radiation_sum":"MJ/m²","et0_fao_evapotranspiration":"mm"},"daily":{"time":["2023-05-28","2023-05-29","2023-05-30","2023-05-31","2023-06-01","2023-06-02","2023-06-03"],"temperature_2m_max":[23.8,21.5,22.3,25.1,21.9,19.1,20.0],"temperature_2m_min":[10.4,8.5,9.3,10.6,9.6,6.8,7.8],"sunrise":["2023-05-27T21:50","2023-05-28T21:49","2023-05-29T21:48","2023-05-30T21:48","2023-05-31T21:47","2023-06-01T21:46","2023-06-02T21:45"],"sunset":["2023-05-28T14:16","2023-05-29T14:17","2023-05-30T14:18","2023-05-31T14:20","2023-06-01T14:21","2023-06-02T14:22","2023-06-03T14:23"],"uv_index_max":[6.60,5.85,6.60,6.20,6.70,6.65,6.60],"uv_index_clear_sky_max":[6.60,6.60,6.60,6.70,6.70,6.65,6.65],"precipitation_sum":[0.00,0.00,0.00,0.00,0.00,0.00,0.00],"rain_sum":[0.00,0.00,0.00,0.00,0.00,0.00,0.00],"showers_sum":[0.00,0.00,0.00,0.00,0.00,0.00,0.00],"snowfall_sum":[0.00,0.00,0.00,0.00,0.00,0.00,0.00],"precipitation_hours":[0.0,0.0,0.0,0.0,0.0,0.0,0.0],"precipitation_probability_max":[0,3,0,0,0,0,0],"windspeed_10m_max":[14.5,13.0,11.7,10.5,14.1,16.7,12.6],"windgusts_10m_max":[31.7,32.0,33.1,25.2,34.2,39.2,34.2],"winddirection_10m_dominant":[55,343,22,308,303,37,73],"shortwave_radiation_sum":[28.32,24.56,27.81,26.50,26.36,23.46,28.04],"et0_fao_evapotranspiration":[5.23,4.76,5.20,5.16,4.67,4.60,4.99]}}


import unittest
from unittest.mock import MagicMock, patch

# https://www.pythontutorial.net/python-unit-testing/python-mock-requests/
class TestWeatherRequest(unittest.TestCase):

    @patch('weather_etl.requests')
    def test_get_weather_api_response(self, mock_requests):
        pass
