import os, json, boto3
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DateType, StringType, FloatType, IntegerType, TimestampType
import dataclasses
import datetime
# import database
from .config import DEFAULT_LOOKBACK_DAYS, lakes_table_name, weather_by_day_table_name
from .data_models import DailyWeather, WeatherRequest
import logging


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
    from pyspark.sql.functions import col, lit, max, min, coalesce

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
        .select(
            coalesce("latitude", "nearby_city_latitude").alias("latitude"), 
            coalesce("longitude", "nearby_city_longitude").alias("longitude")
        )
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


    needed_dates_df = (
        spark.createDataFrame(
            [[d] for d in weather_dates],
            schema=StructType().add(field="date", data_type=DateType())
        )
        .hint("broadcast")
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

    def get_pyspark_type(the_type):
        return {
            float: FloatType(),
            int: IntegerType(),
            str: StringType(),
            datetime.date: DateType()
        }[the_type]

    daily_weather_schema = StructType([
                            StructField(name=name, dataType=get_pyspark_type(python_type)) 
                            for name, python_type 
                            in DailyWeather.__annotations__.items()
                        ])

    new_weathers = (
        weathers_rdd.toDF(schema=daily_weather_schema)
        .withColumn("date", col("date").cast(DateType()))
        .withColumn("sunrise", col("sunrise").cast(TimestampType()))
        .withColumn("sunset", col("sunset").cast(TimestampType()))
    )

    if logger.isEnabledFor(logging.DEBUG):
        new_weathers.cache()
        new_weathers.show()

    (
        new_weathers.write
        .option("dbtable", "daily_weather")
        .options(**get_jdbc_options())
        .format("jdbc")
        .mode("append")
        .save()
    )


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


