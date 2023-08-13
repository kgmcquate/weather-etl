import os, json, boto3
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DateType, StringType, FloatType, IntegerType, TimestampType
import dataclasses
import datetime
from .database import engine
from .config import DEFAULT_LOOKBACK_DAYS, API_PARALLELISM, lakes_table_name, weather_by_day_table_name, weather_by_day_temp_table_name
from .data_models import DailyWeather, WeatherRequest
import logging
from sqlalchemy import text

NUMBER_OF_WATERBODIES_LIMIT = 1200

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
        .sort(col("areasqkm").desc())
        .limit(NUMBER_OF_WATERBODIES_LIMIT)
        .select(
            "latitude", 
            "longitude"
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
        # Get the start and end times for each location, since the API takes a lat/lng and date range
        .groupBy("latitude", "longitude").agg(
            min(col("date")).alias("start_date"),
            max(col("date")).alias("end_date")
        )
    )


    responses_rdd = (
        weathers_to_get
        # Limit for testing
        # .limit(1000)
        # Turn into API requests
        .rdd
        .map(lambda row: WeatherRequest(**row.asDict()))
        # Only run 5 requests concurrently
        .coalesce(API_PARALLELISM)
        # Send request to API
        # Each request turns a list of weathers, so flatMap
        .map(lambda req: req.get_weather_data())
    )

    errors = responses_rdd.filter(lambda x: isinstance(x, Exception))
    if logger.isEnabledFor(logging.DEBUG):
        errors.cache()
        cnt = errors.count()
        if cnt > 0:
            logger.debug(f"{str(cnt)} Errors:")
            logger.debug(errors.take(100))

    
    weathers_rdd = (
        responses_rdd
        .filter(lambda x: isinstance(x, list))
        .flatMap(lambda x: x)
        .map(lambda x: x.__dict__)
    )


    if logger.isEnabledFor(logging.DEBUG):
        weathers_rdd.cache()
        logger.debug(weathers_rdd.take(10))

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
        cnt = new_weathers.count()
        logger.debug(f"Number of successful results: {cnt}")
        new_weathers.show()


    # TODO write to stage table and merge to avoid conflicts from live updates from the API
    (
        new_weathers.write
        .option("dbtable", weather_by_day_temp_table_name) 
        .options(**get_jdbc_options())
        .format("jdbc")
        .mode("overwrite")
        .save()
    )

    cols_string = ','.join([f'"{c}"' for c in new_weathers.columns])

    merge_sql = f"""
            MERGE INTO {weather_by_day_table_name} tgt
            USING {weather_by_day_temp_table_name} src
            ON tgt.date = src.date AND tgt.longitude = src.longitude AND tgt.latitude = src.latitude
            WHEN MATCHED THEN do NOTHING
            when not matched then insert ({cols_string})
            values ({cols_string})
        """

    with engine.connect() as con:
        resp = con.execute(text(merge_sql))
        print(resp)
