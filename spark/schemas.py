from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

SHIP_SCHEMA = StructType([
    StructField("stream", StringType()),
    StructField("location", StringType()),
    StructField("timestamp", StringType()),
    StructField("event", StringType()),
    StructField("track_id", IntegerType()),
    StructField("zone_from", StringType()),
    StructField("zone_to", StringType()),
    StructField("enter_total", IntegerType()),
    StructField("exit_total", IntegerType()),
])

WEATHER_SCHEMA = StructType([
    StructField("stream", StringType()),
    StructField("location", StringType()),
    StructField("timestamp", StringType()),
    StructField("wave_height", DoubleType()),
    StructField("temperature_2m", DoubleType()),
    StructField("precipitation", DoubleType()),
    StructField("weather_code", IntegerType()),
    StructField("visibility", DoubleType()),
    StructField("wind_speed_10m", DoubleType()),
    StructField("day_or_night", StringType()),
    StructField("wind_gusts_10m", DoubleType()),
])
