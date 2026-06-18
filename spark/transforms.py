from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_json, to_timestamp, date_trunc, row_number
from pyspark.sql.types import StructType
from pyspark.sql.window import Window

def parse_kafka_json(df, schema: StructType, value_col="value") -> DataFrame:
    parsed = df.select(
        from_json(col(value_col).cast("string"), schema).alias("rec"),
    ).select("rec.*")
    return parsed.withColumn("ts", to_timestamp(col("timestamp")))

def parse_json_lines(df, schema: StructType, value_col="value") -> DataFrame:
    return parse_kafka_json(df, schema, value_col=value_col)

def with_hour(df: DataFrame) -> DataFrame:
    return df.withColumn("hour", date_trunc("hour", col("ts")))

def weather_by_hour(weather: DataFrame) -> DataFrame:
    w = with_hour(weather)
    win = Window.partitionBy("hour", "location").orderBy(col("ts").desc())
    return w.withColumn("rn", row_number().over(win)).filter(col("rn") == 1).drop("rn")

def join_ships_weather(ships: DataFrame, weather: DataFrame) -> DataFrame:
    s = with_hour(ships).alias("s")
    w = weather_by_hour(weather).alias("w")
    return s.join(w, (col("s.hour") == col("w.hour")) & (col("s.location") == col("w.location")), "left").select(
        col("s.ts"),
        col("s.location"),
        col("s.event"),
        col("s.track_id"),
        col("s.zone_from"),
        col("s.zone_to"),
        col("s.enter_total"),
        col("s.exit_total"),
        col("w.wave_height"),
        col("w.temperature_2m"),
        col("w.wind_speed_10m"),
        col("w.wind_gusts_10m"),
        col("w.visibility"),
        col("w.weather_code"),
        col("w.day_or_night"),
        col("w.precipitation"),
    )

def hourly_traffic(ships: DataFrame) -> DataFrame:
    from pyspark.sql.functions import count, sum as spark_sum, when

    return with_hour(ships).groupBy("hour", "location").agg(
        spark_sum(when(col("event") == "enter", 1).otherwise(0)).alias("enters"),
        spark_sum(when(col("event") == "exit", 1).otherwise(0)).alias("exits"),
        count("*").alias("events"),
    ).orderBy("hour")
