import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max, to_timestamp

from spark.config import spark_settings
from spark.schemas import SHIP_SCHEMA, WEATHER_SCHEMA
from spark.transforms import hourly_traffic, join_ships_weather


def create_spark(app_name="port-adriano-harbor-batch"):
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

def _read_jsonl(spark, path, schema):
    lines = spark.read.text(str(path))
    from pyspark.sql.functions import from_json
    parsed = lines.select(from_json(col("value"), schema).alias("rec")).select("rec.*")
    return parsed.withColumn("ts", to_timestamp(col("timestamp")))

def run_batch(show=True):
    cfg = spark_settings()
    parquet = cfg["parquet_dir"]
    parquet.mkdir(parents=True, exist_ok=True)

    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    ships_path = cfg["ships_jsonl"]
    weather_path = cfg["weather_jsonl"]

    if not ships_path.exists():
        print(f"No ship events at {ships_path}")
        spark.stop()
        return

    ships = _read_jsonl(spark, ships_path, SHIP_SCHEMA)
    weather = None
    if weather_path.exists():
        weather = _read_jsonl(spark, weather_path, WEATHER_SCHEMA)

    traffic = hourly_traffic(ships)
    traffic.write.mode("overwrite").parquet(str(parquet / "hourly_traffic"))
    ships.write.mode("overwrite").parquet(str(parquet / "ships"))
    if weather is not None:
        weather.write.mode("overwrite").parquet(str(parquet / "weather"))
        enriched = join_ships_weather(ships, weather)
        enriched.write.mode("overwrite").parquet(str(parquet / "ship_weather"))

    occ = ships.groupBy("location").agg(
        spark_max("enter_total").alias("enter_total"),
        spark_max("exit_total").alias("exit_total"),
    ).withColumn("occupancy", col("enter_total") - col("exit_total"))
    occ.write.mode("overwrite").parquet(str(parquet / "occupancy"))

    print(f"Batch analytics written to {parquet}")

    if show:
        print("\n--- Hourly traffic ---")
        traffic.show(truncate=False)
        print("\n--- Occupancy ---")
        occ.show(truncate=False)
        if weather is not None:
            print("\n--- Ship events + weather (sample) ---")
            join_ships_weather(ships, weather).show(truncate=False)

    spark.stop()

def main(argv=None):
    parser = argparse.ArgumentParser(description="Batch harbor analytics from JSONL")
    parser.add_argument("--no-show", action="store_true", help="Skip console tables")
    args = parser.parse_args(argv)
    run_batch(show=not args.no_show)


if __name__ == "__main__":
    main()
