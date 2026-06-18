import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max, to_timestamp

from spark.config import spark_settings
from spark.schemas import SHIP_SCHEMA, WEATHER_SCHEMA
from spark.transforms import hourly_traffic, join_ships_weather, parse_kafka_json

def _kafka_packages():
    import pyspark

    version = pyspark.__version__
    major = int(version.split(".")[0])
    scala = "2.13" if major >= 4 else "2.12"
    return f"org.apache.spark:spark-sql-kafka-0-10_{scala}:{version}"

def create_spark(app_name="port-adriano-harbor-analytics"):
    kafka_packages = _kafka_packages()
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.jars.packages", kafka_packages)
        .getOrCreate()
    )

def _kafka_source(spark, bootstrap, topic):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
    )

def _parquet_sink(df, path, checkpoint, trigger_sec):
    return (
        df.writeStream
        .format("parquet")
        .option("path", str(path))
        .option("checkpointLocation", str(checkpoint))
        .outputMode("append")
        .trigger(processingTime=f"{trigger_sec} seconds")
    )

def _console_sink(df, label, trigger_sec):
    return (
        df.writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", False)
        .queryName(label)
        .trigger(processingTime=f"{trigger_sec} seconds")
    )

def _ensure_ts(df):
    if "ts" in df.columns:
        return df
    return df.withColumn("ts", to_timestamp(col("timestamp")))

def run_streaming(console=False, harbor_id=None):
    cfg = spark_settings(harbor_id=harbor_id)
    parquet = cfg["parquet_dir"]
    ckpt = cfg["checkpoint_dir"]

    for sub in ("ships", "weather", "hourly_traffic", "ship_weather", "occupancy"):
        (parquet / sub).mkdir(parents=True, exist_ok=True)
    ckpt.mkdir(parents=True, exist_ok=True)

    spark = create_spark(f"{cfg['harbor_id']}-harbor-analytics")
    spark.sparkContext.setLogLevel("WARN")

    bootstrap = cfg["bootstrap_servers"]
    trigger = cfg["trigger_seconds"]

    ships = parse_kafka_json(_kafka_source(spark, bootstrap, cfg["ship_topic"]), SHIP_SCHEMA)
    weather = parse_kafka_json(
        _kafka_source(spark, bootstrap, cfg["weather_topic"]), WEATHER_SCHEMA,
    )

    queries = [
        _parquet_sink(ships, parquet / "ships", ckpt / "ships", trigger).start(),
        _parquet_sink(weather, parquet / "weather", ckpt / "weather", trigger).start(),
    ]

    def analyze_batch(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            return
        weather_path = parquet / "weather"
        if not weather_path.exists() or not any(weather_path.iterdir()):
            return
        w = _ensure_ts(spark.read.parquet(str(weather_path)))

        join_ships_weather(batch_df, w).write.mode("append").parquet(str(parquet / "ship_weather"))
        hourly_traffic(batch_df).write.mode("append").parquet(str(parquet / "hourly_traffic"))

        batch_df.groupBy("location").agg(
            spark_max("enter_total").alias("enter_total"),
            spark_max("exit_total").alias("exit_total"),
        ).withColumn("occupancy", col("enter_total") - col("exit_total")).write.mode(
            "append",
        ).parquet(str(parquet / "occupancy"))

    queries.append(
        ships.writeStream
        .foreachBatch(analyze_batch)
        .option("checkpointLocation", str(ckpt / "analytics"))
        .trigger(processingTime=f"{trigger} seconds")
        .start(),
    )

    if console:
        queries.append(_console_sink(ships, "ships", trigger).start())
        queries.append(_console_sink(weather, "weather", trigger).start())

    print(f"Spark streaming started (trigger={trigger}s)")
    print(f"  harbor: {cfg['location']} ({cfg['harbor_id']})")
    print(f"  Kafka: {bootstrap}")
    print(f"  topics: {cfg['ship_topic']}, {cfg['weather_topic']}")
    print(f"  parquet: {parquet}")
    if console:
        print("  console: ships + weather")
    print("  Ctrl+C to stop")

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        pass
    finally:
        for q in queries:
            if q.isActive:
                q.stop()
        spark.stop()

def main(argv=None):
    parser = argparse.ArgumentParser(description="Harbor Spark streaming analytics")
    parser.add_argument(
        "--harbor",
        default=os.getenv("HARBOR"),
        help="Harbor id",
    )
    parser.add_argument(
        "--console",
        action="store_true",
        help="Print ship + weather events to stdout",
    )
    args = parser.parse_args(argv)
    run_streaming(console=args.console, harbor_id=args.harbor)

if __name__ == "__main__":
    main()
