"""Windowed event-time metrics for a JSON IoT stream."""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession, functions as F, types as T


EVENT_SCHEMA = T.StructType(
    [
        T.StructField("time", T.TimestampType(), nullable=False),
        T.StructField("action", T.StringType(), nullable=False),
    ]
)


def window_metrics(events: DataFrame, window_size: str, watermark: str) -> DataFrame:
    return (
        events.withWatermark("time", watermark)
        .groupBy(F.window("time", window_size), "action")
        .count()
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "action",
            "count",
        )
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--checkpoint", required=True)
    parser.add_argument("--window", default="1 hour")
    parser.add_argument("--watermark", default="2 hours")
    parser.add_argument("--trigger-once", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("iot-window-monitor").getOrCreate()
    events = (
        spark.readStream.schema(EVENT_SCHEMA)
        .option("maxFilesPerTrigger", 1)
        .json(args.input)
    )
    writer = (
        window_metrics(events, args.window, args.watermark)
        .writeStream.outputMode("update")
        .format("console")
        .option("checkpointLocation", args.checkpoint)
    )
    if args.trigger_once:
        writer = writer.trigger(availableNow=True)
    writer.start().awaitTermination()


if __name__ == "__main__":
    main()

