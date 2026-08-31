"""Continuously aggregate words from atomically arriving text files."""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession, functions as F


def word_counts(lines: DataFrame) -> DataFrame:
    return (
        lines.select(F.explode(F.split(F.lower(F.col("value")), r"\W+")).alias("word"))
        .filter(F.length("word") > 1)
        .groupBy("word")
        .count()
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--checkpoint", required=True)
    parser.add_argument("--trigger-once", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("review-word-count-stream").getOrCreate()
    lines = spark.readStream.format("text").option("maxFilesPerTrigger", 1).load(args.input)
    writer = (
        word_counts(lines)
        .writeStream.outputMode("complete")
        .format("console")
        .option("checkpointLocation", args.checkpoint)
    )
    if args.trigger_once:
        writer = writer.trigger(availableNow=True)
    writer.start().awaitTermination()


if __name__ == "__main__":
    main()

