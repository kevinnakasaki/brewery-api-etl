from typing import Any

import logging
import os

import pyspark.sql.functions as F
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, DataFrame


def get_spark(app_name: str, configs: dict[str, str] | None = None) -> SparkSession:
    """Create a SparkSession with proper configuration."""
    configs = configs or {}

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.endpoint", os.environ.get("OBJECT_STORAGE_ENDPOINT")
        )
        .config("spark.hadoop.fs.s3a.access.key", os.environ.get("OBJECT_STORAGE_KEY"))
        .config(
            "spark.hadoop.fs.s3a.secret.key", os.environ.get("OBJECT_STORAGE_SECRET")
        )
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    if configs:
        for key, value in configs.items():
            logging.info(f"Applying additional Spark config '{key} : {value}'")
            spark.conf.set(key, value)

    return spark


def validate_df(
    df: DataFrame,
    unique_keys: list[str] | None = None,
    not_null_cols: list[str] | None = None,
) -> DataFrame:
    """
    Validate a DataFrame and return an aggregated view with the resulting metrics.

    """
    unique_keys: list[str] = unique_keys or []
    not_null_cols: list[str] = not_null_cols or []
    total_count: int = df.count()
    metrics: list[Any] = []

    if unique_keys:
        logging.info("[Uniqueness check] Starting...")
        duplicate_count = (
            df.groupBy(*unique_keys).count().filter(F.col("count") > 1).count()
        )
        pct_duplicate = (duplicate_count / total_count * 100) if total_count else 0
        metrics.append(
            (
                "duplicates",
                ",".join(unique_keys),
                duplicate_count,
                total_count,
                pct_duplicate,
            )
        )
        logging.info(
            f"[Uniqueness check] -> Keys '{unique_keys}' | "
            f"Duplicate records: {duplicate_count} | "
            f"Total records: {total_count} | "
            f"Pct duplicates: {pct_duplicate:.2f}%"
        )
    else:
        logging.warning("[Uniqueness check] Skipping due to missing keys")

    if not_null_cols:
        logging.info("[NULLs check] Starting...")
        for col in not_null_cols:
            null_count = df.filter(F.col(col).isNull()).count()
            pct_null = (null_count / total_count * 100) if total_count else 0
            metrics.append(("NULLs", col, null_count, total_count, pct_null))
            logging.info(
                f"[NULLs check] -> Column '{col}' | "
                f"NULL count: {null_count} | "
                f"Total records: {total_count} | "
                f"Pct NULL: {pct_null:.2f}%"
            )
    else:
        logging.warning("[NULLs check] Skipping due to missing columns to check")

    df_metrics = df.sparkSession.createDataFrame(
        data=metrics,
        schema=[
            "metric",
            "column_or_keys",
            "invalid_count",
            "total_count",
            "pct_invalid",
        ],
    )

    return df_metrics
