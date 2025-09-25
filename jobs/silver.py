"""
Silver: Data cleaning, normalization and partitioning by location.

"""

import logging
from datetime import datetime

import pyspark.sql.functions as F
from pyspark.sql import Column
from pyspark.sql.types import DoubleType

from jobs.utils import get_spark


# Util function to DRY
def _trim_column(column: str) -> Column:
    return F.trim(F.col(column))


def silver_job(**kwargs) -> None:
    """
    Silver job transformations, normalization and partitioning.

    1. Transformations and normalizations:
        - Remove trailing and leading whitespaces
        - Create a flag to valid postal codes
        - Validate and convert coordinates
        - Standardized brewery types, countries and states
        - Standardized adress field
        - Handle missing values
    2. Partitioning:
        - Partition by country and state_province

    """
    spark = get_spark("SilverJob")
    try:
        source = kwargs["bronze_path"]
        logging.info(f"Reading from source: {source}")
        df = spark.read.format("parquet").load(source)

        logging.info(f"Filtering only date >= '{str(datetime.now().date())}'")
        df = df.filter(F.col("ingestion_date") >= F.current_date())

        logging.info("Repartitioning to avoid data skewness")
        cores = spark.sparkContext.defaultParallelism
        # https://fractal.ai/blog/databricks-spark-jobs-optimization-techniques-shuffle-partition-technique
        partitions = cores * 2
        logging.info(
            f"Repartitioning: {df.rdd.getNumPartitions()} to {partitions} partitions"
        )
        df = df.repartition(partitions)

        logging.info(f"Bronze Layer record count: {df.count()}")

        logging.info("Removing invalid records")
        df = df.filter((F.col("id").isNotNull()) & (F.col("id") != "")).filter(
            (F.col("name").isNotNull()) & (F.col("name") != "")
        )

        logging.info("Trimming records")
        df = df.withColumns(
            {
                c: _trim_column(c)
                for c in (
                    "name",
                    "brewery_type",
                    "city",
                    "state_province",
                    "postal_code",
                    "country",
                    "phone",
                    "website_url",
                    "state",
                    "street",
                )
            }
        )

        logging.info("Create a flag to identify valid postal codes")
        df = df.withColumn(
            "fl_valid_postal_code",
            F.rlike("postal_code", F.lit(r"([0-9]{5}-[0-9]{4})")),
        )

        logging.info("Validating and casting coordinates")
        df = df.withColumns(
            {
                f"valid_{c}": F.when(
                    F.col(c).rlike(r"^(-?\d+\.\d*)$"), F.col(c).cast(DoubleType())
                ).otherwise(F.lit(None).cast(DoubleType()))
                for c in ("longitude", "latitude")
            }
        )

        logging.info("Standardize brewery type, country and state")
        df = df.withColumns(
            {c: F.upper(F.col(c)) for c in ("brewery_type", "country", "state")}
        )

        logging.info("Standardize adress")
        df = df.withColumn(
            "valid_address",
            F.coalesce(
                F.col("address_1"),
                F.col("street"),
                F.col("address_2"),
                F.col("address_3"),
            ),
        )

        logging.info("Validate partitioning columns")
        df = df.withColumns(
            {
                c: F.when(F.col(c).isNull(), "UNKNOWN").otherwise(F.col(c))
                for c in ("country", "state")
            }
        )

        logging.info(
            f"Count after all transformations and normalizations: {df.count()}"
        )

        logging.info("Saving data into the Silver Layer")
        (
            df.write.mode("overwrite")
            .partitionBy(["country", "state"])
            .format("parquet")
            .save(kwargs["silver_path"])
        )
        logging.info("Done!")

    except Exception:
        raise
    finally:
        spark.stop()
