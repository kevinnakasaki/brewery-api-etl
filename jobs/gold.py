"""
Gold: Aggregated views about data.

"""

import logging

import pyspark.sql.functions as F

from jobs.utils import get_spark


def gold_job(**kwargs) -> None:
    """
    Gold job creating aggregated views with brewery data.

    Aggregations in this job:
        - Quantity of breweries per type and location
    Statistics:
        - Count of breweries
        - Count of Brewery types
        - Count of breweries with valid websites

    """
    spark = get_spark("GoldJob")
    try:
        source = kwargs["silver_path"]
        logging.info(f"Reading from source: {source}")
        df = spark.read.format("parquet").load(source)

        # 'Cause we've already used a partition strategy in the data on silver
        # we don't need to repartition data as we did from bronze to silver
        logging.info("Creating aggregated view of 'Breweries per Type and Location'")
        df_breweries_type_location = (
            df.groupBy("brewery_type", "country", "state", "city")
            .agg(F.count("*").alias("breweries_qty"))
            .orderBy(F.col("breweries_qty").desc())
        )
        logging.info(
            f"Breweries per Type and Location created: {df_breweries_type_location.count()} records"
        )
        logging.info("Breweries per Type and Location sample:")
        df_breweries_type_location.show(5, truncate=False)

        logging.info("Saving data into the Gold Layer")
        (
            df_breweries_type_location.write.mode("overwrite")
            .partitionBy("ingestion_date")
            .format("parquet")
            .save(kwargs["gold_path"])
        )
        logging.info("Done!")

        logging.info("Collecting Gold statistics:")
        total_breweries = df.count()
        qty_brewery_types = df.select("brewery_type").distinct().count()
        breweries_with_valid_websites = df.filter(
            (F.col("website_url").isNotNull()) & (F.col("website_url") != "")
        ).count()
        logging.info(f"Count of breweries: {total_breweries}")
        logging.info(f"Count of brewery types: {qty_brewery_types}")
        logging.info(
            f"Count of breweries with valid websites: {breweries_with_valid_websites}"
        )
    except Exception:
        raise
    finally:
        spark.stop()
