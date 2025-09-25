"""
Bronze: Raw data API ingestion "as-is"

"""

from typing import Any

import logging

import requests
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, StructField, StructType
from requests import HTTPError

from jobs.utils import get_spark

SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("brewery_type", StringType(), True),
        StructField("address_1", StringType(), True),
        StructField("address_2", StringType(), True),
        StructField("address_3", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state_province", StringType(), True),
        StructField("postal_code", StringType(), True),
        StructField("country", StringType(), True),
        StructField("longitude", StringType(), True),
        StructField("latitude", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("website_url", StringType(), True),
        StructField("state", StringType(), True),
        StructField("street", StringType(), True),
    ]
)


def _fetch_data(url: str, max_records: int) -> list[Any]:
    """Fetch API data with pagination"""
    data = []
    page = 1
    while len(data) < max_records:
        try:
            response = requests.get(url, params={"page": page, "per_page": 200})
            response.raise_for_status()
            data.extend(response.json())
            if len(data) < 200:
                break
            page += 1
        except HTTPError:
            logging.warning(f"There are no data to fetch. Ending...")
    return data[:max_records]


def bronze_job(**kwargs) -> None:
    """
    Bronze job ingesting from Brewery DB API and persisting it into the Data Lake.

    1. Fetch data from API
    2. Store raw JSON with control columns
    3. Save to bronze layer in Parquet to easy the reading in the future

    """
    logging.info("Fetching data from the API")
    data = _fetch_data(
        url=kwargs["base_url"], max_records=kwargs.get("max_records", 1000)
    )
    logging.info(f"Returned {len(data)} records from the API")

    logging.info("Initializing Spark Application...")
    spark = get_spark("BronzeJob")
    try:
        df = spark.createDataFrame(data, schema=SCHEMA)
        df = df.withColumn("ingestion_timestamp", F.current_timestamp()).withColumn(
            "ingestion_date", F.col("ingestion_timestamp").cast("date")
        )
        logging.info("Saving data into the Bronze Layer")
        (
            df.write.mode("overwrite")
            .partitionBy("ingestion_date")
            .format("parquet")
            .save(kwargs["bronze_path"])
        )
        logging.info("Done!")
    except Exception:
        raise
    finally:
        spark.stop()
