"""
Data Quality: Validate data uniqueness and missing data.

"""

import logging

import pyspark.sql.functions as F

from jobs.utils import get_spark, validate_df


def data_quality_job(**kwargs) -> None:
    """
    Apply data quality validations and persist data on an additional layer
    in the Datalake for better analysis.

    """
    spark = get_spark("DataQualityJob")
    try:
        logging.info(f"Reading from Silver Layer")
        df = spark.read.format("parquet").load(kwargs["silver_path"])

        logging.info("Running Data Quality")
        df_quality = validate_df(
            df,
            unique_keys=["id"],
            not_null_cols=["id", "name", "country", "state", "brewery_type"],
        ).withColumns(
            {"datalake_layer": F.lit("silver"), "created_at": F.current_date()}
        )
        logging.info("Saving data into the Quality Layer")
        (
            df_quality.write.mode("overwrite")
            .partitionBy(["datalake_layer", "created_at"])
            .format("parquet")
            .save(kwargs["quality_path"])
        )
        logging.info("Done!")

    except Exception:
        raise
    finally:
        spark.stop()
