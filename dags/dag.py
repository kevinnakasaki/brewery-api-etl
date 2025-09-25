from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param

from jobs import bronze_job, silver_job, gold_job, data_quality_job

DEFAULT_ARGS = {
    "owner": "kevinnakasaki",
    "depends_on_past": False,
    "start_date": datetime(2025, 9, 24),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "tags": ["api", "datalake"],
}

# The 'base_url' is static because the whole process was built
# based on its API Docs (schema, pagination, endpoint, etc.)
DEFAULT_PARAMS = {
    "base_url": "https://api.openbrewerydb.org/v1/breweries",
    "bronze_path": "s3a://bronze/breweries",
    "silver_path": "s3a://silver/breweries",
    "gold_path": "s3a://gold/breweries",
    "quality_path": "s3a://quality/breweries",
}


with DAG(
    dag_id="brewery_etl_pipeline",
    default_args=DEFAULT_ARGS,
    schedule=None,
    catchup=False,
    description="Brewery ETL from API to Datalake with Medallion Architecture",
    params={
        "max_records": Param(
            default=100,
            type="integer",
            description="Maximum number of records to fetch from the API.",
        ),
        "spark_configs": Param(
            default={"spark.sql.sources.partitionOverwriteMode": "dynamic"},
            type="object",
            description="Additional Spark configuration as key-value pairs.",
        ),
    },
    render_template_as_native_obj=True,
) as dag:

    DEFAULT_PARAMS.update(
        spark_configs="{{ params.spark_configs }}",
        max_records="{{ params.max_records }}",
    )

    fetch_data_bronze = PythonOperator(
        task_id="bronze",
        python_callable=bronze_job,
        op_kwargs=DEFAULT_PARAMS,
        doc_md="Ingest raw data 'as-is' from Open Brewery API",
    )
    transform_silver = PythonOperator(
        task_id="silver",
        python_callable=silver_job,
        op_kwargs=DEFAULT_PARAMS,
        doc_md="Apply transformations and partition to data",
    )
    aggregate_gold = PythonOperator(
        task_id="gold",
        python_callable=gold_job,
        op_kwargs=DEFAULT_PARAMS,
        doc_md="Create the aggregated view of breweries per type and location",
    )
    data_quality = PythonOperator(
        task_id="data_quality",
        python_callable=data_quality_job,
        op_kwargs=DEFAULT_PARAMS,
        doc_md="Validate data and persist the resulting metrics for future analysis",
    )

    fetch_data_bronze >> transform_silver >> aggregate_gold >> data_quality
