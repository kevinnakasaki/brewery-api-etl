# Brewery API ETL

This project offers an overview of an ELT pipeline following the medallion architecture, using data from [Open Brewery DB API](https://www.openbrewerydb.org/).


## Architecture Overview
### Bronze Layer
Raw data ingestion "as-is", fetching data from the API and storing it in a bucket in the Object Storage.
- **Format**: Parquet
- **Partitioning**: `ingestion_date`, a date based on the API fetching

### Silver Layer
Transformed, normalized and cleaned data, persisted in the Object Storage by `country` and `state`.
- **Format**: Parquet
- **Partitioning**: `country` and `state`

### Gold Layer
Aggregated data to business consumers' analysis.
- **Format**: Parquet
- **Partitioning**: `ingestion_date`, the date of the API fetching

## Setup
### Prequisites
- Docker and Docker Compose

### Steps
#### Create a .env file
Based on the given [.env.example](https://github.com/kevinnakasaki/brewery-api-etl/blob/a33b52f4c5e8a68ac8d90e9e8ddd31389954276f/.env.example):

```txt
# Airflow
AIRFLOW_UID=1000
AIRFLOW_GID=0
AIRFLOW_USERNAME=admin
AIRFLOW_PASSWORD=admin
FERNET_KEY=Vo3PiB2FicePNYIbcLoq3SrG1umhsLgQgLLG5-6HNjI=
SECRET_KEY=1gwxw8chOj1FlqK8lXLayyykmHuaBnooZkERWJgMa5A

# Postgres
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow

# MinIO
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
```

#### Clone the repo
```sh
$~ git clone https://github.com/kevinnakasaki/brewery-api-etl.git
$~ cd brewery-api-etl
```

#### Make commands in the shell
1. Use `make setup` to configure all the services, downloading all the required images and setting them up.
2. After that, use `make run` to start the services locally.

## Links
- [Airflow UI](http://localhost:8080): Use the configured user and passwords in the `.env` file. For this example: *airflow* for both of them.
- [MinIO Console](http://localhost:9001): Use the configured user and password in the `.env` file. For this example: *minioadmin* for both of them.

## Project Structure
```
brewery-api-etl/
├── dags/
│   └── dag.py
├── jobs/
│   ├── utils.py            # common utilities
│   ├── bronze.py           # Bronze layer script
│   ├── silver.py           # Silver layer script
│   ├── gold.py             # Gold layer script
│   └── quality.py          # Data quality script
├── config/
│   └── minio_setup.sh      # MinIO setup
├── docker-compose.yaml     # Services setup
├── Dockerfile              # Custom Airflow image, with Apache Spark, Hadoop and AWS SDKs
├── requirements.txt        # Python dependencies
└── Makefile                # Project entrypoint commands
```