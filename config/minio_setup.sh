#!/bin/sh
sleep 10 # wait until the service is available
mc alias set minio http://minio:9000 $MINIO_ACCESS_KEY $MINIO_SECRET_KEY
mc mb -p minio/bronze
mc anonymous set public minio/bronze
mc mb -p minio/silver
mc anonymous set public minio/silver
mc mb -p minio/gold
mc anonymous set public minio/gold
mc mb -p minio/quality
mc anonymous set public minio/quality
echo "MinIO setup successfully finished!"
