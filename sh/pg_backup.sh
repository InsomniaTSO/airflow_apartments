#!/usr/bin/env bash

# do postgres database backup to s3 storage

DB_NAME=$1
DB_USER=$2
DB_PASS=$3

BUCKET_NAME=postgres-apartment-database

TIMESTAMP=$(date +%F_%T | tr ':' '-')
TEMP_FILE=$(mktemp tmp.XXXXXXXXXX)
S3_FILE="s3://$BUCKET_NAME/backup-$TIMESTAMP"

PGPASSWORD=$DB_PASS 
pg_dump -Fc --no-acl -h localhost -U $DB_USER $DB_NAME > $TEMP_FILE
aws --endpoint-url=https://storage.yandexcloud.net/ \
s3 cp $TEMP_FILE $S3_FILE
rm "$TEMP_FILE"
