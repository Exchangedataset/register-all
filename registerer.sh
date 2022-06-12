#!/bin/bash

set -o pipefail

DATABASE_HOST="exchangedataset-database.cqdpweplfhn7.us-east-2.rds.amazonaws.com"
DATABASE_USER="dump"
DATABASE_PASSWORD="???"
DATABASE_NAME="dataset_info"
DATABASE_PORT="25883"
CA_ROOT="rds-ca-2019-root.pem"
S3_BUCKET="exchangedataset-data"

if [ -f "stop" ]; then
  exit 1
fi
touch "stop"

for file in "$1"*; do
  path=$file

  echo "checking $path"
  head=$(gzip -dc "$path" | head -n1)
  res=$?
  # if command failed, gzip file is corrupted
  # 141 is SIGPIPE error, which gzip will return because pipe to head
  # is dead
  if [ $res -ne 0 ] && [ $res -ne 141 ]; then
    rm "stop"
    exit 1
  fi
  tail=$(gzip -dc "$path" | tail -n1)
  res=$?
  # is corrupted?
  if [ $? -ne 0 ] && [ $res -ne 141 ]; then
    rm "stop"
    exit 1
  fi
  if [ -v last_path ]; then
    last_file_name=$(basename "$last_file")
    SQL="INSERT INTO datasets VALUES(\"$last_file_name\", \"$exchange\", $start_nanosec, $end_nanosec, $is_start)"
    aws s3 cp "$last_path" s3://"$S3_BUCKET"/
    mysql --host="$DATABASE_HOST" --user="$DATABASE_USER" --password="$DATABASE_PASSWORD" --port="$DATABASE_PORT" --ssl-ca "$CA_ROOT" --execute="$SQL" "$DATABASE_NAME"
    if [ $? -ne 0 ]; then
      aws rm s3://"$S3_BUCKET"/"$last_file_name"
      exit 1
    fi
    rm "$last_path"
  fi
  exchange=$(basename "$file" | cut -d'_' -f1)
  start_nanosec=$(echo "$head" | cut -f2)
  end_nanosec=$(echo "$tail" | cut -f2)
  is_start=0
  if [ "$(echo "$head" | cut -f1)" = 'start' ]; then
    is_start=1
  fi
  last_path=$path
  last_file=$file
done

rm "stop"
