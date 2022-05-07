#!/bin/sh

echo "Waiting for DB..."

while ! nc -z $DB_HOST $DB_PORT; do
  sleep 0.1
done

echo "DB started"


echo "Waiting for Elasticsearch..."

while ! nc -z $ES_HOST $ES_PORT; do
  sleep 0.1
done

echo "Elasticsearch started"

exec "$@"
