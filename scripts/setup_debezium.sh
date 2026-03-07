#!/bin/sh

echo "Waiting for Debezium to be ready..."

until [ "$(curl -s -o /dev/null -w '%{http_code}' http://localhost:8083/)" = "200" ]; do
  sleep 2
done

echo "Debezium is ready!"

echo "Registering Debezium connector..."

curl -s -X POST \
  -H "Accept: application/json" \
  -H "Content-Type: application/json" \
  http://localhost:8083/connectors \
  -d @/configs/debezium_connector.json

echo "Connector registration completed!"