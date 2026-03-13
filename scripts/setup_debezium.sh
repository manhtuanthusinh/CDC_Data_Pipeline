#!/bin/sh

# Use the service name defined in docker-compose
DEBEZIUM_URL="http://debezium:8083"
CONNECTOR_NAME="mysql-inventory-connector"

echo "Waiting for Debezium to be ready at $DEBEZIUM_URL..."
until [ "$(curl -s -o /dev/null -w '%{http_code}' $DEBEZIUM_URL/)" = "200" ]; do
  sleep 2
done

echo "Debezium is ready!"

# Check if the connector already exists
if curl -s "$DEBEZIUM_URL/connectors" | grep -q "$CONNECTOR_NAME"; then
    echo "Connector $CONNECTOR_NAME already exists. Deleting it..."
    curl -s -X DELETE "$DEBEZIUM_URL/connectors/$CONNECTOR_NAME"
    sleep 2
fi

echo "Registering Debezium connector..."
# We use -i to see the HTTP response code (201 is success)
curl -i -X POST \
  -H "Accept: application/json" \
  -H "Content-Type: application/json" \
  "$DEBEZIUM_URL/connectors" \
  -d @/configs/debezium_connector.json

echo "\nConnector registration request sent!"

echo "Waiting for connector to reach RUNNING state..."
# This loop checks the actual internal status of the task
until curl -s "$DEBEZIUM_URL/connectors/$CONNECTOR_NAME/status" | grep -q "RUNNING"; do
  echo "Still waiting for connector status to be RUNNING..."
  sleep 2
done

echo "Connector is RUNNING and ready for Spark!"