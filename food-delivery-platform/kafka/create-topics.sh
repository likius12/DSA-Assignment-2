#!/bin/bash


KAFKA_BROKER="kafka:9092"
PARTITIONS=3
REPLICATION=1

TOPICS=(
  "orders.created"
  "orders.confirmed"
  "orders.cancelled"
  "payments.completed"
  "payments.failed"
  "delivery.assigned"
  "delivery.completed"
  "restaurant.menu.updated"
)

echo "Creating Kafka topics..."
for topic in "${TOPICS[@]}"; do
  kafka-topics --create \
    --if-not-exists \
    --bootstrap-server "$KAFKA_BROKER" \
    --topic "$topic" \
    --partitions "$PARTITIONS" \
    --replication-factor "$REPLICATION"
  echo "  ✅ $topic"
done

echo ""
echo "All topics:"
kafka-topics --list --bootstrap-server "$KAFKA_BROKER"