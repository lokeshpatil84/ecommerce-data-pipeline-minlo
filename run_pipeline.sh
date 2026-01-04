#!/bin/bash
set -e

echo "=========================================="
echo "  E-Commerce CDC Pipeline - End to End"
echo "=========================================="

cd /home/lokeshp6/Desktop/ecommerce-data-pipeline/docker

# Step 1: Start all services
echo ""
echo "Step 1: Starting Docker services..."
docker-compose up -d

# Step 2: Wait for services
echo ""
echo "Step 2: Waiting for services (30 seconds)..."
sleep 30

# Step 3: Register Debezium connector
echo ""
echo "Step 3: Registering Debezium connector..."
curl -s -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @/home/lokeshp6/Desktop/ecommerce-data-pipeline/config/debezium-config.json
echo ""

# Step 4: Wait for connector
sleep 5

# Step 5: Insert test data
echo ""
echo "Step 4: Inserting test orders..."
docker exec ecommerce-postgres psql -U postgres -d ecommerce -c "
INSERT INTO ecommerce.orders (customer_id, total_amount, status, shipping_address) VALUES 
(1, 99.99, 'pending', 'Test Address 1'), 
(2, 149.99, 'processing', 'Test Address 2'),
(3, 299.99, 'shipped', 'Test Address 3');
"

# Step 6: Verify CDC events
echo ""
echo "Step 5: Verifying CDC events in Kafka..."
EVENTS=$(docker exec kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic dbserver1.ecommerce.orders --from-beginning --max-messages 3 2>/dev/null | grep -c "order_id" || echo "0")
echo "CDC Events captured: $EVENTS"

echo ""
echo "=========================================="
echo "  Pipeline Execution Complete!"
echo "=========================================="
echo ""
echo "Access Points:"
echo "  - Kafka UI:  http://localhost:8080"
echo "  - Airflow:   http://localhost:8090 (admin/admin)"
echo "  - Debezium:  http://localhost:8083"
echo ""
echo "Kafka Topics:"
echo "  - dbserver1.ecommerce.orders"
echo "  - dbserver1.ecommerce.customers"
echo "  - dbserver1.ecommerce.products"

