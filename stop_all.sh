#!/bin/bash

# Stop all Docker containers and services

echo "🛑 Stopping all Docker services..."

cd docker

# Stop and remove all containers
docker-compose down

# Also stop any orphaned containers
docker ps -q --filter "name=ecommerce" --filter "name=kafka" --filter "name=debezium" --filter "name=minio" --filter "name=airflow" | xargs -r docker stop

echo "✅ All services stopped!"
echo ""
echo "Stopped containers:"
echo "   - ecommerce-postgres (5432)"
echo "   - kafka (9092)"
echo "   - debezium (8083)"
echo "   - kafka-ui (8080)"
echo "   - airflow-webserver (8090)"
echo "   - airflow-scheduler"
echo "   - minio (9000, 9001)"

