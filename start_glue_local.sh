#!/bin/bash

# AWS Glue Local with MinIO Setup Script
# This script sets up AWS Glue-like environment locally with MinIO (S3-compatible storage)

set -e

echo "🚀 Starting AWS Glue Local with MinIO..."

# Navigate to docker directory
cd "$(dirname "$0")/docker"

# Create MinIO bucket for warehouse
create_bucket() {
    echo "📦 Creating MinIO bucket: $1"
    curl -s -X PUT -u admin:admin123 \
        "http://localhost:9000/$1" || echo "Bucket $1 already exists"
}

# Start MinIO in background if not running
if ! docker ps | grep -q minio; then
    echo "🐳 Starting MinIO container..."
    docker-compose up -d minio

    # Wait for MinIO to be ready
    echo "⏳ Waiting for MinIO to be ready..."
    sleep 5

    # Create bucket
    create_bucket "warehouse"
    echo "✅ MinIO started successfully"
    echo "   - MinIO Console: http://localhost:9001 (admin/admin123)"
    echo "   - S3 Endpoint: http://localhost:9000"
else
    echo "✅ MinIO is already running"
fi

# Build Glue Local image
echo "🔨 Building AWS Glue Local Docker image..."
docker-compose build glue-local

echo ""
echo "🎉 Setup complete!"
echo ""
echo "📋 Usage:"
echo "   To run Kafka to Iceberg job:"
echo "   docker-compose run --rm glue-local"
echo ""
echo "   To run aggregation job:"
echo "   docker-compose run --rm glue-local python3 /workspace/glue-jobs/iceberg_aggregation.py --JOB_NAME aggregation --iceberg_warehouse s3a://warehouse/ --database_name ecommerce"
echo ""
echo "📦 Data will be stored in:"
echo "   - MinIO at: s3a://warehouse/ecommerce/"
echo ""
echo "🌐 Access MinIO Console: http://localhost:9001"
echo "   Username: admin"
echo "   Password: admin123"

