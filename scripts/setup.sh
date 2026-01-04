bash
#!/bin/bash

set -e

echo "🚀 Starting E-Commerce Data Pipeline Setup..."

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

# Check prerequisites
echo -e "${BLUE}Checking prerequisites...${NC}"
command -v docker >/dev/null 2>&1 || { echo "Docker is required but not installed. Aborting." >&2; exit 1; }
command -v docker-compose >/dev/null 2>&1 || { echo "Docker Compose is required but not installed. Aborting." >&2; exit 1; }
command -v terraform >/dev/null 2>&1 || { echo "Terraform is required but not installed. Aborting." >&2; exit 1; }
command -v aws >/dev/null 2>&1 || { echo "AWS CLI is required but not installed. Aborting." >&2; exit 1; }

echo -e "${GREEN}✓ All prerequisites met${NC}"

# Start local services
echo -e "${BLUE}Starting Docker services...${NC}"
cd docker
docker-compose up -d

echo -e "${GREEN}Waiting for services to be ready...${NC}"
sleep 30

# Check service health
echo -e "${BLUE}Checking service health...${NC}"
curl -f http://localhost:8083/ || echo "Debezium not ready yet"
curl -f http://localhost:9092/ || echo "Kafka not ready yet"

# Register Debezium connector
echo -e "${BLUE}Registering Debezium connector...${NC}"
sleep 10
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @../config/debezium-config.json

echo -e "${GREEN}✓ Debezium connector registered${NC}"

# Initialize Terraform
echo -e "${BLUE}Initializing Terraform...${NC}"
cd ../terraform
terraform init

echo -e "${GREEN}✓ Setup complete!${NC}"
echo ""
echo "Services running:"
echo "  - PostgreSQL: localhost:5432"
echo "  - Kafka: localhost:9092"
echo "  - Debezium: http://localhost:8083"
echo "  - Kafka UI: http://localhost:8080"
echo "  - Airflow: http://localhost:8081 (admin/admin)"
echo ""
echo "Next steps:"
echo "  1. Configure AWS credentials: aws configure"
echo "  2. Deploy infrastructure: ./scripts/deploy.sh"
echo "  3. Test pipeline: python scripts/test_pipeline.py"
