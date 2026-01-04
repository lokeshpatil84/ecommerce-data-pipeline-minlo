bash
#!/bin/bash

set -e

echo "🚀 Deploying to AWS..."

GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

# Deploy Terraform infrastructure
echo -e "${BLUE}Deploying Terraform infrastructure...${NC}"
cd terraform

terraform plan -out=tfplan
echo "Review the plan above. Press Enter to continue or Ctrl+C to cancel..."
read

terraform apply tfplan

echo -e "${GREEN}✓ Infrastructure deployed${NC}"

# Get outputs
ICEBERG_BUCKET=$(terraform output -raw iceberg_bucket)
GLUE_DATABASE=$(terraform output -raw glue_database)

echo ""
echo "Deployment complete!"
echo "  - Iceberg Bucket: ${ICEBERG_BUCKET}"
echo "  - Glue Database: ${GLUE_DATABASE}"
echo ""
echo "To start streaming job:"
echo "  aws glue start-job-run --job-name ecommerce-pipeline-kafka-to-iceberg"
