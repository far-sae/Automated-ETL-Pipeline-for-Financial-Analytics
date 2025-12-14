#!/bin/bash
# Deployment script for Financial ETL Pipeline

set -e

echo "=== Financial ETL Pipeline Deployment ==="
echo ""

# Color codes for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Function to print colored output
print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

# Check prerequisites
echo "Checking prerequisites..."

if ! command -v docker &> /dev/null; then
    print_error "Docker is not installed"
    exit 1
fi
print_success "Docker is installed"

if ! command -v docker-compose &> /dev/null; then
    print_error "Docker Compose is not installed"
    exit 1
fi
print_success "Docker Compose is installed"

# Check .env file
if [ ! -f ".env" ]; then
    print_warning ".env file not found, copying from .env.example"
    cp .env.example .env
    print_warning "Please update .env with your credentials before proceeding"
    exit 1
fi
print_success ".env file found"

# Create necessary directories
echo ""
echo "Creating directories..."
mkdir -p airflow/logs airflow/plugins data logs config
print_success "Directories created"

# Build Docker images
echo ""
echo "Building Docker images..."
docker-compose build
print_success "Docker images built"

# Start services
echo ""
echo "Starting services..."
docker-compose up -d

# Wait for services to be healthy
echo ""
echo "Waiting for services to be ready..."
sleep 10

# Check service health
echo ""
echo "Checking service health..."

# Check PostgreSQL
if docker-compose exec -T postgres pg_isready -U etl_user > /dev/null 2>&1; then
    print_success "PostgreSQL is ready"
else
    print_error "PostgreSQL is not ready"
fi

# Check Redis
if docker-compose exec -T redis redis-cli ping > /dev/null 2>&1; then
    print_success "Redis is ready"
else
    print_error "Redis is not ready"
fi

# Check Airflow Webserver
sleep 20
if curl -f http://localhost:8080/health > /dev/null 2>&1; then
    print_success "Airflow Webserver is ready"
else
    print_warning "Airflow Webserver may still be starting..."
fi

# Display access information
echo ""
echo "=== Deployment Complete ==="
echo ""
echo "Access Information:"
echo "  Airflow UI: http://localhost:8080"
echo "  Username: admin"
echo "  Password: admin"
echo ""
echo "PostgreSQL:"
echo "  Host: localhost"
echo "  Port: 5432"
echo "  Database: financial_analytics"
echo "  User: etl_user"
echo ""
echo "Redis:"
echo "  Host: localhost"
echo "  Port: 6379"
echo ""
echo "Useful Commands:"
echo "  View logs: docker-compose logs -f [service_name]"
echo "  Stop services: docker-compose down"
echo "  Restart services: docker-compose restart"
echo "  Trigger DAG: docker-compose exec airflow-scheduler airflow dags trigger financial_etl_pipeline"
echo ""
print_success "ETL Pipeline is ready!"
