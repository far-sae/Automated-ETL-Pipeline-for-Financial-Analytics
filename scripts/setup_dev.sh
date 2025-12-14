#!/bin/bash
# Setup script for local development

set -e

echo "=== Setting up Financial ETL Pipeline for Development ==="
echo ""

# Create virtual environment
echo "Creating Python virtual environment..."
python3 -m venv venv
source venv/bin/activate

# Install dependencies
echo "Installing Python dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

# Copy env file
if [ ! -f ".env" ]; then
    echo "Creating .env file from template..."
    cp .env.example .env
    echo "⚠ Please update .env with your credentials"
fi

# Create directories
echo "Creating project directories..."
mkdir -p airflow/logs airflow/plugins data logs config tests

echo ""
echo "=== Setup Complete ==="
echo ""
echo "Next steps:"
echo "1. Update .env with your credentials"
echo "2. Run: source venv/bin/activate"
echo "3. Run: ./scripts/deploy.sh"
echo ""
