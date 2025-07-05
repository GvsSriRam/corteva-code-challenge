#!/bin/bash

# Weather Data Warehouse Deployment Script for AWS EC2
# This script sets up the application on a fresh Ubuntu EC2 instance

set -e  # Exit on any error

echo "🚀 Starting Weather Data Warehouse deployment..."

# Update system
echo "📦 Updating system packages..."
sudo apt-get update
sudo apt-get upgrade -y

# Install required packages
echo "🔧 Installing system dependencies..."
sudo apt-get install -y \
    python3 \
    python3-pip \
    python3-venv \
    nginx \
    postgresql \
    postgresql-contrib \
    curl \
    git \
    unzip

# Create application directory
echo "📁 Setting up application directory..."
sudo mkdir -p /opt/weather-warehouse
sudo chown $USER:$USER /opt/weather-warehouse
cd /opt/weather-warehouse

# Clone or copy application (assuming code is already here)
echo "📋 Setting up application files..."

# Create virtual environment
echo "🐍 Setting up Python virtual environment..."
python3 -m venv venv
source venv/bin/activate

# Install Python dependencies
echo "📚 Installing Python dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

# Set up PostgreSQL
echo "🗄️ Setting up PostgreSQL..."
sudo -u postgres psql -c "CREATE DATABASE weather_db;"
sudo -u postgres psql -c "CREATE USER weather_user WITH PASSWORD 'weather_pass';"
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE weather_db TO weather_user;"

# Configure environment
echo "⚙️ Configuring environment..."
cat > .env << EOF
DATABASE_URL=postgresql://weather_user:weather_pass@localhost:5432/weather_db
FLASK_ENV=production
FLASK_APP=src.app
EOF

# Set up systemd service
echo "🔧 Setting up systemd service..."
sudo tee /etc/systemd/system/weather-warehouse.service > /dev/null << EOF
[Unit]
Description=Weather Data Warehouse API
After=network.target postgresql.service

[Service]
Type=exec
User=$USER
Group=$USER
WorkingDirectory=/opt/weather-warehouse
Environment=PATH=/opt/weather-warehouse/venv/bin
ExecStart=/opt/weather-warehouse/venv/bin/gunicorn --bind 0.0.0.0:5000 --workers 4 --timeout 120 src.app:app
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

# Configure Nginx
echo "🌐 Configuring Nginx..."
sudo tee /etc/nginx/sites-available/weather-warehouse > /dev/null << EOF
server {
    listen 80;
    server_name _;

    location / {
        proxy_pass http://127.0.0.1:5000;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
    }
}
EOF

# Enable Nginx site
sudo ln -sf /etc/nginx/sites-available/weather-warehouse /etc/nginx/sites-enabled/
sudo rm -f /etc/nginx/sites-enabled/default

# Start services
echo "🚀 Starting services..."
sudo systemctl daemon-reload
sudo systemctl enable weather-warehouse
sudo systemctl start weather-warehouse
sudo systemctl restart nginx

# Ingest data
echo "📊 Ingesting weather data..."
source venv/bin/activate
python src/ingest.py

echo "✅ Deployment complete!"
echo "🌐 API is available at: http://$(curl -s http://169.254.169.254/latest/meta-data/public-ipv4)"
echo "📚 API Documentation: http://$(curl -s http://169.254.169.254/latest/meta-data/public-ipv4)/docs"
echo "💚 Health Check: http://$(curl -s http://169.254.169.254/latest/meta-data/public-ipv4)/api/health" 