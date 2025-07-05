# Production Deployment Guide: AWS ECS/Fargate

## Overview

This guide describes a production-ready deployment of the Weather Data Warehouse API using AWS managed services for scalability, reliability, and maintainability.

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   CloudFront    │    │   Route 53      │    │   API Gateway   │
│   (CDN)         │    │   (DNS)         │    │   (Optional)    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                    ┌─────────────────┐
                    │   Application   │
                    │   Load Balancer │
                    └─────────────────┘
                                 │
                    ┌─────────────────┐
                    │   ECS Cluster   │
                    │   (Fargate)     │
                    └─────────────────┘
                                 │
         ┌───────────────────────┼───────────────────────┐
         │                       │                       │
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   RDS           │    │   S3            │    │   EventBridge   │
│   (PostgreSQL)  │    │   (Data Files)  │    │   (Scheduler)   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         │                       │                       │
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   ElastiCache   │    │   CloudWatch    │    │   Lambda        │
│   (Redis)       │    │   (Monitoring)  │    │   (Ingestion)   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## AWS Services Used

### Core Services
- **ECS (Elastic Container Service) with Fargate**: Container orchestration
- **RDS (Relational Database Service)**: Managed PostgreSQL database
- **Application Load Balancer**: Traffic distribution and SSL termination
- **S3**: Data file storage and static assets
- **EventBridge**: Scheduled data ingestion
- **Lambda**: Serverless data processing

### Supporting Services
- **CloudWatch**: Monitoring, logging, and alerting
- **IAM**: Security and access management
- **VPC**: Network isolation
- **ElastiCache**: Redis for caching (optional)
- **Secrets Manager**: Secure credential storage

## Deployment Steps

### 1. Infrastructure Setup (Terraform/CloudFormation)

#### VPC and Networking
```hcl
# Sample Terraform configuration
resource "aws_vpc" "main" {
  cidr_block = "10.0.0.0/16"
  
  tags = {
    Name = "weather-warehouse-vpc"
  }
}

resource "aws_subnet" "private" {
  count = 2
  vpc_id = aws_vpc.main.id
  cidr_block = "10.0.${count.index + 1}.0/24"
  availability_zone = data.aws_availability_zones.available.names[count.index]
  
  tags = {
    Name = "weather-warehouse-private-${count.index + 1}"
  }
}

resource "aws_subnet" "public" {
  count = 2
  vpc_id = aws_vpc.main.id
  cidr_block = "10.0.${count.index + 10}.0/24"
  availability_zone = data.aws_availability_zones.available.names[count.index]
  
  tags = {
    Name = "weather-warehouse-public-${count.index + 1}"
  }
}
```

#### RDS Database
```hcl
resource "aws_db_instance" "weather_db" {
  identifier = "weather-warehouse-db"
  engine = "postgres"
  engine_version = "15.4"
  instance_class = "db.t3.micro"
  allocated_storage = 20
  storage_type = "gp2"
  
  db_name = "weather_db"
  username = "weather_user"
  password = aws_secretsmanager_secret_version.db_password.secret_string
  
  vpc_security_group_ids = [aws_security_group.rds.id]
  db_subnet_group_name = aws_db_subnet_group.main.name
  
  backup_retention_period = 7
  backup_window = "03:00-04:00"
  maintenance_window = "sun:04:00-sun:05:00"
  
  deletion_protection = true
  skip_final_snapshot = false
  
  tags = {
    Name = "weather-warehouse-database"
  }
}
```

### 2. Container Configuration

#### ECS Task Definition
```json
{
  "family": "weather-warehouse-api",
  "networkMode": "awsvpc",
  "requiresCompatibilities": ["FARGATE"],
  "cpu": "512",
  "memory": "1024",
  "executionRoleArn": "arn:aws:iam::123456789012:role/ecsTaskExecutionRole",
  "taskRoleArn": "arn:aws:iam::123456789012:role/weather-warehouse-task-role",
  "containerDefinitions": [
    {
      "name": "weather-api",
      "image": "123456789012.dkr.ecr.us-east-1.amazonaws.com/weather-warehouse:latest",
      "portMappings": [
        {
          "containerPort": 5000,
          "protocol": "tcp"
        }
      ],
      "environment": [
        {
          "name": "FLASK_ENV",
          "value": "production"
        }
      ],
      "secrets": [
        {
          "name": "DATABASE_URL",
          "valueFrom": "arn:aws:secretsmanager:us-east-1:123456789012:secret:weather-warehouse/db-url"
        }
      ],
      "logConfiguration": {
        "logDriver": "awslogs",
        "options": {
          "awslogs-group": "/ecs/weather-warehouse",
          "awslogs-region": "us-east-1",
          "awslogs-stream-prefix": "ecs"
        }
      },
      "healthCheck": {
        "command": ["CMD-SHELL", "curl -f http://localhost:5000/api/health || exit 1"],
        "interval": 30,
        "timeout": 5,
        "retries": 3,
        "startPeriod": 60
      }
    }
  ]
}
```

#### ECS Service
```json
{
  "cluster": "weather-warehouse-cluster",
  "serviceName": "weather-warehouse-api",
  "taskDefinition": "weather-warehouse-api",
  "desiredCount": 2,
  "launchType": "FARGATE",
  "networkConfiguration": {
    "awsvpcConfiguration": {
      "subnets": ["subnet-12345678", "subnet-87654321"],
      "securityGroups": ["sg-12345678"],
      "assignPublicIp": "DISABLED"
    }
  },
  "loadBalancers": [
    {
      "targetGroupArn": "arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/weather-api/1234567890123456",
      "containerName": "weather-api",
      "containerPort": 5000
    }
  ],
  "deploymentConfiguration": {
    "maximumPercent": 200,
    "minimumHealthyPercent": 50,
    "deploymentCircuitBreaker": {
      "enable": true,
      "rollback": true
    }
  }
}
```

### 3. Data Ingestion Pipeline

#### EventBridge Rule for Scheduling
```json
{
  "Name": "weather-data-ingestion-daily",
  "ScheduleExpression": "cron(0 2 * * ? *)",
  "State": "ENABLED",
  "Targets": [
    {
      "Id": "weather-ingestion-lambda",
      "Arn": "arn:aws:lambda:us-east-1:123456789012:function:weather-ingestion",
      "Input": "{\"source\": \"scheduled\", \"ingest_run_id\": \"daily-$(date +%Y%m%d)\"}"
    }
  ]
}
```

#### Lambda Function for Data Ingestion
```python
import boto3
import json
import os
from datetime import datetime
import psycopg2
from sqlalchemy import create_engine
from src.ingest import ingest_weather_data

def lambda_handler(event, context):
    """Lambda function to ingest weather data from S3"""
    
    # Get database connection from Secrets Manager
    secrets_client = boto3.client('secretsmanager')
    db_secret = secrets_client.get_secret_value(
        SecretId='weather-warehouse/db-url'
    )
    database_url = json.loads(db_secret['SecretString'])['url']
    
    # Set environment variable for the ingestion script
    os.environ['DATABASE_URL'] = database_url
    
    try:
        # Download data files from S3 to /tmp
        s3_client = boto3.client('s3')
        bucket_name = 'weather-warehouse-data'
        
        # Download weather data files
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix='wx_data/'
        )
        
        for obj in response.get('Contents', []):
            if obj['Key'].endswith('.txt'):
                local_path = f"/tmp/{os.path.basename(obj['Key'])}"
                s3_client.download_file(bucket_name, obj['Key'], local_path)
        
        # Create wx_data directory structure
        os.makedirs('/tmp/wx_data', exist_ok=True)
        for file in os.listdir('/tmp'):
            if file.endswith('.txt'):
                os.rename(f"/tmp/{file}", f"/tmp/wx_data/{file}")
        
        # Run ingestion
        ingest_run_id = event.get('ingest_run_id', f"lambda-{datetime.now().strftime('%Y%m%d-%H%M%S')}")
        records_ingested = ingest_weather_data(
            wx_data_dir='/tmp/wx_data',
            source='scheduled',
            ingest_run_id=ingest_run_id
        )
        
        # Send metrics to CloudWatch
        cloudwatch = boto3.client('cloudwatch')
        cloudwatch.put_metric_data(
            Namespace='WeatherWarehouse',
            MetricData=[
                {
                    'MetricName': 'RecordsIngested',
                    'Value': records_ingested,
                    'Unit': 'Count',
                    'Timestamp': datetime.now()
                }
            ]
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Ingestion completed successfully',
                'records_ingested': records_ingested,
                'ingest_run_id': ingest_run_id
            })
        }
        
    except Exception as e:
        # Send error to CloudWatch
        cloudwatch = boto3.client('cloudwatch')
        cloudwatch.put_metric_data(
            Namespace='WeatherWarehouse',
            MetricData=[
                {
                    'MetricName': 'IngestionErrors',
                    'Value': 1,
                    'Unit': 'Count',
                    'Timestamp': datetime.now()
                }
            ]
        )
        
        raise e
```

### 4. Monitoring and Alerting

#### CloudWatch Alarms
```json
{
  "AlarmName": "weather-api-high-error-rate",
  "AlarmDescription": "High error rate in weather API",
  "MetricName": "HTTPCode_Target_5XX_Count",
  "Namespace": "AWS/ApplicationELB",
  "Statistic": "Sum",
  "Period": 300,
  "EvaluationPeriods": 2,
  "Threshold": 10,
  "ComparisonOperator": "GreaterThanThreshold",
  "AlarmActions": ["arn:aws:sns:us-east-1:123456789012:weather-alerts"]
}
```

#### CloudWatch Dashboard
```json
{
  "DashboardName": "WeatherWarehouse",
  "DashboardBody": {
    "widgets": [
      {
        "type": "metric",
        "properties": {
          "metrics": [
            ["AWS/ApplicationELB", "RequestCount", "LoadBalancer", "weather-api-alb"],
            [".", "TargetResponseTime", ".", "."],
            [".", "HTTPCode_Target_2XX_Count", ".", "."],
            [".", "HTTPCode_Target_5XX_Count", ".", "."]
          ],
          "period": 300,
          "stat": "Sum",
          "region": "us-east-1",
          "title": "API Performance"
        }
      },
      {
        "type": "metric",
        "properties": {
          "metrics": [
            ["WeatherWarehouse", "RecordsIngested"],
            [".", "IngestionErrors"]
          ],
          "period": 3600,
          "stat": "Sum",
          "region": "us-east-1",
          "title": "Data Ingestion"
        }
      }
    ]
  }
}
```

### 5. Security Configuration

#### IAM Roles and Policies
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "secretsmanager:GetSecretValue"
      ],
      "Resource": "arn:aws:secretsmanager:us-east-1:123456789012:secret:weather-warehouse/*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "arn:aws:logs:us-east-1:123456789012:log-group:/ecs/weather-warehouse:*"
    }
  ]
}
```

#### Security Groups
```hcl
resource "aws_security_group" "alb" {
  name = "weather-warehouse-alb"
  vpc_id = aws_vpc.main.id
  
  ingress {
    from_port = 80
    to_port = 80
    protocol = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
  
  ingress {
    from_port = 443
    to_port = 443
    protocol = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
  
  egress {
    from_port = 0
    to_port = 0
    protocol = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group" "ecs" {
  name = "weather-warehouse-ecs"
  vpc_id = aws_vpc.main.id
  
  ingress {
    from_port = 5000
    to_port = 5000
    protocol = "tcp"
    security_groups = [aws_security_group.alb.id]
  }
  
  egress {
    from_port = 0
    to_port = 0
    protocol = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}
```

## Deployment Process

### 1. Build and Push Container
```bash
# Build Docker image
docker build -t weather-warehouse .

# Tag for ECR
docker tag weather-warehouse:latest 123456789012.dkr.ecr.us-east-1.amazonaws.com/weather-warehouse:latest

# Push to ECR
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 123456789012.dkr.ecr.us-east-1.amazonaws.com
docker push 123456789012.dkr.ecr.us-east-1.amazonaws.com/weather-warehouse:latest
```

### 2. Deploy Infrastructure
```bash
# Using Terraform
terraform init
terraform plan
terraform apply

# Or using AWS CLI
aws cloudformation create-stack \
  --stack-name weather-warehouse \
  --template-body file://infrastructure.yaml \
  --capabilities CAPABILITY_IAM
```

### 3. Deploy Application
```bash
# Update ECS service
aws ecs update-service \
  --cluster weather-warehouse-cluster \
  --service weather-warehouse-api \
  --force-new-deployment
```

## Cost Optimization

### Resource Sizing
- **RDS**: Start with db.t3.micro, scale based on usage
- **ECS**: Use Fargate Spot for non-critical workloads
- **Lambda**: Pay per execution, ideal for scheduled tasks

### Monitoring Costs
- Set up CloudWatch billing alarms
- Use S3 lifecycle policies for data retention
- Monitor unused resources with AWS Cost Explorer

## Disaster Recovery

### Backup Strategy
- **RDS**: Automated backups with 7-day retention
- **S3**: Cross-region replication for critical data
- **ECS**: Blue-green deployments for zero-downtime updates

### Recovery Procedures
1. **Database**: Restore from RDS snapshot
2. **Application**: Redeploy from ECR image
3. **Data**: Re-run ingestion from S3 backup

## Scaling Considerations

### Horizontal Scaling
- ECS Auto Scaling based on CPU/memory usage
- RDS read replicas for read-heavy workloads
- Application Load Balancer for traffic distribution

### Vertical Scaling
- Upgrade RDS instance class
- Increase ECS task CPU/memory allocation
- Add ElastiCache for caching frequently accessed data

## Security Best Practices

1. **Network Security**: Use private subnets for ECS tasks
2. **Secrets Management**: Store credentials in AWS Secrets Manager
3. **IAM**: Principle of least privilege for all roles
4. **Encryption**: Enable encryption at rest and in transit
5. **Monitoring**: Set up CloudTrail for API call logging

## Maintenance

### Regular Tasks
- **Security Updates**: Patch container images monthly
- **Database Maintenance**: RDS automated maintenance windows
- **Monitoring**: Review CloudWatch metrics weekly
- **Backup Testing**: Test restore procedures quarterly

### Updates and Deployments
- Use blue-green deployments for zero downtime
- Test in staging environment first
- Monitor deployment metrics and rollback if needed
- Update documentation with each release

This production deployment provides a scalable, reliable, and maintainable solution for the Weather Data Warehouse API using AWS managed services. 