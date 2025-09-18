# MySQL to S3 Data Ingestion Script

A comprehensive Python script for ingesting data from MySQL databases into S3 buckets as Parquet files, designed specifically for Jupyter Notebook environments.

## Features

- ✅ **MySQL Database Connectivity**: Uses SQLAlchemy with PyMySQL for robust database connections
- ✅ **S3 Integration**: Boto3-powered uploads with automatic Parquet conversion
- ✅ **Multiple Table Support**: Process multiple tables with individual configurations
- ✅ **Batch Processing**: Configurable batch sizes to manage memory usage
- ✅ **Progress Tracking**: Visual progress bars optimized for Jupyter Notebooks
- ✅ **Error Handling**: Comprehensive logging and error recovery
- ✅ **Flexible Configuration**: JSON-based configuration with environment variable support
- ✅ **S3 Object Structure**: Follows data lake patterns: `landing/source_alias/database_name/table_name/job_date/`

## Installation

### 1. Install Required Dependencies

```bash
pip install -r requirements.txt
```

Or install individual packages:

```bash
pip install pandas sqlalchemy pymysql boto3 pyarrow tqdm numpy python-dateutil
```

### 2. Required Dependencies

- **pandas**: Data manipulation and analysis
- **sqlalchemy**: SQL toolkit and Object-Relational Mapping
- **pymysql**: Pure Python MySQL client
- **boto3**: AWS SDK for Python
- **pyarrow**: Apache Arrow Python bindings for Parquet support
- **tqdm**: Progress bars for notebooks
- **numpy**: Numerical computing support
- **python-dateutil**: Date/time utilities

## Quick Start

### 1. Create Configuration File

```python
from mysql_to_s3_ingestion import create_sample_config

# Create a sample configuration file
create_sample_config("my_config.json")
```

### 2. Edit Configuration

Update `my_config.json` with your actual credentials:

```json
{
  "mysql": {
    "host": "your-mysql-host.com",
    "port": 3306,
    "database": "your_database",
    "username": "your_username",
    "password": "your_password"
  },
  "s3": {
    "bucket": "your-s3-bucket",
    "region": "us-west-2",
    "source_alias": "mysql_prod",
    "access_key_id": "your_access_key",
    "secret_access_key": "your_secret_key"
  },
  "tables": [
    {
      "name": "users",
      "batch_size": 10000
    },
    {
      "name": "orders",
      "batch_size": 5000,
      "job_date": "2024-01-15"
    }
  ],
  "logging": {
    "log_dir": "./logs"
  }
}
```

### 3. Run Ingestion

#### In Jupyter Notebook:

```python
from mysql_to_s3_ingestion import MySQLToS3Ingestion, load_config, display_results_summary

# Load configuration
config = load_config("my_config.json")

# Initialize ingestion engine
ingestion = MySQLToS3Ingestion(config)

# Run ingestion process
results = ingestion.run_ingestion()

# Display results
display_results_summary(results)

# Clean up
ingestion.close_connections()
```

#### Command Line:

```bash
python mysql_to_s3_ingestion.py --config my_config.json
```

## Configuration Reference

### MySQL Configuration

```json
{
  "mysql": {
    "host": "hostname or IP address",
    "port": 3306,
    "database": "database_name",
    "username": "mysql_username", 
    "password": "mysql_password",
    "ssl_ca": "/path/to/ca-cert.pem",     // Optional: SSL CA certificate
    "ssl_cert": "/path/to/client-cert.pem", // Optional: SSL client certificate
    "ssl_key": "/path/to/client-key.pem"    // Optional: SSL client key
  }
}
```

### S3 Configuration

```json
{
  "s3": {
    "bucket": "your-s3-bucket-name",
    "region": "us-west-2",
    "source_alias": "mysql_prod",           // Used in S3 object path
    "access_key_id": "AWS_ACCESS_KEY_ID",   // Optional: can use IAM roles
    "secret_access_key": "AWS_SECRET_KEY"   // Optional: can use IAM roles
  }
}
```

### Table Configuration

```json
{
  "tables": [
    {
      "name": "table_name",
      "batch_size": 10000,              // Optional: rows per batch (default: 10000)
      "job_date": "2024-01-15"          // Optional: custom date for partitioning
    }
  ]
}
```

### Logging Configuration

```json
{
  "logging": {
    "log_dir": "./logs"                 // Directory for log files
  }
}
```

## S3 Object Structure

Files are uploaded to S3 following this structure:

```
s3://your-bucket/landing/source_alias/database_name/table_name/job_date/table_name.parquet
```

Example:
```
s3://data-lake/landing/mysql_prod/ecommerce/users/2024-01-15/users.parquet
s3://data-lake/landing/mysql_prod/ecommerce/orders/2024-01-15/orders.parquet
```

## Usage Examples

### Example 1: Basic Usage

```python
from mysql_to_s3_ingestion import MySQLToS3Ingestion

config = {
    "mysql": {
        "host": "localhost",
        "database": "ecommerce",
        "username": "user",
        "password": "pass"
    },
    "s3": {
        "bucket": "my-data-lake",
        "region": "us-east-1"
    },
    "tables": [
        {"name": "products", "batch_size": 5000},
        {"name": "customers", "batch_size": 10000}
    ]
}

ingestion = MySQLToS3Ingestion(config)
results = ingestion.run_ingestion()
```

### Example 2: Single Table Processing

```python
# Process just one table
table_config = {
    "name": "large_table",
    "batch_size": 1000,
    "job_date": "2024-01-20"
}

success = ingestion.process_table(table_config)
```

### Example 3: Using Environment Variables

```bash
export MYSQL_HOST="mysql.example.com"
export MYSQL_USERNAME="myuser"
export MYSQL_PASSWORD="mypassword"
export AWS_ACCESS_KEY_ID="AKIAIOSFODNN7EXAMPLE"
export AWS_SECRET_ACCESS_KEY="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
```

```python
import os

config = {
    "mysql": {
        "host": os.getenv("MYSQL_HOST"),
        "username": os.getenv("MYSQL_USERNAME"),
        "password": os.getenv("MYSQL_PASSWORD"),
        "database": "mydb"
    },
    "s3": {
        "bucket": "my-bucket",
        "region": "us-west-2"
        # access_key_id and secret_access_key will be read from environment
    },
    "tables": [{"name": "users"}]
}
```

## Advanced Features

### Custom Batch Processing

```python
# Large table with small batches
large_table_config = {
    "name": "transactions",
    "batch_size": 1000  # Smaller batches for memory management
}

# Small table with large batches  
small_table_config = {
    "name": "categories", 
    "batch_size": 50000  # Larger batches for efficiency
}
```

### SSL Connection

```json
{
  "mysql": {
    "host": "secure-mysql.example.com",
    "database": "mydb",
    "username": "user",
    "password": "pass",
    "ssl_ca": "/path/to/ca-cert.pem",
    "ssl_cert": "/path/to/client-cert.pem",
    "ssl_key": "/path/to/client-key.pem"
  }
}
```

### Custom Date Partitioning

```json
{
  "tables": [
    {
      "name": "daily_sales",
      "job_date": "2024-01-15"  // Custom date for this table
    },
    {
      "name": "monthly_reports", 
      "job_date": "2024-01-01"  // Different date for this table
    }
  ]
}
```

## Troubleshooting

### Common Issues

#### 1. MySQL Connection Errors

```
Error: Failed to connect to MySQL: (2003, "Can't connect to MySQL server")
```

**Solutions:**
- Verify host, port, username, and password
- Check firewall settings
- Ensure MySQL service is running
- Verify network connectivity

#### 2. AWS Credentials Error

```
Error: AWS credentials not found
```

**Solutions:**
- Set AWS credentials in configuration file
- Use environment variables: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
- Configure AWS CLI: `aws configure`
- Use IAM roles for EC2 instances

#### 3. S3 Permission Errors

```
Error: Access Denied (S3 bucket)
```

**Solutions:**
- Verify S3 bucket name and region
- Check IAM permissions for S3 access
- Ensure bucket exists and is accessible

#### 4. Memory Issues

```
MemoryError: Unable to allocate array
```

**Solutions:**
- Reduce `batch_size` in table configuration
- Process tables individually
- Increase available system memory

### Performance Optimization

1. **Batch Size Tuning:**
   - Start with 10,000 rows per batch
   - Reduce for large tables or limited memory
   - Increase for small tables and high memory

2. **Network Optimization:**
   - Use regions close to your data sources
   - Consider VPC endpoints for S3 access
   - Optimize MySQL connection pooling

3. **Parallel Processing:**
   - Process different tables simultaneously
   - Use multiple notebook kernels for large datasets

### Monitoring and Logging

- Log files are created in the specified `log_dir`
- Each run creates a timestamped log file
- Console output shows real-time progress
- Use log level DEBUG for detailed troubleshooting

## Security Best Practices

1. **Credential Management:**
   - Use environment variables for sensitive data
   - Consider AWS IAM roles instead of access keys
   - Rotate credentials regularly

2. **Network Security:**
   - Use SSL/TLS for MySQL connections
   - Configure VPC security groups appropriately
   - Use private subnets when possible

3. **Data Protection:**
   - Enable S3 encryption at rest
   - Use S3 bucket policies for access control
   - Consider S3 versioning for data recovery

## Contributing

This script is part of the hydaai/winutils repository. To contribute:

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is licensed under the Apache License 2.0 - see the LICENSE file for details.

## Support

For issues and questions:
- Check the troubleshooting section above
- Review log files for detailed error messages
- Open an issue in the GitHub repository
- Verify configuration parameters

## Changelog

### Version 1.0.0
- Initial release
- MySQL to S3 ingestion functionality
- Jupyter Notebook support
- Comprehensive error handling and logging
- Configurable batch processing
- Progress tracking with visual indicators