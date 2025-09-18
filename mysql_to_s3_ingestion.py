#!/usr/bin/env python3
"""
MySQL to S3 Data Ingestion Script for Jupyter Notebooks

This script provides functionality to ingest data from MySQL databases into S3 buckets
as Parquet files. It's designed to be run interactively within Jupyter Notebooks.

Key Features:
- MySQL database connectivity using SQLAlchemy
- Multiple table support with configurable batch processing
- S3 integration for Parquet file storage
- Comprehensive logging and error handling
- JSON-based configuration management
- Jupyter Notebook-friendly output and progress tracking

Author: Generated for hydaai/winutils repository
License: Apache 2.0 (same as repository)
"""

import json
import logging
import os
import sys
from datetime import datetime
from typing import Dict, List, Optional, Any
import warnings

# Suppress pandas future warnings for cleaner notebook output
warnings.filterwarnings('ignore', category=FutureWarning)

try:
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import create_engine, text
    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError
    import pyarrow as pa
    import pyarrow.parquet as pq
    from tqdm.notebook import tqdm
except ImportError as e:
    print(f"❌ Missing required dependency: {e}")
    print("\nPlease install the required packages:")
    print("pip install pandas sqlalchemy pymysql boto3 pyarrow tqdm")
    sys.exit(1)


class MySQLToS3Ingestion:
    """
    Main class for handling MySQL to S3 data ingestion.
    
    This class provides methods to:
    - Connect to MySQL databases
    - Extract data from multiple tables
    - Convert data to Parquet format
    - Upload files to S3 buckets
    - Track progress and handle errors
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the ingestion engine with configuration.
        
        Args:
            config: Dictionary containing source and destination configurations
        """
        self.config = config
        self.logger = self._setup_logging()
        self.mysql_engine = None
        self.s3_client = None
        
        # Validate configuration
        self._validate_config()
        
        # Initialize connections
        self._setup_mysql_connection()
        self._setup_s3_client()
    
    def _setup_logging(self) -> logging.Logger:
        """
        Set up logging configuration for the ingestion process.
        
        Returns:
            Configured logger instance
        """
        logger = logging.getLogger('mysql_s3_ingestion')
        logger.setLevel(logging.INFO)
        
        # Remove existing handlers to avoid duplicate logs
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        # Console handler for Jupyter notebook output
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        # File handler for persistent logging
        log_dir = self.config.get('logging', {}).get('log_dir', './logs')
        os.makedirs(log_dir, exist_ok=True)
        
        log_file = os.path.join(log_dir, f"ingestion_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        
        return logger
    
    def _validate_config(self):
        """
        Validate the configuration parameters.
        
        Raises:
            ValueError: If required configuration parameters are missing
        """
        required_sections = ['mysql', 's3', 'tables']
        for section in required_sections:
            if section not in self.config:
                raise ValueError(f"Missing required configuration section: {section}")
        
        # Validate MySQL configuration
        mysql_required = ['host', 'database', 'username', 'password']
        for param in mysql_required:
            if param not in self.config['mysql']:
                raise ValueError(f"Missing required MySQL parameter: {param}")
        
        # Validate S3 configuration
        s3_required = ['bucket', 'region']
        for param in s3_required:
            if param not in self.config['s3']:
                raise ValueError(f"Missing required S3 parameter: {param}")
        
        # Validate tables configuration
        if not isinstance(self.config['tables'], list) or len(self.config['tables']) == 0:
            raise ValueError("Tables configuration must be a non-empty list")
        
        self.logger.info("✅ Configuration validation passed")
    
    def _setup_mysql_connection(self):
        """
        Establish connection to MySQL database using SQLAlchemy.
        """
        try:
            mysql_config = self.config['mysql']
            
            # Build connection string
            connection_string = (
                f"mysql+pymysql://{mysql_config['username']}:{mysql_config['password']}"
                f"@{mysql_config['host']}:{mysql_config.get('port', 3306)}"
                f"/{mysql_config['database']}"
            )
            
            # Add SSL and other connection parameters if specified
            connect_args = {}
            if mysql_config.get('ssl_ca'):
                connect_args['ssl_ca'] = mysql_config['ssl_ca']
            if mysql_config.get('ssl_cert'):
                connect_args['ssl_cert'] = mysql_config['ssl_cert']
            if mysql_config.get('ssl_key'):
                connect_args['ssl_key'] = mysql_config['ssl_key']
            
            # Create engine
            self.mysql_engine = create_engine(
                connection_string,
                connect_args=connect_args,
                pool_pre_ping=True,
                pool_recycle=3600
            )
            
            # Test connection
            with self.mysql_engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            self.logger.info(f"✅ Successfully connected to MySQL database: {mysql_config['database']}")
            
        except Exception as e:
            self.logger.error(f"❌ Failed to connect to MySQL: {str(e)}")
            raise
    
    def _setup_s3_client(self):
        """
        Set up AWS S3 client for file uploads.
        """
        try:
            s3_config = self.config['s3']
            
            # Initialize S3 client
            session = boto3.Session(
                aws_access_key_id=s3_config.get('access_key_id'),
                aws_secret_access_key=s3_config.get('secret_access_key'),
                region_name=s3_config['region']
            )
            
            self.s3_client = session.client('s3')
            
            # Test S3 access by checking if bucket exists
            try:
                self.s3_client.head_bucket(Bucket=s3_config['bucket'])
                self.logger.info(f"✅ Successfully connected to S3 bucket: {s3_config['bucket']}")
            except ClientError as e:
                error_code = int(e.response['Error']['Code'])
                if error_code == 404:
                    self.logger.warning(f"⚠️ S3 bucket '{s3_config['bucket']}' not found, but connection is valid")
                else:
                    raise
                    
        except NoCredentialsError:
            self.logger.error("❌ AWS credentials not found. Please configure AWS credentials.")
            raise
        except Exception as e:
            self.logger.error(f"❌ Failed to setup S3 client: {str(e)}")
            raise
    
    def _get_table_row_count(self, table_name: str) -> int:
        """
        Get the total number of rows in a table for progress tracking.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Total number of rows in the table
        """
        try:
            with self.mysql_engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                return result.scalar()
        except Exception as e:
            self.logger.warning(f"Could not get row count for {table_name}: {str(e)}")
            return 0
    
    def _extract_table_data(self, table_name: str, batch_size: int = 10000) -> pd.DataFrame:
        """
        Extract data from a MySQL table in batches.
        
        Args:
            table_name: Name of the table to extract
            batch_size: Number of rows to process in each batch
            
        Returns:
            Complete DataFrame containing all table data
        """
        self.logger.info(f"🔄 Starting extraction from table: {table_name}")
        
        try:
            # Get total row count for progress tracking
            total_rows = self._get_table_row_count(table_name)
            self.logger.info(f"📊 Total rows to extract: {total_rows:,}")
            
            if total_rows == 0:
                self.logger.warning(f"⚠️ Table {table_name} is empty")
                return pd.DataFrame()
            
            # Extract data in batches
            all_data = []
            offset = 0
            
            # Create progress bar for Jupyter notebook
            pbar = tqdm(total=total_rows, desc=f"Extracting {table_name}", unit="rows")
            
            while True:
                query = f"SELECT * FROM {table_name} LIMIT {batch_size} OFFSET {offset}"
                
                batch_df = pd.read_sql(query, self.mysql_engine)
                
                if batch_df.empty:
                    break
                
                all_data.append(batch_df)
                offset += len(batch_df)
                pbar.update(len(batch_df))
                
                self.logger.debug(f"Extracted batch: {offset:,} / {total_rows:,} rows")
            
            pbar.close()
            
            if all_data:
                final_df = pd.concat(all_data, ignore_index=True)
                self.logger.info(f"✅ Successfully extracted {len(final_df):,} rows from {table_name}")
                return final_df
            else:
                self.logger.warning(f"⚠️ No data extracted from {table_name}")
                return pd.DataFrame()
                
        except Exception as e:
            self.logger.error(f"❌ Failed to extract data from {table_name}: {str(e)}")
            raise
    
    def _upload_to_s3(self, df: pd.DataFrame, s3_key: str) -> bool:
        """
        Convert DataFrame to Parquet and upload to S3.
        
        Args:
            df: DataFrame to upload
            s3_key: S3 object key for the file
            
        Returns:
            True if upload successful, False otherwise
        """
        try:
            s3_config = self.config['s3']
            bucket = s3_config['bucket']
            
            # Convert DataFrame to Parquet bytes
            table = pa.Table.from_pandas(df)
            
            # Use BytesIO buffer to avoid creating temporary files
            from io import BytesIO
            buffer = BytesIO()
            pq.write_table(table, buffer)
            buffer.seek(0)
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket=bucket,
                Key=s3_key,
                Body=buffer.getvalue(),
                ContentType='application/octet-stream'
            )
            
            self.logger.info(f"✅ Successfully uploaded to S3: s3://{bucket}/{s3_key}")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Failed to upload to S3: {str(e)}")
            return False
    
    def _generate_s3_key(self, table_name: str, job_date: str = None) -> str:
        """
        Generate S3 object key following the specified structure.
        
        Args:
            table_name: Name of the source table
            job_date: Date string for partitioning (defaults to current date)
            
        Returns:
            Formatted S3 object key
        """
        if job_date is None:
            job_date = datetime.now().strftime('%Y-%m-%d')
        
        s3_config = self.config['s3']
        mysql_config = self.config['mysql']
        
        source_alias = s3_config.get('source_alias', 'mysql_source')
        database_name = mysql_config['database']
        
        s3_key = f"landing/{source_alias}/{database_name}/{table_name}/{job_date}/{table_name}.parquet"
        
        return s3_key
    
    def process_table(self, table_config: Dict[str, Any]) -> bool:
        """
        Process a single table: extract data and upload to S3.
        
        Args:
            table_config: Configuration for the specific table
            
        Returns:
            True if processing successful, False otherwise
        """
        table_name = table_config['name']
        batch_size = table_config.get('batch_size', 10000)
        
        try:
            self.logger.info(f"🚀 Processing table: {table_name}")
            
            # Extract data from MySQL
            df = self._extract_table_data(table_name, batch_size)
            
            if df.empty:
                self.logger.warning(f"⚠️ Skipping {table_name} - no data to process")
                return True
            
            # Generate S3 key
            job_date = table_config.get('job_date')
            s3_key = self._generate_s3_key(table_name, job_date)
            
            # Upload to S3
            success = self._upload_to_s3(df, s3_key)
            
            if success:
                self.logger.info(f"✅ Successfully processed table: {table_name}")
                return True
            else:
                self.logger.error(f"❌ Failed to process table: {table_name}")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Error processing table {table_name}: {str(e)}")
            return False
    
    def run_ingestion(self) -> Dict[str, bool]:
        """
        Run the complete data ingestion process for all configured tables.
        
        Returns:
            Dictionary mapping table names to their processing status
        """
        self.logger.info("🚀 Starting MySQL to S3 data ingestion process")
        
        results = {}
        total_tables = len(self.config['tables'])
        
        # Create overall progress bar
        table_pbar = tqdm(total=total_tables, desc="Processing tables", unit="table")
        
        for i, table_config in enumerate(self.config['tables'], 1):
            table_name = table_config['name']
            
            self.logger.info(f"📋 Processing table {i}/{total_tables}: {table_name}")
            
            try:
                success = self.process_table(table_config)
                results[table_name] = success
                
                if success:
                    self.logger.info(f"✅ Table {table_name} processed successfully")
                else:
                    self.logger.error(f"❌ Table {table_name} processing failed")
                    
            except Exception as e:
                self.logger.error(f"❌ Unexpected error processing {table_name}: {str(e)}")
                results[table_name] = False
            
            table_pbar.update(1)
        
        table_pbar.close()
        
        # Summary
        successful_tables = sum(1 for success in results.values() if success)
        failed_tables = total_tables - successful_tables
        
        self.logger.info(f"📊 Ingestion Summary:")
        self.logger.info(f"   ✅ Successful: {successful_tables}/{total_tables}")
        self.logger.info(f"   ❌ Failed: {failed_tables}/{total_tables}")
        
        if failed_tables == 0:
            self.logger.info("🎉 All tables processed successfully!")
        else:
            self.logger.warning(f"⚠️ {failed_tables} table(s) failed to process")
        
        return results
    
    def close_connections(self):
        """Clean up database connections."""
        if self.mysql_engine:
            self.mysql_engine.dispose()
            self.logger.info("🔌 MySQL connection closed")


def load_config(config_path: str) -> Dict[str, Any]:
    """
    Load configuration from JSON file.
    
    Args:
        config_path: Path to the JSON configuration file
        
    Returns:
        Configuration dictionary
    """
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        return config
    except FileNotFoundError:
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in configuration file: {e}")


def create_sample_config(config_path: str = "ingestion_config.json"):
    """
    Create a sample configuration file for reference.
    
    Args:
        config_path: Path where the sample config should be created
    """
    sample_config = {
        "mysql": {
            "host": "your-mysql-host.com",
            "port": 3306,
            "database": "your_database",
            "username": "your_username",
            "password": "your_password",
            "ssl_ca": "/path/to/ca-cert.pem",
            "ssl_cert": "/path/to/client-cert.pem",
            "ssl_key": "/path/to/client-key.pem"
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
                "batch_size": 10000,
                "job_date": "2024-01-15"
            },
            {
                "name": "orders",
                "batch_size": 5000
            },
            {
                "name": "products",
                "batch_size": 1000
            }
        ],
        "logging": {
            "log_dir": "./logs"
        }
    }
    
    with open(config_path, 'w') as f:
        json.dump(sample_config, f, indent=2)
    
    print(f"✅ Sample configuration created: {config_path}")
    print("Please update the configuration with your actual database and S3 credentials.")


# Jupyter Notebook Helper Functions
def display_results_summary(results: Dict[str, bool]):
    """
    Display a formatted summary of ingestion results for Jupyter notebooks.
    
    Args:
        results: Dictionary mapping table names to their processing status
    """
    print("\n" + "="*60)
    print("📊 INGESTION RESULTS SUMMARY")
    print("="*60)
    
    successful = []
    failed = []
    
    for table_name, success in results.items():
        if success:
            successful.append(table_name)
            print(f"✅ {table_name}")
        else:
            failed.append(table_name)
            print(f"❌ {table_name}")
    
    print("\n" + "-"*60)
    print(f"📈 Total Tables: {len(results)}")
    print(f"✅ Successful: {len(successful)}")
    print(f"❌ Failed: {len(failed)}")
    
    if failed:
        print(f"\n⚠️ Failed Tables: {', '.join(failed)}")
    
    print("="*60)


def main():
    """
    Main function for command-line usage.
    This function is primarily for demonstration and testing.
    In Jupyter notebooks, use the MySQLToS3Ingestion class directly.
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='MySQL to S3 Data Ingestion')
    parser.add_argument('--config', '-c', required=True, help='Path to configuration JSON file')
    parser.add_argument('--create-sample-config', action='store_true', 
                       help='Create a sample configuration file')
    
    args = parser.parse_args()
    
    if args.create_sample_config:
        create_sample_config()
        return
    
    # Load configuration and run ingestion
    try:
        config = load_config(args.config)
        
        # Initialize and run ingestion
        ingestion = MySQLToS3Ingestion(config)
        results = ingestion.run_ingestion()
        
        # Display results
        display_results_summary(results)
        
        # Clean up
        ingestion.close_connections()
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()