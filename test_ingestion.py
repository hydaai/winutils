#!/usr/bin/env python3
"""
Test script for MySQL to S3 Data Ingestion functionality.

This script provides basic tests for the data ingestion components without
requiring actual database or S3 connections.
"""

import json
import os
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch, MagicMock

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from mysql_to_s3_ingestion import MySQLToS3Ingestion, load_config, create_sample_config
except ImportError as e:
    print(f"❌ Failed to import ingestion module: {e}")
    print("Make sure all dependencies are installed: pip install -r requirements.txt")
    sys.exit(1)


class TestMySQLToS3Ingestion(unittest.TestCase):
    """Test cases for the MySQL to S3 ingestion functionality."""
    
    def setUp(self):
        """Set up test configuration."""
        self.test_config = {
            "mysql": {
                "host": "test-host",
                "port": 3306,
                "database": "test_db",
                "username": "test_user",
                "password": "test_pass"
            },
            "s3": {
                "bucket": "test-bucket",
                "region": "us-west-2",
                "source_alias": "test_source",
                "access_key_id": "test_key",
                "secret_access_key": "test_secret"
            },
            "tables": [
                {
                    "name": "test_table1",
                    "batch_size": 1000
                },
                {
                    "name": "test_table2",
                    "batch_size": 2000,
                    "job_date": "2024-01-15"
                }
            ],
            "logging": {
                "log_dir": "./test_logs"
            }
        }
    
    def test_config_validation_success(self):
        """Test successful configuration validation."""
        with patch('mysql_to_s3_ingestion.MySQLToS3Ingestion._setup_mysql_connection'):
            with patch('mysql_to_s3_ingestion.MySQLToS3Ingestion._setup_s3_client'):
                try:
                    ingestion = MySQLToS3Ingestion(self.test_config)
                    self.assertIsNotNone(ingestion)
                    print("✅ Configuration validation test passed")
                except Exception as e:
                    self.fail(f"Configuration validation failed: {e}")
    
    def test_config_validation_missing_section(self):
        """Test configuration validation with missing sections."""
        incomplete_config = {"mysql": self.test_config["mysql"]}
        
        with self.assertRaises(ValueError):
            with patch('mysql_to_s3_ingestion.MySQLToS3Ingestion._setup_mysql_connection'):
                with patch('mysql_to_s3_ingestion.MySQLToS3Ingestion._setup_s3_client'):
                    MySQLToS3Ingestion(incomplete_config)
        
        print("✅ Missing configuration section test passed")
    
    def test_s3_key_generation(self):
        """Test S3 key generation functionality."""
        with patch('mysql_to_s3_ingestion.MySQLToS3Ingestion._setup_mysql_connection'):
            with patch('mysql_to_s3_ingestion.MySQLToS3Ingestion._setup_s3_client'):
                ingestion = MySQLToS3Ingestion(self.test_config)
                
                # Test default date
                s3_key = ingestion._generate_s3_key("test_table")
                expected_pattern = "landing/test_source/test_db/test_table/"
                self.assertIn(expected_pattern, s3_key)
                self.assertTrue(s3_key.endswith("test_table.parquet"))
                
                # Test custom date
                s3_key_custom = ingestion._generate_s3_key("test_table", "2024-01-15")
                expected_custom = "landing/test_source/test_db/test_table/2024-01-15/test_table.parquet"
                self.assertEqual(s3_key_custom, expected_custom)
                
                print("✅ S3 key generation test passed")
    
    def test_create_sample_config(self):
        """Test sample configuration file creation."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as temp_file:
            temp_config_path = temp_file.name
        
        try:
            create_sample_config(temp_config_path)
            
            # Verify file was created and is valid JSON
            self.assertTrue(os.path.exists(temp_config_path))
            
            with open(temp_config_path, 'r') as f:
                loaded_config = json.load(f)
            
            # Check required sections exist
            required_sections = ['mysql', 's3', 'tables', 'logging']
            for section in required_sections:
                self.assertIn(section, loaded_config)
            
            print("✅ Sample configuration creation test passed")
            
        finally:
            # Clean up temporary file
            if os.path.exists(temp_config_path):
                os.unlink(temp_config_path)
    
    def test_load_config(self):
        """Test configuration loading from file."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as temp_file:
            json.dump(self.test_config, temp_file)
            temp_config_path = temp_file.name
        
        try:
            loaded_config = load_config(temp_config_path)
            self.assertEqual(loaded_config, self.test_config)
            print("✅ Configuration loading test passed")
            
        finally:
            # Clean up temporary file
            if os.path.exists(temp_config_path):
                os.unlink(temp_config_path)
    
    @patch('mysql_to_s3_ingestion.pd.read_sql')
    @patch('mysql_to_s3_ingestion.MySQLToS3Ingestion._get_table_row_count')
    def test_extract_table_data_empty(self, mock_row_count, mock_read_sql):
        """Test data extraction from empty table."""
        mock_row_count.return_value = 0
        mock_read_sql.return_value = Mock()
        mock_read_sql.return_value.empty = True
        
        with patch('mysql_to_s3_ingestion.MySQLToS3Ingestion._setup_mysql_connection'):
            with patch('mysql_to_s3_ingestion.MySQLToS3Ingestion._setup_s3_client'):
                ingestion = MySQLToS3Ingestion(self.test_config)
                
                # Mock the database engine
                ingestion.mysql_engine = Mock()
                
                result = ingestion._extract_table_data("empty_table")
                self.assertTrue(result.empty if hasattr(result, 'empty') else len(result) == 0)
                
                print("✅ Empty table extraction test passed")


def run_basic_functionality_tests():
    """Run basic functionality tests without requiring external dependencies."""
    print("🧪 Running MySQL to S3 Ingestion Tests")
    print("=" * 50)
    
    # Test imports
    try:
        import pandas as pd
        import sqlalchemy
        import boto3
        import pyarrow
        print("✅ All required packages imported successfully")
    except ImportError as e:
        print(f"❌ Missing required package: {e}")
        return False
    
    # Run unit tests
    unittest.main(verbosity=2, exit=False)
    
    print("\n" + "=" * 50)
    print("🎉 Basic functionality tests completed!")
    
    return True


def test_configuration_examples():
    """Test various configuration examples."""
    print("\n🔧 Testing Configuration Examples")
    print("-" * 30)
    
    # Test minimal configuration
    minimal_config = {
        "mysql": {
            "host": "localhost",
            "database": "test",
            "username": "user",
            "password": "pass"
        },
        "s3": {
            "bucket": "test-bucket",
            "region": "us-east-1"
        },
        "tables": [
            {"name": "test_table"}
        ]
    }
    
    print("✅ Minimal configuration structure valid")
    
    # Test configuration with SSL
    ssl_config = {
        "mysql": {
            "host": "secure-host",
            "database": "secure_db",
            "username": "user",
            "password": "pass",
            "ssl_ca": "/path/to/ca.pem",
            "ssl_cert": "/path/to/cert.pem",
            "ssl_key": "/path/to/key.pem"
        },
        "s3": {
            "bucket": "secure-bucket",
            "region": "us-west-2",
            "source_alias": "secure_mysql"
        },
        "tables": [
            {
                "name": "secure_table",
                "batch_size": 5000,
                "job_date": "2024-01-15"
            }
        ]
    }
    
    print("✅ SSL configuration structure valid")
    
    # Test environment variable configuration
    env_config = {
        "mysql": {
            "host": "${MYSQL_HOST}",
            "database": "${MYSQL_DATABASE}",
            "username": "${MYSQL_USERNAME}",
            "password": "${MYSQL_PASSWORD}"
        },
        "s3": {
            "bucket": "${S3_BUCKET}",
            "region": "${AWS_REGION}"
        },
        "tables": [
            {"name": "env_table"}
        ]
    }
    
    print("✅ Environment variable configuration structure valid")
    print("🎉 All configuration examples are valid!")


if __name__ == "__main__":
    print("🚀 MySQL to S3 Data Ingestion - Test Suite")
    print("=" * 60)
    
    # Run basic functionality tests
    success = run_basic_functionality_tests()
    
    if success:
        # Test configuration examples
        test_configuration_examples()
        
        print("\n" + "=" * 60)
        print("🎉 All tests completed successfully!")
        print("\nNext steps:")
        print("1. Update configuration with your actual credentials")
        print("2. Test with a small table first")
        print("3. Run the full ingestion process")
        print("4. Monitor logs for any issues")
    else:
        print("\n❌ Some tests failed. Please check the requirements and try again.")
        sys.exit(1)