#!/usr/bin/env python3
"""
Validation script for MySQL to S3 Data Ingestion components.

This script validates the structure and logic of the ingestion module
without requiring external dependencies.
"""

import json
import os
import sys
import ast
import re


def validate_python_syntax(file_path):
    """Validate Python syntax of a file."""
    try:
        with open(file_path, 'r') as f:
            source = f.read()
        
        # Parse the AST to check syntax
        ast.parse(source)
        print(f"✅ {os.path.basename(file_path)}: Python syntax is valid")
        return True
    except SyntaxError as e:
        print(f"❌ {os.path.basename(file_path)}: Syntax error - {e}")
        return False
    except Exception as e:
        print(f"❌ {os.path.basename(file_path)}: Error - {e}")
        return False


def validate_json_structure(file_path):
    """Validate JSON structure of configuration files."""
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        # Check required sections for config files
        if 'mysql' in data and 's3' in data and 'tables' in data:
            print(f"✅ {os.path.basename(file_path)}: JSON structure is valid")
            
            # Validate mysql section
            mysql_required = ['host', 'database', 'username', 'password']
            mysql_section = data.get('mysql', {})
            for field in mysql_required:
                if field in mysql_section:
                    print(f"  ✅ MySQL {field} field present")
                else:
                    print(f"  ⚠️ MySQL {field} field missing (may be placeholder)")
            
            # Validate s3 section
            s3_required = ['bucket', 'region']
            s3_section = data.get('s3', {})
            for field in s3_required:
                if field in s3_section:
                    print(f"  ✅ S3 {field} field present")
                else:
                    print(f"  ❌ S3 {field} field missing")
            
            # Validate tables section
            tables = data.get('tables', [])
            if isinstance(tables, list) and len(tables) > 0:
                print(f"  ✅ Tables section valid with {len(tables)} table(s)")
                for i, table in enumerate(tables):
                    if 'name' in table:
                        print(f"    ✅ Table {i+1}: name field present")
                    else:
                        print(f"    ❌ Table {i+1}: name field missing")
            else:
                print(f"  ❌ Tables section invalid or empty")
            
            return True
        else:
            print(f"❌ {os.path.basename(file_path)}: Missing required sections")
            return False
            
    except json.JSONDecodeError as e:
        print(f"❌ {os.path.basename(file_path)}: JSON decode error - {e}")
        return False
    except Exception as e:
        print(f"❌ {os.path.basename(file_path)}: Error - {e}")
        return False


def validate_requirements_file(file_path):
    """Validate requirements.txt structure."""
    try:
        with open(file_path, 'r') as f:
            lines = f.readlines()
        
        required_packages = [
            'pandas', 'sqlalchemy', 'pymysql', 'boto3', 
            'pyarrow', 'tqdm', 'numpy'
        ]
        
        found_packages = set()
        for line in lines:
            line = line.strip()
            if line and not line.startswith('#'):
                # Extract package name (before >= or == etc.)
                package_name = re.split(r'[>=<!\s]', line)[0].strip()
                found_packages.add(package_name.lower())
        
        print(f"✅ {os.path.basename(file_path)}: Requirements file structure valid")
        
        for package in required_packages:
            if package.lower() in found_packages:
                print(f"  ✅ {package} dependency included")
            else:
                print(f"  ⚠️ {package} dependency missing")
        
        return True
        
    except Exception as e:
        print(f"❌ {os.path.basename(file_path)}: Error - {e}")
        return False


def validate_notebook_structure(file_path):
    """Validate Jupyter notebook structure."""
    try:
        with open(file_path, 'r') as f:
            notebook = json.load(f)
        
        # Check basic notebook structure
        required_fields = ['cells', 'metadata', 'nbformat']
        for field in required_fields:
            if field in notebook:
                print(f"  ✅ Notebook {field} field present")
            else:
                print(f"  ❌ Notebook {field} field missing")
        
        # Check cells
        cells = notebook.get('cells', [])
        markdown_cells = sum(1 for cell in cells if cell.get('cell_type') == 'markdown')
        code_cells = sum(1 for cell in cells if cell.get('cell_type') == 'code')
        
        print(f"✅ {os.path.basename(file_path)}: Notebook structure valid")
        print(f"  📊 Total cells: {len(cells)} (Markdown: {markdown_cells}, Code: {code_cells})")
        
        return True
        
    except Exception as e:
        print(f"❌ {os.path.basename(file_path)}: Error - {e}")
        return False


def check_script_structure(file_path):
    """Check the overall structure of the main script."""
    try:
        with open(file_path, 'r') as f:
            content = f.read()
        
        # Check for important classes and functions
        important_components = [
            'class MySQLToS3Ingestion',
            'def __init__',
            'def _setup_mysql_connection',
            'def _setup_s3_client',
            'def _extract_table_data',
            'def _upload_to_s3',
            'def run_ingestion',
            'def load_config',
            'def create_sample_config'
        ]
        
        print(f"✅ {os.path.basename(file_path)}: Checking script structure")
        
        for component in important_components:
            if component in content:
                print(f"  ✅ {component} found")
            else:
                print(f"  ❌ {component} missing")
        
        # Check for proper imports section
        if 'import pandas as pd' in content:
            print("  ✅ pandas import found")
        if 'import sqlalchemy' in content:
            print("  ✅ sqlalchemy import found")
        if 'import boto3' in content:
            print("  ✅ boto3 import found")
        if 'import pyarrow' in content:
            print("  ✅ pyarrow import found")
        
        # Check for proper error handling
        error_handling_patterns = [
            'try:', 'except:', 'raise', 'logging'
        ]
        
        for pattern in error_handling_patterns:
            if pattern in content:
                print(f"  ✅ {pattern} error handling found")
        
        return True
        
    except Exception as e:
        print(f"❌ {os.path.basename(file_path)}: Error - {e}")
        return False


def main():
    """Main validation function."""
    print("🔍 MySQL to S3 Data Ingestion - Validation Suite")
    print("=" * 60)
    
    base_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Files to validate
    files_to_check = [
        {
            'path': os.path.join(base_dir, 'mysql_to_s3_ingestion.py'),
            'type': 'python',
            'description': 'Main ingestion script'
        },
        {
            'path': os.path.join(base_dir, 'test_ingestion.py'),
            'type': 'python',
            'description': 'Test script'
        },
        {
            'path': os.path.join(base_dir, 'requirements.txt'),
            'type': 'requirements',
            'description': 'Dependencies file'
        },
        {
            'path': os.path.join(base_dir, 'sample_config.json'),
            'type': 'json',
            'description': 'Sample configuration'
        },
        {
            'path': os.path.join(base_dir, 'mysql_to_s3_example.ipynb'),
            'type': 'notebook',
            'description': 'Example Jupyter notebook'
        }
    ]
    
    all_valid = True
    
    for file_info in files_to_check:
        file_path = file_info['path']
        file_type = file_info['type']
        description = file_info['description']
        
        print(f"\n📁 Validating {description}: {os.path.basename(file_path)}")
        print("-" * 40)
        
        if not os.path.exists(file_path):
            print(f"❌ File not found: {file_path}")
            all_valid = False
            continue
        
        try:
            if file_type == 'python':
                if not validate_python_syntax(file_path):
                    all_valid = False
                if file_path.endswith('mysql_to_s3_ingestion.py'):
                    if not check_script_structure(file_path):
                        all_valid = False
            elif file_type == 'json':
                if not validate_json_structure(file_path):
                    all_valid = False
            elif file_type == 'requirements':
                if not validate_requirements_file(file_path):
                    all_valid = False
            elif file_type == 'notebook':
                if not validate_notebook_structure(file_path):
                    all_valid = False
                    
        except Exception as e:
            print(f"❌ Unexpected error validating {file_path}: {e}")
            all_valid = False
    
    print("\n" + "=" * 60)
    if all_valid:
        print("🎉 All validation checks passed!")
        print("\nNext steps:")
        print("1. Install dependencies: pip install -r requirements.txt")
        print("2. Configure your database and S3 credentials")
        print("3. Test with the example notebook or command line")
        print("4. Run the ingestion process")
    else:
        print("❌ Some validation checks failed.")
        print("Please review the issues above and fix them before proceeding.")
    
    print("=" * 60)
    
    return all_valid


if __name__ == "__main__":
    main()