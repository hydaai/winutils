#!/usr/bin/env python3
"""
Quick setup script for MySQL to S3 Data Ingestion.

This script helps users get started quickly by:
1. Checking system requirements
2. Installing dependencies (optional)
3. Creating sample configuration
4. Providing next steps guidance
"""

import os
import sys
import subprocess
import importlib
import json


def check_python_version():
    """Check if Python version is compatible."""
    version = sys.version_info
    if version.major >= 3 and version.minor >= 7:
        print(f"✅ Python {version.major}.{version.minor}.{version.micro} is compatible")
        return True
    else:
        print(f"❌ Python {version.major}.{version.minor}.{version.micro} is not compatible")
        print("   Please upgrade to Python 3.7 or higher")
        return False


def check_dependencies():
    """Check if required dependencies are installed."""
    required_packages = [
        'pandas', 'sqlalchemy', 'pymysql', 'boto3', 
        'pyarrow', 'tqdm', 'numpy'
    ]
    
    missing_packages = []
    installed_packages = []
    
    for package in required_packages:
        try:
            importlib.import_module(package)
            installed_packages.append(package)
            print(f"✅ {package} is installed")
        except ImportError:
            missing_packages.append(package)
            print(f"❌ {package} is not installed")
    
    return installed_packages, missing_packages


def install_dependencies(missing_packages):
    """Install missing dependencies."""
    if not missing_packages:
        print("✅ All dependencies are already installed")
        return True
    
    print(f"\n📦 Installing {len(missing_packages)} missing package(s)...")
    
    try:
        # Use requirements.txt if available, otherwise install individually
        if os.path.exists('requirements.txt'):
            print("📋 Installing from requirements.txt...")
            result = subprocess.run([
                sys.executable, '-m', 'pip', 'install', '-r', 'requirements.txt'
            ], capture_output=True, text=True)
        else:
            print("📋 Installing packages individually...")
            result = subprocess.run([
                sys.executable, '-m', 'pip', 'install'
            ] + missing_packages, capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ Dependencies installed successfully")
            return True
        else:
            print("❌ Failed to install dependencies")
            print(f"Error: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ Error installing dependencies: {e}")
        return False


def create_sample_configuration():
    """Create a sample configuration file."""
    try:
        from mysql_to_s3_ingestion import create_sample_config
        
        config_file = "my_ingestion_config.json"
        create_sample_config(config_file)
        
        print(f"✅ Sample configuration created: {config_file}")
        print("   Please edit this file with your actual credentials before using")
        return True
        
    except ImportError:
        print("❌ Cannot import ingestion module. Please install dependencies first.")
        return False
    except Exception as e:
        print(f"❌ Error creating sample configuration: {e}")
        return False


def validate_installation():
    """Validate that everything is set up correctly."""
    try:
        from mysql_to_s3_ingestion import MySQLToS3Ingestion
        print("✅ MySQL to S3 ingestion module imported successfully")
        
        # Check if validation script exists and run it
        if os.path.exists('validate_ingestion.py'):
            print("\n🔍 Running validation checks...")
            result = subprocess.run([
                sys.executable, 'validate_ingestion.py'
            ], capture_output=True, text=True)
            
            if result.returncode == 0:
                print("✅ All validation checks passed")
                return True
            else:
                print("⚠️ Some validation checks failed")
                print(result.stdout)
                return False
        else:
            print("⚠️ Validation script not found, skipping validation")
            return True
            
    except Exception as e:
        print(f"❌ Validation failed: {e}")
        return False


def show_next_steps():
    """Display next steps for the user."""
    print("\n" + "="*60)
    print("🎉 Setup Complete! Next Steps:")
    print("="*60)
    
    print("\n1. 📝 Configure your credentials:")
    print("   - Edit 'my_ingestion_config.json' with your MySQL and S3 credentials")
    print("   - Update table names and batch sizes as needed")
    
    print("\n2. 🧪 Test the setup:")
    print("   - Open the Jupyter notebook: mysql_to_s3_example.ipynb")
    print("   - Or run from command line: python mysql_to_s3_ingestion.py --config my_ingestion_config.json")
    
    print("\n3. 📚 Read the documentation:")
    print("   - Full documentation: README_MYSQL_S3_INGESTION.md")
    print("   - Configuration examples in sample_config.json")
    
    print("\n4. 🚀 Run your first ingestion:")
    print("   - Start with a small table to test")
    print("   - Monitor logs for any issues")
    print("   - Scale up to larger tables as needed")
    
    print("\n5. 🔧 Troubleshooting:")
    print("   - Check logs in the ./logs directory")
    print("   - Verify database connectivity")
    print("   - Confirm S3 permissions")
    
    print("\n" + "="*60)


def main():
    """Main setup function."""
    print("🚀 MySQL to S3 Data Ingestion - Quick Setup")
    print("="*50)
    
    # Check Python version
    if not check_python_version():
        return False
    
    print("\n📦 Checking dependencies...")
    installed, missing = check_dependencies()
    
    # Offer to install missing dependencies
    if missing:
        print(f"\n⚠️ Found {len(missing)} missing dependencies")
        response = input("Would you like to install them now? (y/n): ").lower().strip()
        
        if response in ['y', 'yes']:
            if not install_dependencies(missing):
                print("❌ Failed to install dependencies. Please install manually:")
                print(f"   pip install {' '.join(missing)}")
                return False
        else:
            print("📋 Please install missing dependencies manually:")
            print(f"   pip install {' '.join(missing)}")
            return False
    
    print("\n📄 Creating sample configuration...")
    if not create_sample_configuration():
        return False
    
    print("\n🔍 Validating installation...")
    if not validate_installation():
        print("⚠️ Validation completed with warnings. Check output above.")
    
    show_next_steps()
    return True


if __name__ == "__main__":
    try:
        success = main()
        if not success:
            print("\n❌ Setup incomplete. Please resolve the issues above and try again.")
            sys.exit(1)
    except KeyboardInterrupt:
        print("\n\n⏹️ Setup cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error during setup: {e}")
        sys.exit(1)