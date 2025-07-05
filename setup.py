#!/usr/bin/env python3
"""
Setup script for the Weather Data Pipeline & API.
This script helps users install dependencies and verify the installation.
"""

import subprocess
import sys
import os

def run_command(command, description):
    """Run a command and handle errors."""
    print(f"\n🔄 {description}...")
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        print(f"✅ {description} completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} failed:")
        print(f"Error: {e.stderr}")
        return False

def check_python_version():
    """Check if Python version is compatible."""
    print("🐍 Checking Python version...")
    version = sys.version_info
    if version.major < 3 or (version.major == 3 and version.minor < 8):
        print(f"❌ Python 3.8+ required, found {version.major}.{version.minor}")
        return False
    print(f"✅ Python {version.major}.{version.minor}.{version.micro} is compatible")
    return True

def install_dependencies():
    """Install Python dependencies."""
    return run_command(
        f"{sys.executable} -m pip install -r requirements.txt",
        "Installing Python dependencies"
    )

def verify_installation():
    """Verify that all dependencies are installed correctly."""
    print("\n🔍 Verifying installation...")
    
    required_packages = [
        'flask', 'sqlalchemy', 'flask_restx', 'flask_cors', 'requests'
    ]
    
    missing_packages = []
    for package in required_packages:
        try:
            __import__(package)
            print(f"✅ {package}")
        except ImportError:
            print(f"❌ {package} - MISSING")
            missing_packages.append(package)
    
    if missing_packages:
        print(f"\n❌ Missing packages: {', '.join(missing_packages)}")
        return False
    
    print("\n✅ All dependencies are installed correctly!")
    return True

def create_directories():
    """Create necessary directories."""
    print("\n📁 Creating directories...")
    
    directories = ['logs', 'data']
    for directory in directories:
        if not os.path.exists(directory):
            os.makedirs(directory)
            print(f"✅ Created {directory}/")
        else:
            print(f"✅ {directory}/ already exists")

def main():
    """Main setup function."""
    print("🚀 Weather Data Pipeline & API Setup")
    print("=" * 50)
    
    # Check Python version
    if not check_python_version():
        sys.exit(1)
    
    # Install dependencies
    if not install_dependencies():
        print("\n❌ Setup failed at dependency installation")
        sys.exit(1)
    
    # Verify installation
    if not verify_installation():
        print("\n❌ Setup failed at verification")
        sys.exit(1)
    
    # Create directories
    create_directories()
    
    print("\n🎉 Setup completed successfully!")
    print("\nNext steps:")
    print("1. Run the demo: python demo.py")
    print("2. Or run the pipeline manually:")
    print("   cd src")
    print("   python main.py")
    print("3. Start the API: python main.py --api-only")
    print("\nFor more information, see README.md")

if __name__ == "__main__":
    main() 