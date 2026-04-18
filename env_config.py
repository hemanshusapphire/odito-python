"""
Configuration management for Python workers - PRODUCTION READY
Strict environment variable validation with fail-fast behavior
"""

import os
import sys
from typing import Dict, Any
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class ConfigError(Exception):
    """Configuration related errors"""
    pass

class Config:
    """Centralized configuration with strict validation"""
    
    def __init__(self):
        self._validate_required_env_vars()
        self._config = self._build_config()
    
    def _validate_required_env_vars(self):
        """Validate all required environment variables - FAIL FAST if missing"""
        required_vars = [
            'MONGODB_URI',
            'NODE_BACKEND_URL',
            'PYTHON_WORKER_URL',
            'BACKEND_URL'
        ]
        
        missing_vars = []
        for var in required_vars:
            if not os.getenv(var):
                missing_vars.append(var)
        
        if missing_vars:
            print("❌ CRITICAL: Missing required environment variables:")
            for var in missing_vars:
                print(f"   - {var}")
            print("\n💥 Python worker cannot start without these variables.")
            sys.exit(1)
        
        # Check for optional but recommended variables
        recommended_vars = [
            'GEMINI_API_KEY',
            'OPENAI_API_KEY',
            'LOG_LEVEL'
        ]
        
        missing_recommended = []
        for var in recommended_vars:
            if not os.getenv(var):
                missing_recommended.append(var)
        
        if missing_recommended:
            print("⚠️  Optional environment variables not set:")
            for var in missing_recommended:
                print(f"   - {var}")
    
    def _build_config(self) -> Dict[str, Any]:
        """Build configuration dictionary"""
        return {
            # Database configuration
            'database': {
                'uri': os.getenv('MONGODB_URI'),
                'db_name': self._extract_db_name_from_uri()
            },
            
            # Service URLs - CENTRALIZED CONFIGURATION
            'services': {
                'node_backend': os.getenv('NODE_BACKEND_URL'),
                'python_worker': os.getenv('PYTHON_WORKER_URL'),
                'backend': os.getenv('BACKEND_URL')
            },
            
            # API endpoints
            'api_endpoints': {
                'node_backend_api': f"{os.getenv('NODE_BACKEND_URL')}/api",
                'python_worker_api': f"{os.getenv('PYTHON_WORKER_URL')}/api",
                'backend_api': f"{os.getenv('BACKEND_URL')}/api"
            },
            
            # Media URLs
            'media_urls': {
                'audio': f"{os.getenv('BACKEND_URL')}/audio",
                'video': f"{os.getenv('BACKEND_URL')}/video",
                'reports': f"{os.getenv('BACKEND_URL')}/reports"
            },
            
            # Worker configuration
            'worker': {
                'timeout': int(os.getenv('WORKER_TIMEOUT', '900000')),  # 15 minutes default - increased for 25-page scraping
                'max_retries': int(os.getenv('MAX_RETRIES', '3')),
                'log_level': os.getenv('LOG_LEVEL', 'INFO')
            },
            
            # API Keys (optional)
            'api_keys': {
                'gemini': os.getenv('GEMINI_API_KEY'),
                'openai': os.getenv('OPENAI_API_KEY')
            }
        }
    
    def _extract_db_name_from_uri(self) -> str:
        """Extract database name from MongoDB URI"""
        uri = os.getenv('MONGODB_URI')
        
        # Remove query parameters first
        if '?' in uri:
            uri = uri.split('?')[0]
        
        # Extract database name from path
        if '/' in uri:
            path_parts = uri.split('/')
            if len(path_parts) > 3 and path_parts[3]:
                return path_parts[3]
        
        return 'odito_dev'
    
    def get(self, key_path: str, default=None):
        """Get configuration value using dot notation (e.g., 'services.node_backend')"""
        keys = key_path.split('.')
        value = self._config
        
        try:
            for key in keys:
                value = value[key]
            return value
        except (KeyError, TypeError):
            if default is not None:
                return default
            raise ConfigError(f"Configuration key '{key_path}' not found")
    
    def get_service_url(self, service: str) -> str:
        """Get service URL by name"""
        url = self.get(f'services.{service}')
        if not url:
            raise ConfigError(f"Service URL for '{service}' not configured")
        return url
    
    def get_api_url(self, service: str) -> str:
        """Get API URL for service"""
        url = self.get(f'api_endpoints.{service}_api')
        if not url:
            raise ConfigError(f"API URL for '{service}' not configured")
        return url
    
    def get_media_url(self, media_type: str) -> str:
        """Get media URL by type"""
        url = self.get(f'media_urls.{media_type}')
        if not url:
            raise ConfigError(f"Media URL for '{media_type}' not configured")
        return url
    
    def log_config(self):
        """Log configuration (without exposing secrets)"""
        print("🔧 Python Worker Configuration:")
        
        safe_config = {
            'database_uri': '***CONFIGURED***' if self.get('database.uri') else 'NOT_SET',
            'database_name': self.get('database.db_name'),
            'node_backend': self.get('services.node_backend'),
            'python_worker': self.get('services.python_worker'),
            'backend': self.get('services.backend'),
            'worker_timeout': self.get('worker.timeout'),
            'log_level': self.get('worker.log_level'),
            'gemini_api_key': '***CONFIGURED***' if self.get('api_keys.gemini') else 'NOT_SET',
            'openai_api_key': '***CONFIGURED***' if self.get('api_keys.openai') else 'NOT_SET'
        }
        
        for key, value in safe_config.items():
            print(f"   {key}: {value}")

# Global configuration instance
config = Config()

def get_config() -> Config:
    """Get global configuration instance"""
    return config

def validate_environment():
    """Validate environment and exit if invalid"""
    config.log_config()
    print("✅ Environment validation passed")

# Convenience functions
def get_service_url(service: str) -> str:
    """Get service URL by name"""
    return config.get_service_url(service)

def get_api_url(service: str) -> str:
    """Get API URL for service"""
    return config.get_api_url(service)

def get_media_url(media_type: str) -> str:
    """Get media URL by type"""
    return config.get_media_url(media_type)

def get_database_config() -> Dict[str, Any]:
    """Get database configuration"""
    return config.get('database')

def get_worker_config() -> Dict[str, Any]:
    """Get worker configuration"""
    return config.get('worker')
