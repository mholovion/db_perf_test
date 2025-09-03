"""
Centralized configuration loader for AntiAbuse System
Loads all database configurations from a single YAML file
"""
import os
import yaml
from typing import Dict, Any, List


class ConfigLoader:
    """Loads and manages database configurations"""
    
    def __init__(self, config_file: str = "database_config.yaml"):
        self.config_file = config_file
        self._config = None
        self.load_config()
    
    def load_config(self) -> None:
        """Load configuration from YAML file"""
        config_path = os.path.join(os.path.dirname(__file__), self.config_file)
        
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                self._config = yaml.safe_load(f)
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        except yaml.YAMLError as e:
            raise ValueError(f"Error parsing YAML configuration: {e}")
    
    @property
    def config(self) -> Dict[str, Any]:
        """Get the full configuration"""
        if self._config is None:
            self.load_config()
        return self._config
    
    def is_docker_environment(self) -> bool:
        """Check if running in Docker environment"""
        env_config = self.config.get('environment', {})
        
        # Check for Docker environment files
        for indicator in env_config.get('docker_env_indicators', []):
            if os.path.exists(indicator):
                return True
        
        # Check for Docker environment variable
        docker_var = env_config.get('docker_env_var')
        if docker_var and os.environ.get(docker_var):
            return True
        
        return False
    
    def get_database_config(self, db_name: str) -> Dict[str, Any]:
        """Get configuration for a specific database"""
        db_configs = self.config.get('databases', {})
        
        if db_name not in db_configs:
            raise ValueError(f"Database '{db_name}' not found in configuration")
        
        db_config = db_configs[db_name].copy()
        
        # Determine host based on environment
        if self.is_docker_environment():
            host = db_config['container_name']
        else:
            host = 'localhost'
        
        # Build connection config
        ports = db_config.get('ports', {})
        credentials = db_config.get('credentials', {})
        
        return {
            'host': host,
            'port': ports.get('tcp', ports.get('http', 5432)),
            'http_port': ports.get('http'),
            **credentials
        }
    
    def get_docker_compose_service(self, db_name: str) -> Dict[str, Any]:
        """Get Docker Compose service configuration for a database"""
        db_configs = self.config.get('databases', {})
        
        if db_name not in db_configs:
            raise ValueError(f"Database '{db_name}' not found in configuration")
        
        db_config = db_configs[db_name]
        
        # Build Docker Compose service config
        service_config = {
            'image': db_config['image'],
            'container_name': db_config['container_name'],
            'ports': db_config['docker_ports'],
            'volumes': db_config['volumes']
        }
        
        # Add environment variables
        if 'environment' in db_config:
            service_config['environment'] = db_config['environment']
        
        # Add healthcheck
        if 'healthcheck' in db_config:
            service_config['healthcheck'] = db_config['healthcheck']
        
        return service_config
    
    def get_all_databases(self) -> List[str]:
        """Get list of all configured databases"""
        return list(self.config.get('databases', {}).keys())
    
    def generate_docker_compose(self) -> Dict[str, Any]:
        """Generate complete Docker Compose configuration"""
        docker_config = self.config.get('docker', {})
        
        compose_config = {
            'version': '3.8',
            'services': {},
            'networks': {
                docker_config.get('network_name', 'antiabuse_network'): {
                    'driver': 'bridge'
                }
            },
            'volumes': {}
        }
        
        # Add database services
        for db_name in self.get_all_databases():
            compose_config['services'][db_name] = self.get_docker_compose_service(db_name)
        
        # Add volumes
        for volume in docker_config.get('volumes', []):
            compose_config['volumes'][volume] = None
        
        return compose_config


# Global config loader instance
config_loader = ConfigLoader()


# Convenience functions for backward compatibility
def get_database_config(db_name: str) -> Dict[str, Any]:
    """Get database configuration - backward compatible function"""
    return config_loader.get_database_config(db_name)


def is_docker_environment() -> bool:
    """Check if running in Docker environment - backward compatible function"""
    return config_loader.is_docker_environment()


def get_db_host(db_name: str) -> str:
    """Get database host - backward compatible function"""
    return config_loader.get_database_config(db_name)['host']


def get_db_port(db_name: str, port_type: str = 'tcp') -> int:
    """Get database port - backward compatible function"""
    config = config_loader.get_database_config(db_name)
    
    if port_type == 'http' and 'http_port' in config:
        return config['http_port']
    
    return config['port']