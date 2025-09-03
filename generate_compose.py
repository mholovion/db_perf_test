#!/usr/bin/env python3
"""
Generate docker-compose.yml from centralized configuration
"""
import yaml
from config_loader import config_loader


def generate_docker_compose():
    """Generate docker-compose.yml from centralized config"""
    
    compose_config = config_loader.generate_docker_compose()
    
    # Write to docker-compose.yml
    with open('docker-compose.generated.yml', 'w', encoding='utf-8') as f:
        yaml.dump(compose_config, f, default_flow_style=False, sort_keys=False)
    
    return {
        'generated_file': 'docker-compose.generated.yml',
        'source': 'database_config.yaml',
        'instructions': [
            'Review docker-compose.generated.yml',
            'Replace docker-compose.yml if satisfied: mv docker-compose.generated.yml docker-compose.yml'
        ]
    }


if __name__ == "__main__":
    generate_docker_compose()