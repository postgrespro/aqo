import os
import yaml
from pathlib import Path
from datasets import load_dataset
from huggingface_hub import login

from sensate.schema import GlobalConfigSchema


def setup_huggingface_auth():
    """Setup HuggingFace authentication from .env file."""
    # Look for .env in project root (going up from src/)
    env_file = Path(__file__).parent.parent.parent / '.env'
    
    if env_file.exists():
        # Simple .env parser
        with open(env_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key.strip()] = value.strip()
    
    # Get token and login
    hf_token = os.getenv('HUGGINGFACE_TOKEN_API')
    if hf_token:
        login(token=hf_token, add_to_git_credential=False)
        print("✅ HuggingFace authentication successful")
        return True
    else:
        print("⚠️  No HuggingFace token found in .env file")
        return False


def load_data(owner: str, dataset_name: str) -> list:
    """Load dataset from HuggingFace with authentication."""
    # Setup auth first
    setup_huggingface_auth()
    dataset = load_dataset(f"{owner}/{dataset_name}")['train']
    return dataset['text']

def load_config(path: str) -> GlobalConfigSchema:
    with open(path, 'r') as file:
        config_dict = yaml.safe_load(file)
    return GlobalConfigSchema(**config_dict)