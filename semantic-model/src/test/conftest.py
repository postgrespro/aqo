"""
Configure sys.path so pytest can import modules from src/.
"""
import sys
import os

# Add src/ to path so all sensate/* and utils imports resolve
src_dir = os.path.join(os.path.dirname(__file__), '..')
if src_dir not in sys.path:
    sys.path.insert(0, os.path.abspath(src_dir))
