"""
Main entry point for the Conference Talk RAG System
"""
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from src.ui.conference_talk_app import main

if __name__ == "__main__":
    main()
