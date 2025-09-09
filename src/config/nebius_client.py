"""
Hybrid client: Nebius AI for embeddings, OpenRouter (Grok-4) for chat completions
"""
import os
from openai import OpenAI
from typing import List, Dict, Any
from dotenv import load_dotenv

load_dotenv()

class NebiusClient:
    def __init__(self):
        # Nebius client for embeddings
        self.nebius_client = OpenAI(
            base_url="https://api.studio.nebius.com/v1/",
            api_key=os.getenv("NEBIUS_API_KEY")
        )
        
        # OpenRouter client for chat completions (Grok-4)
        self.openrouter_client = OpenAI(
            base_url="https://openrouter.ai/api/v1",
            api_key=os.getenv("OPENROUTER_API_KEY")
        )
        
        # Model configurations
        self.chat_model = "x-ai/grok-3"  # Grok-4 model via OpenRouter
        self.embedding_model = "Qwen/Qwen3-Embedding-8B"  # Keep Nebius embeddings
        self.embedding_dimensions = 4096
    
    def generate_embedding(self, text: str) -> List[float]:
        """Generate embedding using Nebius AI"""
        try:
            response = self.nebius_client.embeddings.create(
                model=self.embedding_model,
                input=text
            )
            return response.data[0].embedding
        except Exception as e:
            print(f"Error generating embedding: {str(e)}")
            raise
    
    def chat_completion(self, messages: List[Dict], temperature: float = 0.7, max_tokens: int = 2048) -> str:
        """Generate chat completion using Grok-4 via OpenRouter"""
        try:
            response = self.openrouter_client.chat.completions.create(
                model=self.chat_model,
                messages=messages,
                temperature=temperature,
                max_tokens=max_tokens,
                timeout=60
            )
            return response.choices[0].message.content
        except Exception as e:
            print(f"Error in chat completion: {str(e)}")
            raise
    
    def batch_embeddings(self, texts: List[str]) -> List[List[float]]:
        """Generate embeddings for multiple texts using Nebius AI"""
        try:
            response = self.nebius_client.embeddings.create(
                model=self.embedding_model,
                input=texts
            )
            return [data.embedding for data in response.data]
        except Exception as e:
            print(f"Error generating batch embeddings: {str(e)}")
            raise
