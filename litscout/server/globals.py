# server/globals.py

# Global imports and configurations used across the server
import os
from pathlib import Path
import torch
from sentence_transformers import SentenceTransformer

# from server.database.db_utils import get_conn

# Global paths
BASE_DIR = Path(__file__).parent
PGDATA_DIR = BASE_DIR / "database" / "pgdata"
SCHEMA_PATH = BASE_DIR / "database" / "schema.sql"
PGDATA_DIR.mkdir(parents=True, exist_ok=True)

# Shared env-based configuration
ENV_DB_NAME = os.getenv("LITSCOUT_DB_NAME", "litscout")
ENV_DB_USER = os.getenv("LITSCOUT_DB_USER", "admin")
ENV_DB_PASSWORD = os.getenv("LITSCOUT_DB_PASSWORD", "admin")
ENV_DB_HOST = os.getenv("LITSCOUT_DB_HOST", "localhost")
ENV_DB_PORT = os.getenv("LITSCOUT_DB_PORT", "5432")

# Semantic search model
SEMANTIC_SEARCH_MODEL_NAME = os.getenv("LITSCOUT_EMBED_MODEL", "BAAI/bge-base-en-v1.5")
DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
SEMANTIC_SEARCH_MODEL = SentenceTransformer(SEMANTIC_SEARCH_MODEL_NAME, device=DEVICE)