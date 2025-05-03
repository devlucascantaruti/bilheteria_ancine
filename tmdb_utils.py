import os
import json
import requests
import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from tmdbv3api import TMDb, Movie

# 1) Carrega .env
load_dotenv()
api_key = os.getenv("TMDB_API_KEY", "").strip()

# 2) Inicializa TMDB
tmdb = TMDb()
tmdb.api_key = api_key
tmdb.language = "pt-BR"

# 3) Diretório de cache Parquet
CACHE_DIR = os.getenv("TMDB_CACHE_DIR", "tmdb_cache")
os.makedirs(CACHE_DIR, exist_ok=True)

# 4) API de filmes
tmdb_movie = Movie()

@st.cache_data(show_spinner=False)
def buscar_filme_por_titulo(titulo: str):
    """Busca filmes no TMDB; retorna [] se chave ausente."""
    if not api_key:
        return []
    try:
        return tmdb_movie.search(titulo)
    except Exception:
        return []

@st.cache_data(show_spinner=False)
def detalhes_completos_filme(filme_id: int):
    """Retorna JSON de detalhes do filme, cacheado em Parquet."""
    if not api_key:
        return None

    cache_file = os.path.join(CACHE_DIR, f"{filme_id}.parquet")
    if os.path.exists(cache_file):
        try:
            df = pd.read_parquet(cache_file)
            return json.loads(df["json"].iloc[0])
        except:
            os.remove(cache_file)

    url = f"https://api.themoviedb.org/3/movie/{filme_id}"
    params = {
        "api_key": api_key,
        "language": tmdb.language,
        "append_to_response": "credits"
    }
    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        # Salva em Parquet
        pd.DataFrame({"json": [json.dumps(data)]}) \
          .to_parquet(cache_file, index=False)
        return data
    except:
        return None
