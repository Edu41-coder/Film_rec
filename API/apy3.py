from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import pandas as pd
from pathlib import Path

app = FastAPI()

# Définir le chemin de base pour les fichiers CSV
BASE_PATH = Path("F:/Film_recom")

# Fonction pour charger un CSV
def load_csv(filename):
    file_path = BASE_PATH / filename
    if not file_path.exists():
        raise FileNotFoundError(f"Le fichier {filename} n'existe pas.")
    return pd.read_csv(file_path)

@app.get("/")
async def root():
    return {"message": "Bienvenue dans l'API de recommandation de films"}

# Fonction générique pour gérer les requêtes de données
async def get_data(filename: str, limit: int = 10):
    try:
        df = load_csv(f"{filename}.csv")
        return JSONResponse(content=df.head(limit).to_dict(orient="records"))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Utiliser la fonction générique pour toutes les routes de données
@app.get("/movies")
async def get_movies(limit: int = 10):
    return await get_data("movies", limit)

@app.get("/ratings")
async def get_ratings(limit: int = 10):
    return await get_data("ratings", limit)

@app.get("/tags")
async def get_tags(limit: int = 10):
    return await get_data("tags", limit)

@app.get("/links")
async def get_links(limit: int = 10):
    return await get_data("links", limit)

@app.get("/genome-scores")
async def get_genome_scores(limit: int = 10):
    return await get_data("genome-scores", limit)

@app.get("/genome-tags")
async def get_genome_tags(limit: int = 10):
    return await get_data("genome-tags", limit)