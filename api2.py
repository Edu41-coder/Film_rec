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

@app.get("/movies")
async def get_movies(limit: int = 10):
    try:
        df = load_csv("movies.csv")
        return JSONResponse(content=df.head(limit).to_dict(orient="records"))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/ratings")
async def get_ratings(limit: int = 10):
    try:
        df = load_csv("ratings.csv")
        return JSONResponse(content=df.head(limit).to_dict(orient="records"))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/tags")
async def get_tags(limit: int = 10):
    try:
        df = load_csv("tags.csv")
        return JSONResponse(content=df.head(limit).to_dict(orient="records"))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/links")
async def get_links(limit: int = 10):
    try:
        df = load_csv("links.csv")
        return JSONResponse(content=df.head(limit).to_dict(orient="records"))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/genome-scores")
async def get_genome_scores(limit: int = 10):
    try:
        df = load_csv("genome-scores.csv")
        return JSONResponse(content=df.head(limit).to_dict(orient="records"))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/genome-tags")
async def get_genome_tags(limit: int = 10):
    try:
        df = load_csv("genome-tags.csv")
        return JSONResponse(content=df.head(limit).to_dict(orient="records"))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))