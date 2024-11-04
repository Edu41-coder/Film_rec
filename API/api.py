from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import pandas as pd
from pathlib import Path

app = FastAPI()

# Définir le chemin de base pour les fichiers CSV
BASE_PATH = Path("F:/Film_recom/Datasets")

# Dictionnaire global pour stocker les DataFrames
dfs = {}

@app.on_event("startup")
async def load_csv_files():
    global dfs
    csv_files = ["movies.csv", "ratings.csv", "tags.csv", "links.csv", "genome-scores.csv", "genome-tags.csv"]
    
    for file in csv_files:
        file_path = BASE_PATH / file
        if not file_path.exists():
            print(f"Attention : Le fichier {file} n'existe pas.")
            continue
        dfs[file.replace('.csv', '')] = pd.read_csv(file_path)
        print(f"Fichier {file} chargé avec succès !")

@app.get("/")
async def root():
    return {"message": "Bienvenue dans l'API de recommandation de films"}

@app.get("/{dataset}")
async def get_dataset(dataset: str, limit: int = 10):
    if dataset not in dfs:
        raise HTTPException(status_code=404, detail=f"Le dataset {dataset} n'existe pas.")
    
    try:
        return JSONResponse(content=dfs[dataset].head(limit).to_dict(orient="records"))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/movies")
async def get_movies(limit: int = 10):
    return await get_dataset("movies", limit)

@app.get("/ratings")
async def get_ratings(limit: int = 10):
    return await get_dataset("ratings", limit)

@app.get("/tags")
async def get_tags(limit: int = 10):
    return await get_dataset("tags", limit)

@app.get("/links")
async def get_links(limit: int = 10):
    return await get_dataset("links", limit)

@app.get("/genome-scores")
async def get_genome_scores(limit: int = 10):
    return await get_dataset("genome-scores", limit)

@app.get("/genome-tags")
async def get_genome_tags(limit: int = 10):
    return await get_dataset("genome-tags", limit)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)