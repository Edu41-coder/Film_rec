import pandas as pd
from sklearn.neighbors import NearestNeighbors
from sklearn.metrics import mean_squared_error  # Importer la métrique pour les régressions
import pickle
import mlflow
import mlflow.sklearn

def train_model(movie_matrix):
    nbrs = NearestNeighbors(n_neighbors=20, algorithm="ball_tree").fit(
        movie_matrix
    )
    return nbrs

if __name__ == "__main__":
    # Configurer MLflow
    mlflow.start_run()

    # Charger les données
    movie_matrix = pd.read_csv("Datasets/movies_ratings_merged.csv")  # Chemin relatif

    # Enregistrer les paramètres
    mlflow.log_param("data_path", "Datasets/movies_ratings_merged.csv")  # Chemin relatif
    mlflow.log_param("n_neighbors", 20)

    # Supprimer les colonnes 'title', 'userId', et 'genres' pour l'entraînement
    movie_matrix_train = movie_matrix.drop(columns=["title", "userId", "genres", "rating"])  # Exclure 'title', 'userId', 'genres', et 'rating'

    # Définir y_true avec la colonne 'rating'
    y_true = movie_matrix["rating"]  # Utiliser 'rating' comme la colonne cible

    # Entraîner le modèle sur les données
    model = train_model(movie_matrix_train)  # Entraîner le modèle

    # Calculer les prédictions
    predictions = model.kneighbors(movie_matrix_train, return_distance=False)

    # Calculer la moyenne des ratings des voisins pour chaque prédiction
    predicted_ratings = [y_true.iloc[pred].mean() for pred in predictions]  # Calculer la moyenne des ratings des voisins

    # Calculer la métrique (utiliser MSE pour les régressions)
    mse = mean_squared_error(y_true, predicted_ratings)  # Calculer l'erreur quadratique moyenne

    # Enregistrer la métrique
    mlflow.log_metric("mse", mse)

    # Ajouter la colonne 'title' pour afficher les résultats
    movie_matrix['predicted_rating'] = predicted_ratings  # Ajouter les prédictions au DataFrame
    results = movie_matrix[['title', 'movieId', 'rating', 'predicted_rating']]  # Sélectionner les colonnes à afficher

    # Afficher les résultats
    print(results)

    # Enregistrer le modèle
    mlflow.sklearn.log_model(model, "model")

    # Sauvegarder le modèle localement
    with open("models/model.pkl", "wb") as filehandler:
        pickle.dump(model, filehandler)

    # Fin de l'exécution de MLflow
    mlflow.end_run()