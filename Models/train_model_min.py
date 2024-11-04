import pandas as pd
from sklearn.neighbors import NearestNeighbors
from sklearn.metrics import mean_squared_error  # Importer la métrique pour les régressions
import pickle
import mlflow
import mlflow.sklearn

mlflow.set_tracking_uri('http://localhost:5000')

def train_model(movie_matrix):
    print("Entraînement du modèle...")
    nbrs = NearestNeighbors(n_neighbors=20, algorithm="ball_tree").fit(
        movie_matrix
    )
    print("Modèle entraîné avec succès.")
    return nbrs

if __name__ == "__main__":
    # Configurer MLflow
    print("Démarrage de MLflow...")
    mlflow.start_run()

    # Charger les données
    print("Chargement des données...")
    movie_matrix = pd.read_csv("Datasets/movies_ratings_merged.csv")  # Chemin relatif
    print("Données chargées avec succès.")

    # Enregistrer les paramètres
    mlflow.log_param("data_path", "Datasets/movies_ratings_merged.csv")  # Chemin relatif
    mlflow.log_param("n_neighbors", 20)

    # Échantillonner 1 % des données aléatoirement
    print("Échantillonnage de 1 % des données...")
    sample_size = int(0.01 * len(movie_matrix))  # Calculer 1 % de la taille des données
    movie_matrix_sample = movie_matrix.sample(n=sample_size, random_state=42)  # Échantillonner
    print(f"Échantillon de taille {sample_size} créé.")

    # Afficher les colonnes pour vérifier le contenu du DataFrame
    print("Colonnes de l'échantillon :")
    print(movie_matrix_sample.columns)  # Afficher les colonnes du DataFrame

    # Définir y_true avec la colonne 'rating'
    y_true = movie_matrix_sample["rating"]  # Utiliser 'rating' comme la colonne cible
    print("Colonne 'rating' définie comme cible.")

    # Entraîner le modèle sur l'échantillon en supprimant 'title', 'userId', et 'genres'
    model = train_model(movie_matrix_sample.drop(columns=["title", "userId", "rating", "genres"]))  # Exclure 'title', 'userId', 'rating', et 'genres'

    # Calculer les prédictions
    print("Calcul des prédictions...")
    predictions = model.kneighbors(movie_matrix_sample.drop(columns=["title", "userId", "rating", "genres"]), return_distance=False)

    # Calculer la moyenne des ratings des voisins pour chaque prédiction
    print("Calcul des ratings prédits...")
    predicted_ratings = [y_true.iloc[pred].mean() for pred in predictions]  # Calculer la moyenne des ratings des voisins

    # Calculer la métrique (utiliser MSE pour les régressions)
    mse = mean_squared_error(y_true, predicted_ratings)  # Calculer l'erreur quadratique moyenne
    print(f"Erreur quadratique moyenne (MSE) : {mse}")

    # Enregistrer la métrique
    mlflow.log_metric("mse", mse)

    # Ajouter la colonne 'title' pour afficher les résultats
    movie_matrix_sample['predicted_rating'] = predicted_ratings  # Ajouter les prédictions au DataFrame
    results = movie_matrix_sample[['title', 'movieId', 'rating', 'predicted_rating']]  # Sélectionner les colonnes à afficher

    # Afficher les résultats
    print("Résultats :")
    print(results)

    # Enregistrer le modèle
    print("Enregistrement du modèle dans MLflow...")
    mlflow.sklearn.log_model(model, "model")

    # Sauvegarder le modèle localement
    print("Sauvegarde du modèle localement...")
    with open("models/model.pkl", "wb") as filehandler:
        pickle.dump(model, filehandler)
    print("Modèle sauvegardé avec succès.")

    # Fin de l'exécution de MLflow
    print("Fin de l'exécution de MLflow.")
    mlflow.end_run()