import pandas as pd
import numpy as np
from sklearn.preprocessing import MultiLabelBinarizer

class MovieDataPipeline:
    def __init__(self, file_path):
        self.file_path = file_path
        self.data = None
        self.mlb = MultiLabelBinarizer()

    def load_data(self):
        """Charger les données depuis le fichier CSV."""
        self.data = pd.read_csv(self.file_path)
        print("Données chargées avec succès.")

    def clean_data(self):
        """Nettoyer les données."""
        # Extraire l'année du titre
        self.data['year'] = self.data['title'].str.extract('(\d{4})', expand=False)
        self.data['title'] = self.data['title'].str.replace('(\(\d{4}\))', '', regex=True).str.strip()
        
        # Convertir l'année en numérique et filtrer les films à partir de 1900
        self.data['year'] = pd.to_numeric(self.data['year'], errors='coerce')
        self.data = self.data[self.data['year'] >= 1900].copy()
        
        print("Données nettoyées avec succès.")

    def process_genres(self):
        """Traiter les genres et créer des colonnes binaires."""
        # Convertir tous les genres en liste, en remplaçant les NaN par des listes vides
        self.data['genres'] = self.data['genres'].apply(lambda x: [] if pd.isna(x) else str(x).split('|'))
        
        # Utiliser MultiLabelBinarizer pour créer des colonnes binaires pour chaque genre
        genre_matrix = self.mlb.fit_transform(self.data['genres'])
        genre_df = pd.DataFrame(genre_matrix, columns=self.mlb.classes_)
        
        # Concaténer avec le DataFrame original
        self.data = pd.concat([self.data, genre_df], axis=1)
        
        print("Genres traités avec succès.")

    def calculate_genre_count(self):
        """Calculer le nombre de genres par film."""
        print("Type de données de la colonne 'genres':", self.data['genres'].dtype)
        print("Premières valeurs de la colonne 'genres':", self.data['genres'].head())
        print("Nombre de valeurs nulles dans 'genres':", self.data['genres'].isnull().sum())
        
        # Assurez-vous que tous les éléments sont des listes avant d'appliquer len()
        self.data['genre_count'] = self.data['genres'].apply(lambda x: len(x) if isinstance(x, list) else 0)
        print("Nombre de genres par film calculé.")

    def describe_data(self):
        """Décrire les données traitées."""
        print("\nDescription des données traitées:")
        print(f"Nombre total de films: {len(self.data)}")
        print(f"Nombre de colonnes: {len(self.data.columns)}")
        print("\nColonnes présentes:")
        for col in self.data.columns:
            print(f"- {col}")
        print("\nAperçu des données numériques:")
        print(self.data.describe())
        print("\nDistribution des genres:")
        genre_counts = self.data.iloc[:, 5:-1].sum().sort_values(ascending=False)
        print(genre_counts.head(10))
        print("\nNombre moyen de genres par film:", self.data['genre_count'].mean())

    def run_pipeline(self):
        """Exécuter toutes les étapes du pipeline."""
        self.load_data()
        self.clean_data()
        self.process_genres()
        self.calculate_genre_count()
        self.describe_data()
        print("Pipeline exécuté avec succès.")

    def get_data(self):
        """Retourner les données traitées."""
        return self.data

if __name__ == "__main__":
    # Utilisation du pipeline
    pipeline = MovieDataPipeline('/Film_recom/movies.csv')
    pipeline.run_pipeline()

    # Obtenir les données traitées
    processed_data = pipeline.get_data()

    # Afficher les premières lignes pour vérification
    print(processed_data.head())

    # Afficher les informations sur le DataFrame
    print(processed_data.info())