import pandas as pd
import numpy as np
from sklearn.preprocessing import MultiLabelBinarizer

class MergedMovieDataPipeline:
    def __init__(self, file_path):
        self.file_path = file_path
        self.data = None
        self.mlb = MultiLabelBinarizer()

    def load_data(self):
        """Charger les données depuis le fichier CSV."""
        self.data = pd.read_csv(self.file_path)
        print("Données fusionnées chargées avec succès.")
        print("Colonnes présentes dans les données:", self.data.columns.tolist())

    def clean_data(self):
        """Nettoyer les données."""
        if 'year' not in self.data.columns:
            self.data['year'] = self.data['title'].str.extract('(\d{4})', expand=False)
            self.data['title'] = self.data['title'].str.replace('(\(\d{4}\))', '', regex=True).str.strip()
        
        self.data['year'] = pd.to_numeric(self.data['year'], errors='coerce')
        self.data = self.data[self.data['year'] >= 1900].copy()
        
        print("Données nettoyées avec succès.")

    def process_genres(self):
        """Traiter les genres et créer des colonnes binaires."""
        self.data['genres'] = self.data['genres'].apply(lambda x: [] if pd.isna(x) else str(x).split('|'))
        
        genre_matrix = self.mlb.fit_transform(self.data['genres'])
        genre_df = pd.DataFrame(genre_matrix, columns=self.mlb.classes_)
        
        self.data = pd.concat([self.data, genre_df], axis=1)
        
        print("Genres traités avec succès.")

    def calculate_genre_count(self):
        """Calculer le nombre de genres par film."""
        self.data['genre_count'] = self.data['genres'].apply(lambda x: len(x) if isinstance(x, list) else 0)
        print("Nombre de genres par film calculé.")

    def process_ratings(self):
        """Traiter les données de notation."""
        if 'rating' in self.data.columns:
            self.data['rating'] = pd.to_numeric(self.data['rating'], errors='coerce')
            if 'rating_count' in self.data.columns:
                self.data['rating_count'] = pd.to_numeric(self.data['rating_count'], errors='coerce')
            else:
                # Si rating_count n'existe pas, créez-le en comptant les notations par film
                self.data['rating_count'] = self.data.groupby('movieId')['rating'].transform('count')
            print("Données de notation traitées.")
        else:
            print("Pas de données de notation trouvées.")

    def describe_data(self):
        """Décrire les données traitées."""
        print("\nDescription des données fusionnées traitées:")
        print(f"Nombre total de films: {len(self.data)}")
        print(f"Nombre de colonnes: {len(self.data.columns)}")
        print("\nColonnes présentes:")
        for col in self.data.columns:
            print(f"- {col}")
        print("\nAperçu des données numériques:")
        print(self.data.describe())
        print("\nDistribution des genres:")
        genre_counts = self.data[self.mlb.classes_].sum().sort_values(ascending=False)
        print(genre_counts.head(10))
        print("\nNombre moyen de genres par film:", self.data['genre_count'].mean())
        if 'rating' in self.data.columns:
            print("\nNote moyenne:", self.data['rating'].mean())
            if 'rating_count' in self.data.columns:
                print("Nombre moyen de notations par film:", self.data['rating_count'].mean())
            else:
                print("Colonne 'rating_count' non trouvée.")

    def run_pipeline(self):
        """Exécuter toutes les étapes du pipeline."""
        self.load_data()
        self.clean_data()
        self.process_genres()
        self.calculate_genre_count()
        self.process_ratings()
        self.describe_data()
        print("Pipeline exécuté avec succès.")

    def get_data(self):
        """Retourner les données traitées."""
        return self.data

if __name__ == "__main__":
    pipeline = MergedMovieDataPipeline('/Film_recom/merged_movie_data.csv')
    pipeline.run_pipeline()
    processed_data = pipeline.get_data()
    print(processed_data.head())
    print(processed_data.info())