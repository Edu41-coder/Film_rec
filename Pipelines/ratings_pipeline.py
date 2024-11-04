import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

class RatingDataPipeline:
    def __init__(self, file_path):
        self.file_path = file_path
        self.data = None

    def load_data(self):
        """Charger les données depuis le fichier CSV."""
        self.data = pd.read_csv(self.file_path)
        print("Données chargées avec succès.")

    def clean_data(self):
        """Nettoyer les données."""
        # Vérifier les valeurs manquantes
        na_count = self.data.isna().sum()
        if na_count.sum() > 0:
            print("Valeurs manquantes détectées:")
            print(na_count[na_count > 0])
            # Supprimer les lignes avec des valeurs manquantes
            self.data.dropna(inplace=True)
            print("Lignes avec valeurs manquantes supprimées.")
        else:
            print("Aucune valeur manquante détectée.")

        # Vérifier et supprimer les doublons
        duplicates_count = self.data.duplicated().sum()
        if duplicates_count > 0:
            self.data.drop_duplicates(inplace=True)
            print(f"{duplicates_count} doublons supprimés.")
        else:
            print("Aucun doublon détecté.")
        
        # Convertir le timestamp en datetime si présent
        if 'timestamp' in self.data.columns:
            self.data['date'] = pd.to_datetime(self.data['timestamp'], unit='s')
        
        print("Données nettoyées avec succès.")

    def calculate_statistics(self):
        """Calculer des statistiques sur les évaluations."""
        self.data['user_rating_count'] = self.data.groupby('userId')['rating'].transform('count')
        self.data['movie_rating_count'] = self.data.groupby('movieId')['rating'].transform('count')
        self.data['movie_rating_mean'] = self.data.groupby('movieId')['rating'].transform('mean')
        print("Statistiques calculées avec succès.")

    def describe_data(self):
        """Décrire les données traitées."""
        print("\nDescription des données traitées:")
        print(f"Nombre total d'évaluations: {len(self.data)}")
        print(f"Nombre d'utilisateurs uniques: {self.data['userId'].nunique()}")
        print(f"Nombre de films uniques: {self.data['movieId'].nunique()}")
        print("\nStatistiques des évaluations:")
        print(self.data['rating'].describe())
        print("\nTop 10 des films les plus évalués:")
        print(self.data['movieId'].value_counts().head(10))

    def visualize_data(self):
        """Créer des visualisations des données."""
        # Distribution des évaluations
        plt.figure(figsize=(10, 6))
        sns.histplot(self.data['rating'], bins=10, kde=True)
        plt.title('Distribution des évaluations')
        plt.xlabel('Évaluation')
        plt.ylabel('Nombre d\'évaluations')
        plt.show()

        # Distribution du nombre d'évaluations par utilisateur
        plt.figure(figsize=(12, 6))
        sns.histplot(self.data['user_rating_count'], bins=50, kde=True)
        plt.title('Distribution du nombre d\'évaluations par utilisateur')
        plt.xlabel('Nombre d\'évaluations')
        plt.ylabel('Nombre d\'utilisateurs')
        plt.show()

        # Relation entre le nombre d'évaluations et la note moyenne
        plt.figure(figsize=(10, 6))
        sns.scatterplot(x='movie_rating_count', y='movie_rating_mean', data=self.data.drop_duplicates('movieId'))
        plt.title('Relation entre le nombre d\'évaluations et la note moyenne')
        plt.xlabel('Nombre d\'évaluations')
        plt.ylabel('Note moyenne')
        plt.show()

    def run_pipeline(self):
        """Exécuter toutes les étapes du pipeline."""
        self.load_data()
        self.clean_data()
        self.calculate_statistics()
        self.describe_data()
        self.visualize_data()
        print("Pipeline exécuté avec succès.")

    def get_data(self):
        """Retourner les données traitées."""
        return self.data

if __name__ == "__main__":
    pipeline = RatingDataPipeline('/Film_recom/Datasets/ratings.csv')
    pipeline.run_pipeline()