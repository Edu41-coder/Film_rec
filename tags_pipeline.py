import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from wordcloud import WordCloud

class TagDataPipeline:
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
        
        print("Données nettoyées avec succès.")

    def calculate_statistics(self):
        """Calculer des statistiques sur les tags."""
        self.data['tags_per_movie'] = self.data.groupby('movieId')['tag'].transform('count')
        self.data['tags_per_user'] = self.data.groupby('userId')['tag'].transform('count')
        print("Statistiques calculées avec succès.")

    def describe_data(self):
        """Décrire les données traitées."""
        print("\nDescription des données traitées:")
        print(f"Nombre total de tags: {len(self.data)}")
        print(f"Nombre de films uniques: {self.data['movieId'].nunique()}")
        print(f"Nombre d'utilisateurs uniques: {self.data['userId'].nunique()}")
        print(f"Nombre de tags uniques: {self.data['tag'].nunique()}")
        print("\nLes 10 tags les plus fréquents:")
        print(self.data['tag'].value_counts().head(10))

    def visualize_data(self):
        """Créer des visualisations des données."""
        # Distribution du nombre de tags par film
        plt.figure(figsize=(12, 6))
        sns.histplot(self.data['tags_per_movie'], bins=50, kde=True)
        plt.title('Distribution du nombre de tags par film')
        plt.xlabel('Nombre de tags')
        plt.ylabel('Nombre de films')
        plt.show()

        # Distribution du nombre de tags par utilisateur
        plt.figure(figsize=(12, 6))
        sns.histplot(self.data['tags_per_user'], bins=50, kde=True)
        plt.title('Distribution du nombre de tags par utilisateur')
        plt.xlabel('Nombre de tags')
        plt.ylabel('Nombre d\'utilisateurs')
        plt.show()

        # Nuage de mots des tags les plus fréquents
        wordcloud = WordCloud(width=800, height=400, background_color='white').generate_from_frequencies(self.data['tag'].value_counts())
        plt.figure(figsize=(10, 5))
        plt.imshow(wordcloud, interpolation='bilinear')
        plt.axis('off')
        plt.title('Nuage de mots des tags les plus fréquents')
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
    pipeline = TagDataPipeline('/Film_recom/tags.csv')
    pipeline.run_pipeline()