import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

class GenomeScoreDataPipeline:
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
        """Calculer des statistiques sur les genome scores."""
        self.data['tags_per_movie'] = self.data.groupby('movieId')['tagId'].transform('count')
        self.data['movies_per_tag'] = self.data.groupby('tagId')['movieId'].transform('count')
        print("Statistiques calculées avec succès.")

    def describe_data(self):
        """Décrire les données traitées."""
        print("\nDescription des données traitées:")
        print(f"Nombre total d'entrées: {len(self.data)}")
        print(f"Nombre de films uniques: {self.data['movieId'].nunique()}")
        print(f"Nombre de tags uniques: {self.data['tagId'].nunique()}")
        print("\nStatistiques des scores de pertinence:")
        print(self.data['relevance'].describe())
        print("\nStatistiques du nombre de tags par film:")
        print(self.data['tags_per_movie'].describe())
        print("\nStatistiques du nombre de films par tag:")
        print(self.data['movies_per_tag'].describe())

    def visualize_data(self):
        """Créer des visualisations des données."""
        # Distribution des scores de pertinence
        plt.figure(figsize=(12, 6))
        sns.histplot(self.data['relevance'], bins=50, kde=True)
        plt.title('Distribution des scores de pertinence')
        plt.xlabel('Score de pertinence')
        plt.ylabel('Fréquence')
        plt.show()

        # Box plot des scores de pertinence
        plt.figure(figsize=(10, 6))
        sns.boxplot(y='relevance', data=self.data)
        plt.title('Box plot des scores de pertinence')
        plt.ylabel('Score de pertinence')
        plt.show()

        # Distribution du nombre de tags par film
        plt.figure(figsize=(12, 6))
        sns.histplot(self.data['tags_per_movie'].unique(), bins=50, kde=True)
        plt.title('Distribution du nombre de tags par film')
        plt.xlabel('Nombre de tags')
        plt.ylabel('Nombre de films')
        plt.show()

        # Top 20 des tags les plus fréquents
        top_20_tags = self.data['tagId'].value_counts().nlargest(20)
        plt.figure(figsize=(15, 8))
        sns.barplot(x=top_20_tags.index, y=top_20_tags.values)
        plt.title('Top 20 des tags les plus fréquents')
        plt.xlabel('Tag ID')
        plt.ylabel('Fréquence')
        plt.xticks(rotation=45)
        plt.show()

        # Heatmap des scores moyens pour les top 20 films et top 20 tags
        top_20_movies = self.data.groupby('movieId')['relevance'].mean().nlargest(20).index
        top_20_tags = self.data.groupby('tagId')['relevance'].mean().nlargest(20).index
        heatmap_data = self.data[self.data['movieId'].isin(top_20_movies) & self.data['tagId'].isin(top_20_tags)]
        heatmap_pivot = heatmap_data.pivot(index='movieId', columns='tagId', values='relevance')
        plt.figure(figsize=(15, 10))
        sns.heatmap(heatmap_pivot, cmap='YlOrRd', annot=False)
        plt.title('Heatmap des scores moyens pour les top 20 films et top 20 tags')
        plt.xlabel('Tag ID')
        plt.ylabel('Movie ID')
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
    pipeline = GenomeScoreDataPipeline('/Film_recom/genome-scores.csv')
    pipeline.run_pipeline()