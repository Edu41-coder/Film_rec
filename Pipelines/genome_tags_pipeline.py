import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from wordcloud import WordCloud

class GenomeTagDataPipeline:
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
        """Calculer des statistiques sur les genome tags."""
        self.data['tag_length'] = self.data['tag'].str.len()
        print("Statistiques calculées avec succès.")

    def describe_data(self):
        """Décrire les données traitées."""
        print("\nDescription des données traitées:")
        print(f"Nombre total de genome tags: {len(self.data)}")
        print(f"Nombre de tags uniques: {self.data['tag'].nunique()}")
        print("\nStatistiques des tagId:")
        print(self.data['tagId'].describe())
        print("\nStatistiques de la longueur des tags:")
        print(self.data['tag_length'].describe())

    def visualize_data(self):
        """Créer des visualisations des données."""
        # Distribution de la longueur des tags
        plt.figure(figsize=(12, 6))
        sns.histplot(self.data['tag_length'], bins=30, kde=True)
        plt.title('Distribution de la longueur des tags')
        plt.xlabel('Longueur du tag')
        plt.ylabel('Fréquence')
        plt.show()

        # Top 20 des tags les plus longs
        top_20_longest = self.data.nlargest(20, 'tag_length')
        plt.figure(figsize=(15, 8))
        sns.barplot(x='tag', y='tag_length', data=top_20_longest)
        plt.title('Top 20 des tags les plus longs')
        plt.xlabel('Tag')
        plt.ylabel('Longueur du tag')
        plt.xticks(rotation=90)
        plt.show()

        # Nuage de mots des tags
        wordcloud = WordCloud(width=800, height=400, background_color='white').generate(' '.join(self.data['tag']))
        plt.figure(figsize=(10, 5))
        plt.imshow(wordcloud, interpolation='bilinear')
        plt.axis('off')
        plt.title('Nuage de mots des tags')
        plt.show()

        # Distribution des tagId
        plt.figure(figsize=(12, 6))
        sns.histplot(self.data['tagId'], bins=50, kde=True)
        plt.title('Distribution des tagId')
        plt.xlabel('tagId')
        plt.ylabel('Fréquence')
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
    pipeline = GenomeTagDataPipeline('/Film_recom/Datasets/genome-tags.csv')
    pipeline.run_pipeline()