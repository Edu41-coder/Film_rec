import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

class LinkDataPipeline:
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
        """Calculer des statistiques sur les liens."""
        # Ici, nous ne calculons pas de statistiques supplémentaires car les données de liens sont déjà simples
        pass

    def describe_data(self):
        """Décrire les données traitées."""
        print("\nDescription des données traitées:")
        print(f"Nombre total de liens: {len(self.data)}")
        print(f"Nombre de films uniques: {self.data['movieId'].nunique()}")
        print("\nStatistiques des IDs:")
        print(self.data.describe())

    def visualize_data(self):
        """Créer des visualisations des données."""
        # Distribution des movieId
        plt.figure(figsize=(12, 6))
        sns.histplot(self.data['movieId'], bins=50, kde=True)
        plt.title('Distribution des movieId')
        plt.xlabel('movieId')
        plt.ylabel('Fréquence')
        plt.show()

        # Distribution des imdbId
        plt.figure(figsize=(12, 6))
        sns.histplot(self.data['imdbId'], bins=50, kde=True)
        plt.title('Distribution des imdbId')
        plt.xlabel('imdbId')
        plt.ylabel('Fréquence')
        plt.show()

        # Distribution des tmdbId
        plt.figure(figsize=(12, 6))
        sns.histplot(self.data['tmdbId'], bins=50, kde=True)
        plt.title('Distribution des tmdbId')
        plt.xlabel('tmdbId')
        plt.ylabel('Fréquence')
        plt.show()

        # Relation entre movieId et imdbId
        plt.figure(figsize=(12, 6))
        plt.scatter(self.data['movieId'], self.data['imdbId'], alpha=0.5)
        plt.title('Relation entre movieId et imdbId')
        plt.xlabel('movieId')
        plt.ylabel('imdbId')
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
    pipeline = LinkDataPipeline('/Film_recom/links.csv')
    pipeline.run_pipeline()