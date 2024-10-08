import pandas as pd
import numpy as np

class GeneralDataPipeline:
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

    def describe_data(self):
        """Décrire les données traitées."""
        print("\nDescription des données traitées:")
        print(f"Nombre total d'entrées: {len(self.data)}")
        print(f"Nombre de colonnes: {len(self.data.columns)}")
        print("\nColonnes présentes:")
        for col in self.data.columns:
            print(f"- {col}")
        print("\nTypes de données:")
        print(self.data.dtypes)
        print("\nAperçu statistique:")
        print(self.data.describe(include='all').T)
        print("\nValeurs uniques par colonne:")
        for col in self.data.columns:
            print(f"{col}: {self.data[col].nunique()} valeurs uniques")
        print("\nValue counts pour chaque colonne:")
        for col in self.data.columns:
            print(f"\nValue counts pour {col}:")
            value_counts = self.data[col].value_counts()
            print(value_counts.head(10))  # Affiche les 10 premières valeurs
            if len(value_counts) > 10:
                print("...")  # Indique qu'il y a plus de valeurs

    def run_pipeline(self):
        """Exécuter toutes les étapes du pipeline."""
        self.load_data()
        self.clean_data()
        self.describe_data()
        print("Pipeline exécuté avec succès.")

    def get_data(self):
        """Retourner les données traitées."""
        return self.data

if __name__ == "__main__":
    # Liste des chemins vers vos fichiers CSV
    csv_files = [
        '/Film_recom/movies.csv',
        '/Film_recom/ratings.csv',
        '/Film_recom/tags.csv',
        '/Film_recom/links.csv',
        '/Film_recom/genome-scores.csv',
        '/Film_recom/genome-tags.csv'
    ]

    # Traiter chaque fichier
    for file_path in csv_files:
        print(f"\n\nTraitement du fichier : {file_path}")
        pipeline = GeneralDataPipeline(file_path)
        pipeline.run_pipeline()

        # Si vous voulez faire quelque chose avec les données traitées, vous pouvez le faire ici
        # processed_data = pipeline.get_data()
        # ... (faire quelque chose avec processed_data)

    print("\nTous les fichiers ont été traités.")