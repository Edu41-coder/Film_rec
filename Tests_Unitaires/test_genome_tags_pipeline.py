import unittest
import pandas as pd
import numpy as np
from genome_tags_pipeline import GenomeTagDataPipeline
import tempfile
import os

class TestGenomeTagDataPipeline(unittest.TestCase):

    def setUp(self):
        # Créer un DataFrame de test
        self.test_data = pd.DataFrame({
            'tagId': [1, 2, 3, 4, 5],
            'tag': ['action', 'comedy', 'drama', 'sci-fi', 'romance']
        })
        
        # Créer un fichier CSV temporaire
        self.temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
        self.test_data.to_csv(self.temp_file.name, index=False)
        self.temp_file.close()  # Fermer explicitement le fichier
        
        # Initialiser le pipeline avec le fichier temporaire
        self.pipeline = GenomeTagDataPipeline(self.temp_file.name)

    def tearDown(self):
        # Supprimer le fichier temporaire après chaque test
        try:
            os.unlink(self.temp_file.name)
        except PermissionError:
            pass  # Ignorer l'erreur si le fichier ne peut pas être supprimé

    def test_load_data(self):
        self.pipeline.load_data()
        self.assertIsNotNone(self.pipeline.data)
        self.assertEqual(len(self.pipeline.data), 5)
        self.assertEqual(list(self.pipeline.data.columns), ['tagId', 'tag'])

    def test_clean_data_no_changes(self):
        self.pipeline.load_data()
        original_data = self.pipeline.data.copy()
        self.pipeline.clean_data()
        pd.testing.assert_frame_equal(self.pipeline.data, original_data)

    def test_clean_data_with_duplicates(self):
        # Ajouter une ligne dupliquée
        self.test_data = pd.concat([self.test_data, self.test_data.iloc[[0]]])
        self.test_data.to_csv(self.temp_file.name, index=False)
        
        self.pipeline.load_data()
        self.pipeline.clean_data()
        self.assertEqual(len(self.pipeline.data), 5)

    def test_clean_data_with_missing_values(self):
        # Ajouter une ligne avec des valeurs manquantes
        missing_row = pd.DataFrame([[None, None]], columns=self.test_data.columns)
        self.test_data = pd.concat([self.test_data, missing_row], ignore_index=True)
        self.test_data.to_csv(self.temp_file.name, index=False)
        
        self.pipeline.load_data()
        self.pipeline.clean_data()
        self.assertEqual(len(self.pipeline.data), 5)

    def test_calculate_statistics(self):
        self.pipeline.load_data()
        self.pipeline.calculate_statistics()
        self.assertIn('tag_length', self.pipeline.data.columns)
        self.assertEqual(self.pipeline.data['tag_length'].tolist(), [6, 6, 5, 6, 7])

    def test_describe_data(self):
        self.pipeline.load_data()
        self.pipeline.calculate_statistics()
        # Vérifier que describe_data ne lève pas d'exception
        try:
            self.pipeline.describe_data()
        except Exception as e:
            self.fail(f"describe_data a levé une exception inattendue {e}")

    def test_get_data(self):
        self.pipeline.load_data()
        data = self.pipeline.get_data()
        self.assertIsInstance(data, pd.DataFrame)
        self.assertEqual(len(data), 5)

if __name__ == '__main__':
    unittest.main()
    
    #python test_genome_tags_pipeline.py