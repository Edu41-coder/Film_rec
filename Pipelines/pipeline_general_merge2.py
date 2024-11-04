# Première partie - Imports et initialisation
import pandas as pd
import numpy as np
import os
import time
from itertools import combinations
from ipywidgets import widgets
from IPython.display import display, HTML, clear_output


class GeneralMergePipeline:
    def __init__(self, file_paths):
        self.file_paths = file_paths
        self.data_dict = {}  # Pour stocker les DataFrames individuels
        self.merged_data = None
        self.datasets_dir = "/Film_recom/Datasets"
        self.selected_names = None  # Pour stocker les noms des fichiers sélectionnés
        self.selected_paths = None  # Pour stocker les chemins des fichiers sélectionnés
        self.columns_selections = (
            {}
        )  # Pour stocker les sélections de colonnes par table
        self.merge_keys = {}  # Pour stocker les colonnes de fusion par paire de tables
        self.merge_history = []  # Pour stocker l'historique des fusions
        self.merge_descriptions = {
            "inner": "Garde uniquement les films avec correspondances",
            "left": "Garde tous les films, même sans correspondance",
            "right": "Non recommandé pour ce cas",
            "outer": "Non recommandé pour ce cas",
        }

    def select_files(self):
        """Sélectionner les fichiers à fusionner avec une interface graphique."""
        print("\nFichiers disponibles:")
        file_names = [os.path.basename(f).replace(".csv", "") for f in self.file_paths]
        for i, name in enumerate(file_names, 1):
            print(f"{i}. {name}")

        text_input = widgets.Text(
            value="",
            placeholder="Entrez les numéros séparés par des espaces (ex: 1 3 4)",
            description="Sélection:",
            disabled=False,
            layout={"width": "400px"},
        )

        button = widgets.Button(
            description="Valider", button_style="info", layout={"width": "150px"}
        )
        output = widgets.Output()

        def on_button_clicked(b):
            with output:
                output.clear_output()
                try:
                    selection = text_input.value.strip().split()
                    indices = [int(x) - 1 for x in selection]

                    if any(i < 0 or i >= len(file_names) for i in indices):
                        print("❌ Numéro invalide! Veuillez réessayer.")
                        return

                    if len(indices) < 2:
                        print("❌ Veuillez sélectionner au moins 2 fichiers.")
                        return

                    if len(indices) != len(set(indices)):
                        print("❌ Veuillez ne pas répéter les numéros.")
                        return

                    if "movies" not in file_names[indices[0]]:
                        print(
                            "⚠️ Attention: Il est recommandé de commencer avec movies.csv"
                        )
                        print("Voulez-vous continuer quand même? (O/N)")
                        return

                    self.selected_names = [file_names[i] for i in indices]
                    self.selected_paths = [self.file_paths[i] for i in indices]
                    print(
                        f"\n✅ Fichiers sélectionnés : {' + '.join(self.selected_names)}"
                    )

                except ValueError:
                    print("❌ Entrée invalide! Veuillez entrer des numéros.")

        button.on_click(on_button_clicked)
        display(widgets.HBox([text_input, button]))
        display(output)

    def load_selected_data(self):
        """Charger uniquement les fichiers CSV sélectionnés."""
        if not self.selected_names:
            print("❌ Veuillez d'abord sélectionner les fichiers à fusionner.")
            return

        for name, path in zip(self.selected_names, self.selected_paths):
            print(f"\n📂 Chargement de {name}...")
            self.data_dict[name] = pd.read_csv(path)
            df = self.data_dict[name]

            print(f"\n📊 Statistiques pour {name}:")
            print(f"- Lignes: {len(df):,}")
            print(f"- Colonnes: {len(df.columns)}")

            # Informations sur les lignes dupliquées (toutes colonnes confondues)
            n_duplicates = df.duplicated().sum()
            print(f"- Lignes dupliquées: {n_duplicates:,}")

            print("\n🔍 Analyse par colonne:")
            for col in df.columns:
                n_unique = df[col].nunique()
                n_null = df[col].isnull().sum()
                print(f"- {col}:")
                print(f"  • Valeurs uniques: {n_unique:,}")
                print(f"  • Valeurs nulles: {n_null:,}")

            print("\n👀 Aperçu des données:")
            display(df.head())

        print("\n✅ Données chargées avec succès")

    def select_columns_to_keep(self):
        """Sélectionner les colonnes à garder pour chaque DataFrame."""
        if not self.data_dict:
            print("❌ Veuillez d'abord charger les données.")
            return

        def create_selection_interface(name, df):
            """Créer l'interface de sélection pour une table."""
            print(f"\n📋 Table: {name}")

            # Créer les cases à cocher pour chaque colonne
            checkboxes = []
            for col in df.columns:
                n_unique = df[col].nunique()
                n_null = df[col].isnull().sum()

                checkbox = widgets.Checkbox(
                    value=True,
                    description=f"{col} ({n_unique:,} uniques, {n_null:,} nulles)",
                    indent=False,
                    layout={"width": "auto"},
                )
                checkboxes.append(checkbox)

            validate_button = widgets.Button(
                description="✅ Valider et continuer",
                button_style="success",
                layout={"width": "200px"},
            )

            select_all_button = widgets.Button(
                description="Tout sélectionner",
                button_style="info",
                layout={"width": "150px"},
            )

            deselect_all_button = widgets.Button(
                description="Tout désélectionner",
                button_style="warning",
                layout={"width": "150px"},
            )

            output = widgets.Output()

            def on_validate(b):
                with output:
                    output.clear_output()
                    selected_columns = [
                        col
                        for col, checkbox in zip(df.columns, checkboxes)
                        if checkbox.value
                    ]

                    if not selected_columns:
                        print("❌ Veuillez sélectionner au moins une colonne!")
                        return

                    self.columns_selections[name] = selected_columns
                    self.data_dict[name] = self.data_dict[name][selected_columns]

                    print(f"\n✅ Colonnes sélectionnées pour {name}:")
                    for col in selected_columns:
                        print(f"- {col}")
                    print(f"\n📊 Nouvelles dimensions: {self.data_dict[name].shape}")

                    # Désactiver tous les widgets après validation
                    validate_button.disabled = True
                    select_all_button.disabled = True
                    deselect_all_button.disabled = True
                    for checkbox in checkboxes:
                        checkbox.disabled = True

            def on_select_all(b):
                for checkbox in checkboxes:
                    checkbox.value = True

            def on_deselect_all(b):
                for checkbox in checkboxes:
                    checkbox.value = False

            validate_button.on_click(on_validate)
            select_all_button.on_click(on_select_all)
            deselect_all_button.on_click(on_deselect_all)

            buttons = widgets.HBox(
                [select_all_button, deselect_all_button, validate_button]
            )
            checkboxes_container = widgets.VBox(
                checkboxes, layout={"height": "auto", "overflow_y": "auto"}
            )

            display(HTML(f"<h3>🔍 Sélection des colonnes pour {name}</h3>"))
            display(buttons)
            display(checkboxes_container)
            display(output)

        # Traiter chaque table séquentiellement
        for name, df in self.data_dict.items():
            create_selection_interface(name, df)
            display(HTML("<hr style='margin: 2em 0;'>"))

    def clean_data(self):
        """Nettoyer les données avant la fusion."""
        if not self.data_dict:
            print("❌ Veuillez d'abord charger les données.")
            return

        print("\n🧹 Nettoyage des données...")

        for name in self.data_dict:
            print(f"\nNettoyage de {name}:")

            # Créer une copie explicite du DataFrame
            df = self.data_dict[name].copy()

            # Supprimer les doublons
            initial_rows = len(df)
            df = df.drop_duplicates()
            dropped_rows = initial_rows - len(df)
            if dropped_rows > 0:
                print(f"- {dropped_rows:,} doublons supprimés")

            # Supprimer les lignes avec toutes les valeurs manquantes
            initial_rows = len(df)
            df = df.dropna(how="all")
            dropped_rows = initial_rows - len(df)
            if dropped_rows > 0:
                print(f"- {dropped_rows:,} lignes vides supprimées")

            print(f"✅ Nettoyage terminé pour {name}")
            print(f"📊 Dimensions finales: {df.shape}")

            # Mettre à jour le DataFrame dans le dictionnaire
            self.data_dict[name] = df

        print("\n✅ Nettoyage global terminé")
        
        
    def merge_data(self):
        """Fusionner les données de manière séquentielle."""
        if not self.data_dict:
            print("❌ Veuillez d'abord charger les données.")
            return

        def display_dataframe_info(name, df):
            """Afficher les informations d'un DataFrame."""
            print(f"{name}:")
            print(f"- Shape: {df.shape}")
            print(f"- Types des colonnes:")
            for col in df.columns:
                print(f"  • {col}: {df[col].dtype}")

        def create_merge_widgets(common_cols):
            """Créer les widgets pour la fusion."""
            return {
                'cols_select': widgets.SelectMultiple(
                    options=list(common_cols),
                    value=[col for col in common_cols if col in ['movieId']],
                    description='Colonnes:',
                    layout={'width': '400px', 'height': '150px'}
                ),
                'type_select': widgets.Dropdown(
                    options=[
                        ('Inner - Garde uniquement les correspondances', 'inner'),
                        ('Left - Garde tous les films', 'left'),
                        ('Right - Garde toutes les lignes de la 2ème table', 'right'),
                        ('Outer - Garde toutes les lignes des deux tables', 'outer')
                    ],
                    value='inner',
                    description='Type:',
                    layout={'width': '400px'}
                ),
                'validate': widgets.Button(
                    description='✅ Fusionner',
                    button_style='success',
                    layout={'width': '150px'}
                )
            }

        def analyze_merge_relationship(df1, df2, common_cols):
            """Analyser la relation entre deux DataFrames."""
            results = {}
            for col in common_cols:
                left_unique = df1[col].nunique()
                right_unique = df2[col].nunique()
                left_total = len(df1)
                right_total = len(df2)
                
                results[col] = {
                    'left_unique': left_unique,
                    'right_unique': right_unique,
                    'left_total': left_total,
                    'right_total': right_total,
                    'left_ratio': left_total/left_unique if left_unique > 0 else 0,
                    'right_ratio': right_total/right_unique if right_unique > 0 else 0
                }
            return results

        def _display_merge_analysis(name, relations):
            """Afficher l'analyse des relations entre tables."""
            print("\n📊 Analyse des relations:")
            for col, stats in relations.items():
                print(f"\nColonne de jointure: {col}")
                print(f"Table principale ({self.selected_names[0]}):")
                print(f"- {stats['left_unique']:,} valeurs uniques")
                print(f"- {stats['left_total']:,} lignes totales")
                
                print(f"\nTable à fusionner ({name}):")
                print(f"- {stats['right_unique']:,} valeurs uniques")
                print(f"- {stats['right_total']:,} lignes totales")
                
                if stats['right_ratio'] > 1:
                    print(f"\n📈 En moyenne {stats['right_ratio']:.1f} lignes par valeur unique")

        def optimize_dataframe(df):
            """Optimiser l'utilisation de la mémoire d'un DataFrame."""
            for col in df.columns:
                if df[col].dtype == 'int64':
                    df[col] = df[col].astype('int32')
                elif df[col].dtype == 'float64':
                    df[col] = df[col].astype('float32')
            return df

        def merge_with_chunks(df1, df2, common_cols, merge_type='inner', chunk_size=1_000_000):
            """Fusion par chunks pour les grands DataFrames."""
            print(f"📦 Fusion par chunks (taille: {chunk_size:,})...")
            
            # Optimiser les types de données
            df1 = optimize_dataframe(df1)
            df2 = optimize_dataframe(df2)
            
            # Diviser le plus grand DataFrame en chunks
            if len(df1) > len(df2):
                chunks = [df1[i:i + chunk_size] for i in range(0, len(df1), chunk_size)]
                static_df = df2
            else:
                chunks = [df2[i:i + chunk_size] for i in range(0, len(df2), chunk_size)]
                static_df = df1
                # Inverser le type de fusion si nécessaire
                if merge_type == 'left':
                    merge_type = 'right'
                elif merge_type == 'right':
                    merge_type = 'left'

            result_dfs = []
            total_chunks = len(chunks)
            
            for i, chunk in enumerate(chunks, 1):
                print(f"  ↳ Chunk {i}/{total_chunks}...", end='\r')
                if len(df1) > len(df2):
                    temp = pd.merge(chunk, static_df, on=common_cols, how=merge_type)
                else:
                    temp = pd.merge(static_df, chunk, on=common_cols, how=merge_type)
                result_dfs.append(temp)
                
            print("\n✅ Fusion des chunks terminée")
            return pd.concat(result_dfs, ignore_index=True)

        def process_next_merge():
            """Traiter la prochaine fusion."""
            if self.current_merge_index >= len(self.remaining_names):
                self.finalize_merge()
                return

            name = self.remaining_names[self.current_merge_index]
            common_cols = set(self.merged.columns) & set(self.data_dict[name].columns)
            
            print(f"\n🔄 Fusion avec {name}")
            print(f"Colonnes communes: {list(common_cols)}")

            if not common_cols:
                raise Exception(f"❌ Aucune colonne commune avec {name}")

            relations = analyze_merge_relationship(
                self.merged, self.data_dict[name], common_cols
            )
            
            _display_merge_analysis(name, relations)

            merge_options = [
                (f"Inner - {self.merge_descriptions['inner']}", "inner"),
                (f"Left - {self.merge_descriptions['left']}", "left"),
            ]

            dropdown = widgets.Dropdown(
                options=merge_options,
                value='inner',
                description="Type:",
                style={'description_width': 'initial'},
                layout={'width': '500px'}
            )

            button = widgets.Button(
                description="Appliquer la fusion",
                button_style='success',
                layout={'width': '200px'}
            )

            output = widgets.Output()

            def on_merge_clicked(b):
                button.disabled = True
                with output:
                    output.clear_output()
                    merge_type = dropdown.value
                    print(f"🔄 Application de la fusion {merge_type}...")

                    # Calculer la taille potentielle du résultat
                    potential_size = len(self.merged) * len(self.data_dict[name])
                    
                    try:
                        if potential_size > 1e9:  # Si très grande fusion
                            temp_merged = merge_with_chunks(
                                self.merged,
                                self.data_dict[name],
                                list(common_cols),
                                merge_type
                            )
                        else:
                            # Fusion directe pour les petites tables
                            temp_merged = pd.merge(
                                self.merged,
                                self.data_dict[name],
                                on=list(common_cols),
                                how=merge_type
                            )

                        print("\n📊 Résultats:")
                        print(f"- Lignes initiales: {len(self.merged):,}")
                        print(f"- Lignes dans {name}: {len(self.data_dict[name]):,}")
                        print(f"- Résultat final: {len(temp_merged):,} lignes")
                        
                        self.merged = temp_merged
                        self.merge_history.append({
                            'tables': (self.selected_names[0], name),
                            'type': merge_type,
                            'result_rows': len(temp_merged)
                        })
                        self.current_merge_index += 1
                        
                        print("\n✅ Fusion terminée!")
                        process_next_merge()
                        
                    except Exception as e:
                        print(f"\n❌ Erreur lors de la fusion: {str(e)}")
                        button.disabled = False

            button.on_click(on_merge_clicked)

            display(HTML("<h4>🔄 Choisissez le type de fusion :</h4>"))
            display(widgets.HBox([dropdown, button]))
            display(output)

        # Démarrer la première fusion
        try:
            print(f"\n🔄 Fusion de : {' + '.join(self.selected_names)}")
            
            first_name = self.selected_names[0]
            self.merged = self.data_dict[first_name].copy()
            print(f"\n📊 Début avec {first_name}: {len(self.merged):,} lignes")
            
            self.current_merge_index = 0
            self.remaining_names = self.selected_names[1:]
            process_next_merge()
            
        except Exception as e:
            print(f"❌ Erreur: {str(e)}")

    def describe_data(self):
        """Décrire les données fusionnées."""
        if self.merged_data is None:
            print("❌ Aucune donnée fusionnée disponible.")
            return

        print("\n📊 Statistiques finales:")
        print(f"- Entrées: {len(self.merged_data):,}")
        print(f"- Colonnes: {len(self.merged_data.columns)}")

        print("\n🔍 Valeurs uniques par colonne:")
        for col in self.merged_data.columns:
            print(f"- {col}: {self.merged_data[col].nunique():,}")

        print("\n📈 Types de données:")
        print(self.merged_data.dtypes)

        print("\n👀 Aperçu statistique:")
        display(self.merged_data.describe(include="all").T)

    def analyze_duplicate_titles(self):
        """Analyser les films qui ont le même titre mais des IDs différents."""
        if self.merged_data is None:
            print("❌ Aucune donnée fusionnée disponible.")
            return

        print("\n🔍 Analyse des titres en double:")

        # Trouver les titres qui ont plusieurs movieId
        duplicates = self.merged_data[["movieId", "title", "genres"]].drop_duplicates()
        duplicate_titles = duplicates.groupby("title")["movieId"].nunique()
        duplicate_titles = duplicate_titles[duplicate_titles > 1]

        if len(duplicate_titles) == 0:
            print("Aucun titre en double trouvé.")
            return

        print(f"\nNombre de titres en double: {len(duplicate_titles)}")

        for title in duplicate_titles.index:
            print("\n" + "=" * 50)
            print(f"📽️ Titre: {title}")
            films = duplicates[duplicates["title"] == title].sort_values("movieId")
            for _, film in films.iterrows():
                print(f"\nID: {film['movieId']}")
                print(f"Genres: {film['genres']}")

    def analyze_extra_rows(self):
        """Analyser les lignes supplémentaires après la fusion avec tags."""
        extra_rows = self.merged_data[self.merged_data["rating"].isna()]
        print(f"\nLignes supplémentaires: {len(extra_rows)}")
        print("\nExemple de ces lignes:")
        display(extra_rows.head())

    def analyze_tags(self):
        """Analyser en détail la distribution des tags."""
        if "tag" not in self.merged_data.columns:
            print("❌ Pas de colonne tag dans les données.")
            return

        print("\n📊 Analyse des tags:")

        # Nombre total de tags non-nuls
        tags_count = self.merged_data["tag"].notna().sum()
        print(f"Nombre total de tags (non-nuls): {tags_count:,}")

        # Distribution des tags
        tag_distribution = self.merged_data["tag"].value_counts()
        print(f"\nNombre de tags uniques: {len(tag_distribution):,}")

        print("\nTop 10 des tags les plus utilisés:")
        for tag, count in tag_distribution.head(10).items():
            print(f"- {tag}: {count:,} fois")

        # Films tagués
        movies_with_tags = self.merged_data[self.merged_data["tag"].notna()][
            "movieId"
        ].nunique()
        print(f"\nNombre de films ayant au moins un tag: {movies_with_tags:,}")

    def get_data(self):
        """Retourner les données fusionnées."""
        return self.merged_data

    def run_pipeline(self):
        """Exécuter le pipeline complet."""
        try:
            print("🚀 Démarrage du pipeline...")

            # 1. Sélection des fichiers
            self.select_files()
            if not self.selected_names:
                return

            # 2. Chargement des données
            self.load_selected_data()
            if not self.data_dict:
                return

            # 3. Sélection des colonnes
            self.select_columns_to_keep()

            # 4. Fusion des données
            print("\n🔄 Début du processus de fusion...")
            self.merge_data()

            # 5. Analyse des résultats
            if self.merged_data is not None:
                print("\n📊 Analyse des résultats...")
                self.describe_data()
                self.analyze_duplicate_titles()
                if "tag" in self.merged_data.columns:
                    self.analyze_tags()

            print("\n✅ Pipeline terminé avec succès!")

        except Exception as e:
            print(f"\n❌ Erreur dans le pipeline: {str(e)}")
            raise  # Pour le débogage


if __name__ == "__main__":
    csv_files = [
        "/Film_recom/Datasets/movies.csv",
        "/Film_recom/Datasets/ratings.csv",
        "/Film_recom/Datasets/tags.csv",
        "/Film_recom/Datasets/links.csv",
        "/Film_recom/Datasets/genome-scores.csv",
        "/Film_recom/Datasets/genome-tags.csv",
    ]

    pipeline = GeneralMergePipeline(csv_files)
    pipeline.run_pipeline()
    print("\n✅ Traitement terminé.")
