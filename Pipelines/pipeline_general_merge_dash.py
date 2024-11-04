import pandas as pd
import os
import gc
import psutil
from dash import Dash, dcc, html, Input, Output, State
import dash_bootstrap_components as dbc
from dash.exceptions import PreventUpdate

class GeneralMergePipeline:
    def __init__(self, file_paths=None):
        self.file_paths = file_paths if file_paths is not None else []
        self.data_dict = {}
        self.merged_data = None
        self.datasets_dir = "/Film_recom/Datasets"
        self.selected_names = []
        self.selected_paths = []
        self.columns_selections = {}
        self.merged = None
        self.current_merge_index = 0
        self.remaining_names = None
        self.merge_history = []
        self.merge_descriptions = {
            "inner": "Garde uniquement les enregistrements avec correspondances",
            "left": "Garde tous les enregistrements de la table principale",
        }

    def optimize_dataframe(self, df):
        """Optimise la mémoire utilisée par le DataFrame en ajustant les types de données."""
        for col in df.columns:
            col_sample = df[col].head(1000)
            col_type = df[col].dtype

            try:
                if col_type == "object":
                    num_unique = df[col].nunique()
                    if num_unique / len(df) < 0.5:
                        df[col] = df[col].astype("category")
                    else:
                        df[col] = df[col].astype("string[pyarrow]")

                elif "int" in str(col_type):
                    col_min = df[col].min()
                    col_max = df[col].max()

                    if col_min >= 0:
                        if col_max < 255:
                            df[col] = df[col].astype("uint8")
                        elif col_max < 65535:
                            df[col] = df[col].astype("uint16")
                        elif col_max < 4294967295:
                            df[col] = df[col].astype("uint32")
                        else:
                            df[col] = df[col].astype("uint64")
                    else:
                        if col_min > -128 and col_max < 127:
                            df[col] = df[col].astype("int8")
                        elif col_min > -32768 and col_max < 32767:
                            df[col] = df[col].astype("int16")
                        elif col_min > -2147483648 and col_max < 2147483647:
                            df[col] = df[col].astype("int32")
                        else:
                            df[col] = df[col].astype("int64")

                elif "float" in str(col_type):
                    if df[col].notna().all() and df[col].mod(1).eq(0).all():
                        df[col] = df[col].astype("int32")
                    else:
                        df[col] = df[col].astype("float32")

            except Exception as e:
                print(f"⚠️ Impossible d'optimiser la colonne {col}: {str(e)}")
                continue

            gc.collect()

        return df

    def select_files(self):
        """Sélectionner les fichiers à partir des chemins fournis."""
        return [os.path.basename(f).replace(".csv", "") for f in self.file_paths]

    def load_selected_data(self):
        if not self.selected_names:
            print("❌ Sélectionnez d'abord les fichiers.")
            return
        for name, path in zip(self.selected_names, self.selected_paths):
            print(f"\n📂 Chargement de {name}...")
            df = pd.read_csv(path)
            df = self.optimize_dataframe(df)
            self.data_dict[name] = df
            print(f"📊 {len(df):,} lignes, {len(df.columns)} colonnes")
            display(df.head(3))
        print("✅ Données chargées")
        gc.collect()

    def select_columns_to_keep(self, selected_columns):
        """Sélectionner les colonnes à conserver."""
        if not self.data_dict:
            print("❌ Chargez d'abord les données.")
            return

        for name, df in self.data_dict.items():
            if name in selected_columns:
                self.columns_selections[name] = selected_columns[name]
                self.data_dict[name] = self.optimize_dataframe(
                    self.data_dict[name][self.columns_selections[name]].copy()
                )
                print(f"✅ Colonnes sélectionnées pour {name}: {', '.join(self.columns_selections[name])}")
                gc.collect()

    def clean_data(self):
        if not self.data_dict:
            print("❌ Chargez d'abord les données.")
            return
        for name, df in self.data_dict.items():
            initial_rows = len(df)
            df.dropna(inplace=True)
            df.drop_duplicates(inplace=True)
            print(f"Table {name}: {initial_rows - len(df):,} lignes supprimées")
            self.data_dict[name] = self.optimize_dataframe(df)
        gc.collect()

    def merge_data(self):
        if not self.data_dict:
            print("❌ Chargez d'abord les données.")
            return
        try:
            first_name = self.selected_names[0]
            if first_name not in self.columns_selections:
                print(f"❌ Sélectionnez d'abord les colonnes pour {first_name}")
                return

            selected_columns = self.columns_selections[first_name]
            if not selected_columns:
                print(f"❌ Aucune colonne sélectionnée pour {first_name}")
                return

            self.merged = self.optimize_dataframe(
                self.data_dict[first_name][selected_columns].copy()
            )
            print(f"\n📊 Début avec {first_name}: {len(self.merged):,} lignes")
            print("\n📋 Colonnes de départ:")
            for col in sorted(self.merged.columns):
                print(f"- {col}: {self.merged[col].nunique():,} valeurs uniques")

            self.current_merge_index = 0
            self.remaining_names = self.selected_names[1:]
            self.process_next_merge()

        except Exception as e:
            print(f"❌ Erreur: {str(e)}")

    def process_next_merge(self):
        if self.current_merge_index >= len(self.remaining_names):
            self.finalize_merge()
            return

        name = self.remaining_names[self.current_merge_index]
        print(f"\n🔄 Fusion avec {name}")

        merged_cols = set(self.merged.columns)
        next_cols = set(self.data_dict[name].columns)
        common_cols = merged_cols & next_cols

        if not common_cols:
            raise Exception(f"❌ Aucune colonne commune avec {name}")

        # Logique d'analyse de la relation de fusion
        # ...

    def finalize_merge(self):
        if not self.merged.empty:
            output_name = "_".join(self.selected_names) + "_merged.csv"
            output_path = os.path.join(self.datasets_dir, output_name)
            self.merged.to_csv(output_path, index=False)
            print(f"✅ Fusion terminée: {len(self.merged):,} lignes")
            print(f"💾 Sauvegardé: {output_name}")

# Initialisation de l'application Dash
app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
pipeline = GeneralMergePipeline()

# Remplir les chemins de fichiers ici
csv_files = [
    "/Film_recom/Datasets/movies.csv",
    "/Film_recom/Datasets/ratings.csv",
    "/Film_recom/Datasets/tags.csv",
    "/Film_recom/Datasets/links.csv",
    "/Film_recom/Datasets/genome-scores.csv",
    "/Film_recom/Datasets/genome-tags.csv",
]
pipeline.file_paths = csv_files  # Assigner les chemins de fichiers

app.layout = html.Div([
    html.H1("Pipeline de fusion de données"),
    dcc.Checklist(
        id='file-selection',
        options=[{'label': name, 'value': name} for name in pipeline.select_files()],
        value=[],  # Valeur par défaut
        inline=True
    ),
    html.Button('Valider', id='validate-button', n_clicks=0),
    html.Div(id='output-validation'),
    html.Div(id='output-description'),
    dcc.Loading(id="loading", children=[html.Div(id="loading-output")], type="default"),
])

@app.callback(
    Output('output-validation', 'children'),
    Output('output-description', 'children'),
    Input('validate-button', 'n_clicks'),
    State('file-selection', 'value'),
    prevent_initial_call=True
)
def validate_files(n_clicks, selected_files):
    if not selected_files:
        return "❌ Aucune donnée à valider.", ""
    
    pipeline.selected_names = selected_files
    pipeline.selected_paths = [f for f in pipeline.file_paths if os.path.basename(f).replace(".csv", "") in selected_files]
    
    pipeline.load_selected_data()
    pipeline.select_columns()
    pipeline.clean_data()
    pipeline.merge_data()
    description = pipeline.describe_data()
    return "✅ Fichiers validés et fusionnés !", description

if __name__ == '__main__':
    app.run_server(debug=True)