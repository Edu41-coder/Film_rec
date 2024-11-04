# Utiliser une image de base avec Python
FROM python:3.9-slim
# Définir le répertoire de travail
WORKDIR /app
# Copier les fichiers nécessaires dans le conteneur
COPY requirements.txt ./
COPY Datasets/ ./Datasets/
COPY Models/ ./Models/
COPY dvc.yaml ./
COPY dvc.lock ./
COPY .dvc/ ./.dvc/
# Installer les dépendances
RUN pip install --no-cache-dir -r requirements.txt
# Exposer le port pour l'API
EXPOSE 5000
# Commande par défaut pour exécuter l'API
CMD ["python", "Models/train_model.py"]




















