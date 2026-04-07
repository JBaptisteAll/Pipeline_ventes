
# Image de base Python légère
FROM python:3.11-slim
 
# Définit le dossier de travail dans le conteneur
WORKDIR /app
 
# Copie les fichiers du projet dans le conteneur
COPY main.py .
COPY analyses.sql .
 
# Installe les dépendances Python
RUN pip install requests pandas
 
# Lance le script principal
CMD ["python", "main.py"]
 