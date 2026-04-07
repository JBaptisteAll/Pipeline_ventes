#IMPORT DES LIBRAIRIES
import sqlite3
import os
import requests
import pandas as pd
from io import StringIO
from datetime import datetime

# CREE LE DOSSIER + CONNECT A DB + CURSOR
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
os.makedirs(os.path.join(BASE_DIR, "data"), exist_ok=True)
conn = sqlite3.connect(os.path.join(BASE_DIR, "data/ventes.db"))
cursor = conn.cursor()


# FONCTIONS
# Enregistrement des erreur dans la table erreurs_import
def log_erreur(table_source, ligne, colonne, valeur, raison):
    cursor.execute("""
        INSERT INTO erreurs_import
        (table_source, ligne, colonne, valeur, raison, date_import)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (
        table_source,
        ligne,
        colonne,
        str(valeur),
        raison,
        datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ))


# CREATION DES TABLES DE BASE
# TABLE MAGASINS
cursor.execute("""
    CREATE TABLE IF NOT EXISTS magasins (
        id_magasin VARCHAR PRIMARY KEY,
        ville VARCHAR,
        nombre_de_salaries INTEGER
    )
""")

# TABLE PRODUITS
cursor.execute("""
    CREATE TABLE IF NOT EXISTS produits (
        id_reference_produit VARCHAR PRIMARY KEY,
        nom VARCHAR,
        prix DECIMAL(10,2),
        stock INTEGER
    )
""")

# TABLE VENTES
cursor.execute("""
    CREATE TABLE IF NOT EXISTS ventes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date DATETIME,
        id_reference_produit VARCHAR NOT NULL,
        quantite INTEGER,
        id_magasin VARCHAR,
        FOREIGN KEY (id_reference_produit) REFERENCES produits(id_reference_produit),
        FOREIGN KEY (id_magasin) REFERENCES magasins(id_magasin)
    )
""")

# CREATION DES TABLES POUR ANALYSE + ERREUR
# TABLE RESULTATS ANALYSES
cursor.execute("""
    CREATE TABLE IF NOT EXISTS resultats_analyses (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        type_analyse VARCHAR NOT NULL,
        label VARCHAR,
        valeur DECIMAL(10,2),
        date_calcul DATETIME
    )
""")

# TABLE ERREUR IMPORT
cursor.execute("""
    CREATE TABLE IF NOT EXISTS erreurs_import (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        table_source VARCHAR,
        ligne INTEGER,
        colonne VARCHAR,
        valeur VARCHAR,
        raison VARCHAR,
        date_import DATETIME
    )
""")
conn.commit()


# IMPORT DES DONNEES
URL_PRODUITS  = "https://docs.google.com/spreadsheets/d/e/2PACX-1vSawI56WBC64foMT9pKCiY594fBZk9Lyj8_bxfgmq-8ck_jw1Z49qDeMatCWqBxehEVoM6U1zdYx73V/pub?gid=0&single=true&output=csv"
URL_MAGASINS  = "https://docs.google.com/spreadsheets/d/e/2PACX-1vSawI56WBC64foMT9pKCiY594fBZk9Lyj8_bxfgmq-8ck_jw1Z49qDeMatCWqBxehEVoM6U1zdYx73V/pub?gid=714623615&single=true&output=csv"
URL_VENTES    = "https://docs.google.com/spreadsheets/d/e/2PACX-1vSawI56WBC64foMT9pKCiY594fBZk9Lyj8_bxfgmq-8ck_jw1Z49qDeMatCWqBxehEVoM6U1zdYx73V/pub?gid=760830694&single=true&output=csv"

response_produits = requests.get(URL_PRODUITS)
df_produits = pd.read_csv(StringIO(response_produits.content.decode("utf-8-sig")))

response_magasins = requests.get(URL_MAGASINS)
df_magasins = pd.read_csv(StringIO(response_magasins.content.decode("utf-8-sig")))

response_ventes = requests.get(URL_VENTES)
df_ventes = pd.read_csv(StringIO(response_ventes.content.decode('utf-8-sig')))


# NETTOYAGE
# Renommer les colonnes pour matcher la DB
df_produits = df_produits.rename(columns={
    "Nom" : "nom",
    "ID Référence produit" : "id_reference_produit",
    "Prix" : "prix",
    "Stock" : "stock"
})

df_magasins = df_magasins.rename(columns={
    "ID Magasin" : "id_magasin",
    "Ville" : "ville",
    "Nombre de salariés" : "nombre_de_salaries"
})

df_ventes = df_ventes.rename(columns={
    "Date" : "date",
    "ID Référence produit" : "id_reference_produit",
    "Quantité" : "quantite",
    "ID Magasin" : "id_magasin"
})

# pd.to_datetime() reconnaît automatiquement le format de date
df_ventes["date"] = pd.to_datetime(df_ventes["date"])


# df_produits
# NETTOYAGE
df_produits["nom"] = df_produits["nom"].str.strip().str.upper()
df_produits["id_reference_produit"] = df_produits["id_reference_produit"].str.strip().str.upper()

# VALIDATION + IMPORT
produits_inseres = 0
produits_ignores = 0

for index, row in df_produits.iterrows():
    if row["prix"] <= 0:
        log_erreur("produits", index, "prix", row["prix"], "prix <= 0")
        continue
    if row["stock"] < 0:
        log_erreur("produits", index, "stock", row["stock"], "stock négatif")
        continue
    cursor.execute(
        "SELECT id_reference_produit FROM produits WHERE id_reference_produit = ?",
        (row['id_reference_produit'],)
    )
    if cursor.fetchone() is not None:
        produits_ignores += 1
        continue
    try:
        cursor.execute("""
            INSERT INTO produits (id_reference_produit, nom, prix, stock)
            VALUES (?, ?, ?, ?)
        """, (row["id_reference_produit"], row["nom"], row["prix"], row["stock"]))
        produits_inseres += 1
    except Exception as e:
        log_erreur("produits", index, "inconnu", str(row.to_dict()), str(e))
        produits_ignores += 1

conn.commit()




# df_magasins
# NETTOYAGE
df_magasins["ville"] = df_magasins["ville"].str.strip().str.upper()
df_magasins["id_magasin"] = df_magasins["id_magasin"].astype(str).str.strip().str.upper()

# VALIDATION + IMPORT MAGASINS
magasins_inseres = 0
magasins_ignores = 0

for index, row in df_magasins.iterrows():
    if row["nombre_de_salaries"] <= 0:
        log_erreur("magasins", index, "nombre_de_salaries", row["nombre_de_salaries"], "nombre_de_salaries <= 0")
        continue
    cursor.execute(
        "SELECT id_magasin FROM magasins WHERE id_magasin = ?",
        (row['id_magasin'],)
    )
    if cursor.fetchone() is not None:
        magasins_ignores += 1
        continue
    try:
        cursor.execute("""
            INSERT INTO magasins (id_magasin, ville, nombre_de_salaries)
            VALUES (?, ?, ?)
        """, (row["id_magasin"], row["ville"], row["nombre_de_salaries"]))
        magasins_inseres += 1
    except Exception as e:
        log_erreur("magasins", index, "inconnu", str(row.to_dict()), str(e))
        magasins_ignores += 1

conn.commit()



# df_ventes
# NETTOYAGE
df_ventes["id_reference_produit"] = df_ventes["id_reference_produit"].str.strip().str.upper()
df_ventes["id_magasin"] = df_ventes["id_magasin"].astype(str).str.strip().str.upper()

# VALIDATION + IMPORT VENTES
ventes_inseres = 0
ventes_ignores = 0
refs_produits = set(df_produits["id_reference_produit"])
ids_magasins = set(df_magasins["id_magasin"])

for index, row in df_ventes.iterrows():
    if row["quantite"] <= 0:
        log_erreur("ventes", index, "quantite", row["quantite"], "quantite <= 0")
        continue
    if row["id_reference_produit"] not in refs_produits:
        log_erreur("ventes", index, "id_reference_produit", row["id_reference_produit"], "id_reference_produit inexistant dans produits")
        continue
    if row["id_magasin"] not in ids_magasins:
        log_erreur("ventes", index, "id_magasin", row["id_magasin"], "id_magasin inexistant dans magasins")
        continue
    cursor.execute("""
        SELECT id FROM ventes
        WHERE date = ?
        AND id_reference_produit = ?
        AND id_magasin = ?
        AND quantite = ?
    """, (str(row["date"]), row["id_reference_produit"], row["id_magasin"], row["quantite"]))
    if cursor.fetchone() is not None:
        ventes_ignores += 1
        continue
    try:
        cursor.execute("""
            INSERT INTO ventes (date, id_reference_produit, quantite, id_magasin)
            VALUES (?, ?, ?, ?)
        """, (str(row["date"]), row["id_reference_produit"], row["quantite"], row["id_magasin"]))
        ventes_inseres += 1
    except Exception as e:
        log_erreur("ventes", index, "inconnu", str(row.to_dict()), str(e))
        ventes_ignores += 1

conn.commit()


# On lit le fichier analyses.sql depuis le disque
with open(os.path.join(BASE_DIR, "analyses.sql"), "r", encoding="utf-8") as f:
    sql_brut = f.read()

# Sépare les 3 requêtes en les découpant sur ";"
requetes = [r.strip() for r in sql_brut.split(";") if r.strip()]

# Date du calcul
date_calcul = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# a) CHIFFRE D'AFFAIRES TOTAL
cursor.execute(requetes[0])
resultat_ca = cursor.fetchone()
ca_total = resultat_ca[0]

# On stocke le résultat dans resultats_analyses
cursor.execute("""
    INSERT INTO resultats_analyses (type_analyse, label, valeur, date_calcul)
    VALUES (?, ?, ?, ?)
""", ("ca_total", "Chiffre d'affaires global", ca_total, date_calcul))


# b) VENTES PAR PRODUIT
cursor.execute(requetes[1])
resultats_produits = cursor.fetchall()

# On stocke le résultat dans resultats_analyses
for row in resultats_produits:
    cursor.execute("""
        INSERT INTO resultats_analyses (type_analyse, label, valeur, date_calcul)
        VALUES (?, ?, ?, ?)
    """, ("par_produit", row[0], row[2], date_calcul))
    print(f"  → {row[0]} : {row[2]} €")


# c) VENTES PAR RÉGION
cursor.execute(requetes[2])
resultats_regions = cursor.fetchall()

# On stocke le résultat dans resultats_analyses
for row in resultats_regions:
    cursor.execute("""
        INSERT INTO resultats_analyses (type_analyse, label, valeur, date_calcul)
        VALUES (?, ?, ?, ?)
    """, ("par_region", row[0], row[2], date_calcul))
    print(f"  → {row[0]} : {row[2]} €")

conn.commit()

print("\n✅ Tous les résultats sont stockés dans resultats_analyses")