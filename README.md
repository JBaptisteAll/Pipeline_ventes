# Pipeline_ventes — Test Engineering Wild Code School x Simplon

Ce projet a été réalisé dans le cadre de la préparation à la journée de sélection pour le parcours **Data Engineer** (Wild Code School / Simplon).

L'objectif : construire un pipeline de données complet, de la collecte des données brutes jusqu'au stockage des résultats d'analyse, en passant par le nettoyage, la validation et l'import en base de données.

---

## Contexte

Un client souhaite analyser la dynamique de ses ventes sur 30 jours dans plusieurs villes de France. Il met à disposition trois fichiers CSV (produits, magasins, ventes) accessibles via des URLs HTTP.

---

## Les étapes du pipeline

### 1. Collecte des données
Les trois fichiers CSV sont récupérés directement depuis leurs URLs via des requêtes HTTP (`requests`), puis chargés en mémoire avec `pandas`. Aucun fichier n'est stocké localement — les données transitent directement vers la base.

### 2. Nettoyage
Avant tout import, les données sont normalisées :
- suppression des espaces superflus
- mise en majuscules des identifiants (`id_magasin`, `id_reference_produit`) pour garantir la cohérence des jointures
- conversion des dates au bon format avec `pd.to_datetime()`

### 3. Validation
Chaque ligne est vérifiée avant d'être insérée :
- prix > 0, stock ≥ 0, quantité > 0
- les clés étrangères de `ventes` doivent exister dans `produits` et `magasins`

Les lignes invalides ne sont pas supprimées — elles sont redirigées vers une **table de quarantaine** `erreurs_import` (voir section dédiée ci-dessous).

### 4. Déduplication
Pour les ventes, une vérification est effectuée avant chaque insertion : si une ligne avec la même date, le même produit, le même magasin et la même quantité existe déjà en base, elle est ignorée. Cela permet de relancer le pipeline sans créer de doublons.

### 5. Analyses SQL
Trois requêtes sont exécutées depuis le fichier `analyses.sql` :
- Chiffre d'affaires total
- Ventes par produit
- Ventes par région (ville)

Les résultats sont stockés dans la table `resultats_analyses` avec horodatage.

---

## Pourquoi une table `erreurs_import` ?

Plutôt que de simplement ignorer les données invalides ou d'arrêter le programme, j'ai fait le choix de les **tracer dans une table dédiée**.

Chaque erreur enregistre : la table concernée, le numéro de ligne, la colonne en cause, la valeur problématique, la raison du rejet et la date d'import.

Cela permet de :
- **ne pas perdre d'information** — une donnée rejetée aujourd'hui peut être corrigée et réimportée demain
- **auditer le pipeline** — on sait exactement ce qui a été rejeté et pourquoi
- **fiabiliser la base** — seules les données valides entrent dans les tables principales

C'est une bonne pratique courante en ingénierie des données, souvent appelée "quarantaine" ou "dead letter queue".

---

## Résultats d'analyse

### a) Chiffre d'affaires total
Le chiffre d'affaires total sur la période analysée s'élève à **5 268,78 €**.

### b) Ventes par produit
| Produit | CA (€) |
|---|---|
| Produit D | 1 679,79 € |
| Produit E | 1 399,65 € |
| Produit A | 1 199,76 € |
| Produit B | 539,73 € |
| Produit C | 449,85 € |

### c) Ventes par région
| Ville | CA (€) |
|---|---|
| Lyon | 1 059,79 € |
| Marseille | 1 009,73 € |
| Bordeaux | 829,81 € |
| Paris | 799,80 € |
| Nantes | 739,83 € |
| Strasbourg | 579,89 € |
| Lille | 249,93 € |

---

## Schéma de l'architecture

<p align="center">
  <img src="https://github.com/JBaptisteAll/Pipeline_ventes/blob/main/assets/01_schema_architecture_pipeline_ventes.drawio.png" alt="Schéma architecture pipeline" width="1000">
</p>

---

## Modèle Conceptuel de Données

<p align="center">
  <img src="https://github.com/JBaptisteAll/Pipeline_ventes/blob/main/assets/MCD_DB_ventes_PME.png" alt="MCD base de données" width="1000">
</p>

---

## Stack technique

| Outil | Rôle |
|---|---|
| Python 3.11 | Collecte, nettoyage, validation, import |
| pandas | Manipulation des données |
| SQLite | Base de données |
| Docker | Conteneurisation du service Python |
| Docker Compose | Orchestration des deux services |

---

## Lancer le projet

```bash
git clone https://github.com/JBaptisteAll/Pipeline_ventes.git
cd Pipeline_ventes
docker-compose up
```
