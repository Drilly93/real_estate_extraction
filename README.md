# Real Estate Data Acquisition & Price Prediction (Pipeline)

## Description du projet
**Objectif** : Construire un pipeline complet de traitement et d’enrichissement des données immobilières en France, depuis le téléchargement des données brutes jusqu’à la génération d’un dataset final prêt pour la modélisation prédictive des prix de l’immobilier en machine learning.

La pipeline intègre plusieurs sources de données, notamment les valeurs foncières (DVF), les points d’intérêt géographiques (OpenStreetMap), les indicateurs macro-économiques (Banque de France) et les statistiques socio-économiques (INSEE).

A partir des datasets, deux databases SQLite sont générées.

### Installation des dependances
pip install -r requirements.txt

## Ordre d’exécution des scripts

Executer sequentiellement les scripts suivants dans l’ordre indiqué pour reproduire le pipeline complet de traitement des données immobilières :

Dans le folder code_dvf: >>> cd code_dvf

>>> python telechargement_valeur_fonciere.py  

>>> python traitement_open_street_map.py  

>>> python traitement_economie_global.py  

>>> python fusion_vf_eco_insee.py  

>>> python fin_nettoyage.py

Dans le folder code_db: >>> cd code_db

>>> python create_dvf_database.py

>>> python create_deferla_database.py

( Optionnel : Etape de scraping Deferla a effectuer avant , cf voir_en_dessous)

## Fichier DVF generes : 

Tous les fichiers generes sont situes dans le dossier data/
Pour executer le pipeline complet, il faut garder l'arborescence presente dans le git ( et les fichiers associes ) :

**traitement_open_street_map.py** : 
-> ValeursFoncieresParDepartement
-> ValeursFoncieresParDepartement/<code_departement>
-> ValeursFoncieresParDepartement/<code_departement> / <code_departement>.csv
-> DataFrameFinal
-> DataFrameFinal/DataFrame_VF
-> DataFrameFinal/DataFrame_VF/df_vf.parquet

**traitement_open_street_map.py** :
-> OpenStreetMap/france-latest.osm.pbf
-> OpenStreetMap/OSM_triee
-> OpenStreetMap/OSM_triee/<POI>.parquet
-> DataFrameFinal
-> DataFrameFinal/DataFrame_VF+OSM
-> DataFrameFinal/DataFrame_VF+OSM/df_vf_oms.parquet

**traitement_economie_global.py** :
-> Economie_Globale/df_eco.csv

**fusion_vf_eco_insee.py**:
-> DataFrameFinal/DataFrame_VF+OSM+ECO+INSEE
-> DataFrameFinal/DataFrame_VF+OSM+ECO+INSEE/df_vf_oms_eco_insee.parquet

**fin_nettoyage.py** :
-> DataFrameFinal/df_final_propre.parquet
-> DataFrameFinal/df_final_propre_reduit.parquet ( version reduite pour envoie)


## Details des fichiers 

**config.py** : Fichier central de configuration contenant les chemins, constantes et emplacements des fichiers d’entrée/sortie utilisés par l’ensemble du projet. A REGARDER SI PROBLEME DE CHEMIN.

**telechargement_valeur_fonciere.py** : 
Télécharge automatiquement les données DVF (Demandes de Valeurs Foncières) par département depuis l’API data.gouv.fr, puis applique un premier nettoyage pour ne conserver que les ventes de maisons et d’appartements. Le script nettoie les champs numériques, crée des variables temporelles et calcule les prix au m².
Output : base DVF nettoyée au format Parquet (df_vf.parquet).

**traitement_open_street_map.py** : 
Extrait les points d’intérêt pertinents (transports, commerces, écoles, santé, espaces verts, etc.) à partir du fichier OpenStreetMap France (.pbf), puis nettoie les doublons spatiaux. Les transactions DVF sont ensuite enrichies par des variables géographiques telles que le nombre de POI dans un rayon donné et la distance minimale au POI le plus proche, grâce à des structures KD-Tree.
Output : base DVF enrichie géographiquement (df_vf_oms.parquet).

**traitement_economie_global.py** :
Nettoie et harmonise plusieurs séries macro-économiques (production de crédits immobiliers, taux de crédit, variation des encours, inflation) en format mensuel. Les différentes sources sont fusionnées après conversion des dates et filtrage temporel afin de produire un jeu de données économique cohérent.
Output : dataset macro-économique mensuel (df_eco.csv).

**fusion_vf_eco_insee.py** : 
Fusionne la base DVF enrichie par OSM avec les indicateurs macro-économiques mensuels ainsi que les statistiques socio-économiques de l’INSEE aux niveaux communal et départemental. Le script gère l’harmonisation des codes géographiques et des types de données pour produire une base complète et cohérente.
Output : dataset fusionné DVF + OSM + Économie + INSEE (df_vf_oms_eco_insee.parquet).

**fin_nettoyage.py** : Applique le nettoyage final du dataset en traitant les valeurs manquantes, en corrigeant les incohérences résiduelles et en supprimant les valeurs aberrantes de la valeur foncière. Le script effectue également des transformations utiles au machine learning (projection spatiale, logarithme des prix, encodage des variables catégorielles).
Output : dataset final propre et prêt pour la modélisation (df_final_propre.parquet).

Le dernier fichier `df_final_propre.parquet` est le dataset final prêt pour le machine learning.

**create_dvf_database.py** : Permet de générer la database **dvf_immobilier.db** en se basant sur les fonctions définies dans **dvf_database.py**  et sur le dataset **df_final_propre.parquet**

**create_deferla_database.py** : Permet de générer la database **deferla.db** en se basant sur les fonctions définies dans **deferla_database.py**  et sur le dataset **deferla.json**


## Folder : De FerlaReal 

Ce répertoire contient le module d'extraction de données pour l'agence immobilière **DeFerla**.

### Description du projet

Ce module a pour objectif de récupérer les annonces immobilières via **Scrapy**, en contournant les limitations techniques classiques.

### Méthodologie Technique

#### 1. Acquisition (API Reverse Engineering) : Le Saint Graal https://immobilier.altelis.com/deferla

* **Le Problème :** Le site utilisant un rendu dynamique (**JavaScript**), le scraping HTML classique est inefficace (page vide).
* **La Solution :** Ce script intercepte les flux XHR pour interroger directement l'API JSON du fournisseur (*Altelis*).
* **Avantage :** Récupération de données brutes, structurées et complètes.

#### 2. Traitement des images (Custom Pipeline)

* **Implémentation :** Utilisation d'une `ImagesPipeline` Scrapy personnalisée.
* **Performance :** Téléchargement asynchrone des visuels.
* **Organisation automatique :** Création d'un dossier par annonce (`ID_ANNONCE/`) et renommage séquentiel des fichiers pour une base de données propre.

#### 3. Conformité (RGPD)

* **Filtrage automatique :** Exclusion des données personnelles des agents (emails, téléphones directs) avant l'export des données.

---

### Installation

Le projet nécessite **Python 3.x**.

1.  Activer l'environnement virtuel.
2.  Installer les dépendances (Scrapy et Pillow pour le traitement d'images) :

```bash
pip install scrapy pillow
```

## Utilisation

Pour lancer l'extraction et générer le fichier de données :

```bash
cd .\deferla\immo_project\
scrapy crawl deferla -O results/deferla.json
```

## Structure des résultats

En sortie, le script génère :

* `results/deferla.json` : Contient les métadonnées des annonces.
* `images_data/` : Dossier contenant les images organisées.

**Arborescence de sortie :**

```text
/images_data
    ├── VA1980/
    │   ├── image_0.jpg
    │   └── image_1.jpg
    ├── VA2042/
    │   └── ...
```

## SOURCES & REFERENCES

Valeurs Foncières (DVF) :
- Departements : https://www.data.gouv.fr/datasets/departements-de-france
- Data Gouv : https://dvf-api.data.gouv.fr/dvf/csv/?dep=1

OpenStreetMap (OSM) :
- Open Street Map : https://download.geofabrik.de/europe/france.html

INSEE : 
- Niveau de vie : https://www.insee.fr/fr/statistiques?debut=0&theme=1

Economie Globale :
- Banque de France : https://www.banque-france.fr/statistiques/telechargement

Site d'annonces immobilières :
- Deferla : https://immobilier.altelis.com/deferla