"""
DATABASE DVF (Demandes de Valeurs Foncières)

1. Create a SQLite database with a normalised schema
2. Insert data from a polars dataframe
3. Explore the database

SCHEMA :
    DEPARTEMENTS (1) ──→ COMMUNES (N)
                              │
                              ▼ 1:N
    INDICATEURS_ECO (1) ──→ MUTATIONS ←── BIENS (1:N)
                                              │
                                              ▼ 1:1
                                         PROXIMITE

"""

import sqlite3
from pathlib import Path



# SQL SCHEMA


SQL_SCHEMA = """
-- ============================================
-- SCHÉMA RELATIONNEL DVF (Demandes de Valeurs Foncières)
-- Base de données SQLite
-- ============================================

-- Suppression des tables existantes (dans l'ordre des dépendances)
DROP TABLE IF EXISTS mutations;
DROP TABLE IF EXISTS proximite;
DROP TABLE IF EXISTS biens;
DROP TABLE IF EXISTS indicateurs_economiques;
DROP TABLE IF EXISTS communes;
DROP TABLE IF EXISTS departements;

-- ============================================
-- TABLE: departements
-- Données socio-économiques par département
-- ============================================
CREATE TABLE departements (
    code_departement TEXT PRIMARY KEY,
    nb_menages_2021 INTEGER,
    revenu_median_2021 REAL,
    taux_chomage_2023 REAL,
    salaire_net_horaire_moyen_2022 REAL
);

CREATE INDEX idx_dept_revenu ON departements(revenu_median_2021);

-- ============================================
-- TABLE: communes
-- Données démographiques par commune
-- ============================================
CREATE TABLE communes (
    code_commune TEXT PRIMARY KEY,
    code_departement TEXT NOT NULL,
    nb_menages_2021 INTEGER,
    revenu_median_2021 REAL,
    FOREIGN KEY (code_departement) REFERENCES departements(code_departement)
);

CREATE INDEX idx_commune_dept ON communes(code_departement);
CREATE INDEX idx_commune_revenu ON communes(revenu_median_2021);

-- ============================================
-- TABLE: indicateurs_economiques
-- Données macroéconomiques temporelles
-- ============================================
CREATE TABLE indicateurs_economiques (
    date_indicateur DATE PRIMARY KEY,
    credits_habitat_hors_renegociations REAL,
    taux_hors_renegociations REAL,
    variations_encours_mensuelles_cvs REAL,
    ipc REAL
);

CREATE INDEX idx_indic_date ON indicateurs_economiques(date_indicateur);

-- ============================================
-- TABLE: biens
-- Caractéristiques physiques des biens immobiliers
-- ============================================
CREATE TABLE biens (
    id_bien INTEGER PRIMARY KEY AUTOINCREMENT,
    id_parcelle TEXT,
    type_local TEXT CHECK(type_local IN ('Maison', 'Appartement', 'Dépendance', 'Local industriel. commercial ou assimilé')),
    surface_reelle_bati REAL,
    surface_terrain REAL,
    longitude REAL,
    latitude REAL,
    x_proj REAL,
    y_proj REAL,
    type_local__Maison INTEGER DEFAULT 0,
    type_local__Appartement INTEGER DEFAULT 0
);

CREATE INDEX idx_bien_parcelle ON biens(id_parcelle);
CREATE INDEX idx_bien_type ON biens(type_local);
CREATE INDEX idx_bien_surface ON biens(surface_reelle_bati);
CREATE INDEX idx_bien_coords ON biens(longitude, latitude);

-- ============================================
-- TABLE: proximite
-- Indicateurs de proximité aux services/infrastructures
-- ============================================
CREATE TABLE proximite (
    id_proximite INTEGER PRIMARY KEY AUTOINCREMENT,
    id_bien INTEGER NOT NULL,
    -- Gares
    nb_gares INTEGER,
    distance_min_gares REAL,
    distance_min_gares_manquante INTEGER DEFAULT 0,
    -- Commerces
    nb_commerces INTEGER,
    distance_min_commerces REAL,
    distance_min_commerces_manquante INTEGER DEFAULT 0,
    -- Education
    nb_education INTEGER,
    distance_min_education REAL,
    distance_min_education_manquante INTEGER DEFAULT 0,
    -- Espaces verts
    nb_espaces_verts INTEGER,
    distance_min_espaces_verts REAL,
    distance_min_espaces_verts_manquante INTEGER DEFAULT 0,
    -- Santé
    nb_sante INTEGER,
    distance_min_sante REAL,
    distance_min_sante_manquante INTEGER DEFAULT 0,
    -- Pharmacies
    nb_pharmacies INTEGER,
    distance_min_pharmacies REAL,
    distance_min_pharmacies_manquante INTEGER DEFAULT 0,
    -- Aéroports
    nb_aeroports INTEGER,
    distance_min_aeroports REAL,
    distance_min_aeroports_manquante INTEGER DEFAULT 0,
    -- Routes principales
    nb_routes_principales INTEGER,
    distance_min_routes_principales REAL,
    distance_min_routes_principales_manquante INTEGER DEFAULT 0,
    -- Industries
    nb_industries INTEGER,
    distance_min_industries REAL,
    distance_min_industries_manquante INTEGER DEFAULT 0,
    FOREIGN KEY (id_bien) REFERENCES biens(id_bien)
);

CREATE INDEX idx_prox_bien ON proximite(id_bien);

-- ============================================
-- TABLE: mutations
-- Transactions immobilières (table principale)
-- ============================================
CREATE TABLE mutations (
    id_mutation INTEGER PRIMARY KEY AUTOINCREMENT,
    date_mutation DATE NOT NULL,
    valeur_fonciere REAL,
    valeur_fonciere_log REAL,
    nature_mutation TEXT,
    id_bien INTEGER NOT NULL,
    code_commune TEXT NOT NULL,
    prix_par_m2_habitable REAL,
    prix_par_m2_terrain REAL,
    FOREIGN KEY (id_bien) REFERENCES biens(id_bien),
    FOREIGN KEY (code_commune) REFERENCES communes(code_commune),
    FOREIGN KEY (date_mutation) REFERENCES indicateurs_economiques(date_indicateur)
);

CREATE INDEX idx_mut_date ON mutations(date_mutation);
CREATE INDEX idx_mut_valeur ON mutations(valeur_fonciere);
CREATE INDEX idx_mut_bien ON mutations(id_bien);
CREATE INDEX idx_mut_commune ON mutations(code_commune);
CREATE INDEX idx_mut_nature ON mutations(nature_mutation);

-- ============================================
-- VUES UTILES
-- ============================================

-- Vue complète des mutations avec toutes les informations jointes
CREATE VIEW vue_mutations_complete AS
SELECT 
    m.id_mutation,
    m.date_mutation,
    m.valeur_fonciere,
    m.valeur_fonciere_log,
    m.nature_mutation,
    m.prix_par_m2_habitable,
    m.prix_par_m2_terrain,
    -- Bien
    b.id_parcelle,
    b.type_local,
    b.surface_reelle_bati,
    b.surface_terrain,
    b.longitude,
    b.latitude,
    -- Commune
    c.code_commune,
    c.nb_menages_2021 AS nb_menages_commune,
    c.revenu_median_2021 AS revenu_median_commune,
    -- Département
    d.code_departement,
    d.nb_menages_2021 AS nb_menages_departement,
    d.revenu_median_2021 AS revenu_median_departement,
    d.taux_chomage_2023,
    d.salaire_net_horaire_moyen_2022,
    -- Indicateurs économiques
    i.credits_habitat_hors_renegociations,
    i.taux_hors_renegociations,
    i.ipc
FROM mutations m
JOIN biens b ON m.id_bien = b.id_bien
JOIN communes c ON m.code_commune = c.code_commune
JOIN departements d ON c.code_departement = d.code_departement
LEFT JOIN indicateurs_economiques i ON m.date_mutation = i.date_indicateur;

-- Vue des statistiques par commune
CREATE VIEW vue_stats_commune AS
SELECT 
    c.code_commune,
    c.code_departement,
    COUNT(m.id_mutation) AS nb_transactions,
    AVG(m.valeur_fonciere) AS prix_moyen,
    AVG(m.prix_par_m2_habitable) AS prix_m2_moyen,
    MIN(m.valeur_fonciere) AS prix_min,
    MAX(m.valeur_fonciere) AS prix_max
FROM communes c
LEFT JOIN mutations m ON c.code_commune = m.code_commune
GROUP BY c.code_commune, c.code_departement;

-- Vue des statistiques par département
CREATE VIEW vue_stats_departement AS
SELECT 
    d.code_departement,
    d.revenu_median_2021,
    d.taux_chomage_2023,
    COUNT(m.id_mutation) AS nb_transactions,
    AVG(m.valeur_fonciere) AS prix_moyen,
    AVG(m.prix_par_m2_habitable) AS prix_m2_moyen
FROM departements d
LEFT JOIN communes c ON d.code_departement = c.code_departement
LEFT JOIN mutations m ON c.code_commune = m.code_commune
GROUP BY d.code_departement;
"""


# DATABASE CREATION

def create_database(db_path: str = "dvf_immobilier.db") -> str:
    """
    Crée la base de données SQLite avec le schéma défini.
    
    Cette fonction :
    1. Crée un fichier SQLite (ou le remplace s'il existe)
    2. Exécute le schéma SQL pour créer toutes les tables
    3. Crée les index pour optimiser les requêtes
    4. Crée les vues pour faciliter l'exploration
    
    Parameters
    ----------
    db_path : str
        Chemin vers le fichier de base de données à créer
        Exemple: "dvf_immobilier.db" ou "/chemin/vers/ma_base.db"
        
    Returns
    -------
    str
        Chemin vers la base de données créée
        
    Example
    -------
    >>> create_database("ma_base.db")
    ✓ Base de données créée : ma_base.db
    'ma_base.db'
    """
    # Connexion (crée le fichier s'il n'existe pas)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Exécution du schéma SQL complet
    cursor.executescript(SQL_SCHEMA)
    
    # Sauvegarde et fermeture
    conn.commit()
    conn.close()
    
    print(f"✓ Base de données créée : {db_path}")
    return db_path



# DATA ADD FROM POLARS

def insert_data_from_polars(df, db_path: str = "dvf_immobilier.db", batch_size: int = 10000):
    """
    Insère les données du DataFrame Polars dans la base SQLite normalisée.
    
    Cette fonction :
    1. Extrait les données uniques pour les tables de référence
    2. Insère les départements, communes, indicateurs économiques
    3. Insère les biens, proximités et mutations par lots
    
    Parameters
    ----------
    df : pl.DataFrame
        Le DataFrame Polars contenant les données DVF enrichies
        Doit contenir toutes les colonnes du dataset original
        
    db_path : str
        Chemin vers la base de données SQLite
        
    batch_size : int
        Taille des lots pour l'insertion (défaut: 10000)
        Augmenter pour plus de vitesse, diminuer si problèmes de mémoire

    """
    import polars as pl
    
    # Import optionnel de tqdm pour la barre de progression
    try:
        from tqdm import tqdm
        use_tqdm = True
    except ImportError:
        use_tqdm = False
        print("  (Installez tqdm pour avoir une barre de progression: pip install tqdm)")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Activer les foreign keys
    cursor.execute("PRAGMA foreign_keys = ON;")
    
    print("Insertion des données...")
    
    # DÉPARTEMENTS (données uniques)

    print("  → Départements...")
    dept_df = df.select([
        "code_departement",
        "nb_menages_2021_departement",
        "revenu_median_2021_departement",
        "taux_chomage_2023_departement",
        "salaire_net_horaire_moyen_2022_departement"
    ]).unique(subset=["code_departement"]).drop_nulls(subset=["code_departement"])
    
    for row in dept_df.iter_rows():
        cursor.execute("""
            INSERT OR IGNORE INTO departements 
            (code_departement, nb_menages_2021, revenu_median_2021, 
             taux_chomage_2023, salaire_net_horaire_moyen_2022)
            VALUES (?, ?, ?, ?, ?)
        """, row)
    
    conn.commit()
    print(f"      {dept_df.height} départements insérés")
    
    # COMMUNES (données uniques)
    print("  → Communes...")
    commune_df = df.select([
        "code_commune",
        "code_departement",
        "nb_menages_2021_commune",
        "revenu_median_2021_commune"
    ]).unique(subset=["code_commune"]).drop_nulls(subset=["code_commune"])
    
    for row in commune_df.iter_rows():
        cursor.execute("""
            INSERT OR IGNORE INTO communes 
            (code_commune, code_departement, nb_menages_2021, revenu_median_2021)
            VALUES (?, ?, ?, ?)
        """, row)
    
    conn.commit()
    print(f"      {commune_df.height} communes insérées")

    # INDICATEURS ÉCONOMIQUES
    print("  → Indicateurs économiques...")
    indic_df = df.select([
        "date_mutation",
        "Crédits à l'habitat hors renégociations",
        "Taux hors renégociations",
        "Variations d'encours mensuelles cvs",
        "IPC"
    ]).unique(subset=["date_mutation"]).drop_nulls(subset=["date_mutation"])
    
    for row in indic_df.iter_rows():
        cursor.execute("""
            INSERT OR IGNORE INTO indicateurs_economiques 
            (date_indicateur, credits_habitat_hors_renegociations, 
             taux_hors_renegociations, variations_encours_mensuelles_cvs, ipc)
            VALUES (?, ?, ?, ?, ?)
        """, (str(row[0]), row[1], row[2], row[3], row[4]))
    
    conn.commit()
    print(f"      {indic_df.height} dates d'indicateurs insérées")
    
    # BIENS, PROXIMITÉ et MUTATIONS

    print("  → Biens, Proximité et Mutations...")
    
    total_rows = len(df)
    
    # Créer l'itérateur avec ou sans tqdm
    if use_tqdm:
        iterator = tqdm(range(0, total_rows, batch_size), desc="      Batches")
    else:
        iterator = range(0, total_rows, batch_size)
        print(f"      Total: {total_rows} lignes, {(total_rows // batch_size) + 1} batches")
    
    for i in iterator:
        batch = df.slice(i, batch_size)
        
        for row in batch.iter_rows(named=True):
            # -----------------------------------------------------------------
            # Insérer le bien
            # -----------------------------------------------------------------
            cursor.execute("""
                INSERT INTO biens 
                (id_parcelle, type_local, surface_reelle_bati, surface_terrain,
                 longitude, latitude, x_proj, y_proj, 
                 type_local__Maison, type_local__Appartement)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                row["id_parcelle"],
                row["type_local"],
                row["surface_reelle_bati"],
                row["surface_terrain"],
                float(row["longitude"]) if row["longitude"] else None,
                float(row["latitude"]) if row["latitude"] else None,
                row["x_proj"],
                row["y_proj"],
                row["type_local__Maison"],
                row["type_local__Appartement"]
            ))
            
            id_bien = cursor.lastrowid
            
            # -----------------------------------------------------------------
            # Insérer la proximité
            # -----------------------------------------------------------------
            cursor.execute("""
                INSERT INTO proximite 
                (id_bien, nb_gares, distance_min_gares, distance_min_gares_manquante,
                 nb_commerces, distance_min_commerces, distance_min_commerces_manquante,
                 nb_education, distance_min_education, distance_min_education_manquante,
                 nb_espaces_verts, distance_min_espaces_verts, distance_min_espaces_verts_manquante,
                 nb_sante, distance_min_sante, distance_min_sante_manquante,
                 nb_pharmacies, distance_min_pharmacies, distance_min_pharmacies_manquante,
                 nb_aeroports, distance_min_aeroports, distance_min_aeroports_manquante,
                 nb_routes_principales, distance_min_routes_principales, distance_min_routes_principales_manquante,
                 nb_industries, distance_min_industries, distance_min_industries_manquante)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                id_bien,
                row["nb_gares"], row["distance_min_gares"], row["distance_min_gares_manquante"],
                row["nb_commerces"], row["distance_min_commerces"], row["distance_min_commerces_manquante"],
                row["nb_education"], row["distance_min_education"], row["distance_min_education_manquante"],
                row["nb_espaces_verts"], row["distance_min_espaces_verts"], row["distance_min_espaces_verts_manquante"],
                row["nb_sante"], row["distance_min_sante"], row["distance_min_sante_manquante"],
                row["nb_pharmacies"], row["distance_min_pharmacies"], row["distance_min_pharmacies_manquante"],
                row["nb_aeroports"], row["distance_min_aeroports"], row["distance_min_aeroports_manquante"],
                row["nb_routes_principales"], row["distance_min_routes_principales"], row["distance_min_routes_principales_manquante"],
                row["nb_industries"], row["distance_min_industries"], row["distance_min_industries_manquante"]
            ))
            
            # -----------------------------------------------------------------
            # Insérer la mutation
            # -----------------------------------------------------------------
            cursor.execute("""
                INSERT INTO mutations 
                (date_mutation, valeur_fonciere, valeur_fonciere_log, nature_mutation,
                 id_bien, code_commune, prix_par_m2_habitable, prix_par_m2_terrain)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                str(row["date_mutation"]),
                row["valeur_fonciere"],
                row["valeur_fonciere_log"],
                row["nature_mutation"],
                id_bien,
                row["code_commune"],
                row["prix_par_m2_habitable"],
                row["prix_par_m2_terrain"]
            ))
        
        conn.commit()
        
        # Afficher la progression si pas de tqdm
        if not use_tqdm:
            progress = min(i + batch_size, total_rows)
            print(f"      Progression: {progress}/{total_rows} ({100*progress/total_rows:.1f}%)")
    
    conn.close()
    print(f"✓ {total_rows} enregistrements insérés dans {db_path}")

# Exploration functions

def get_database_info(db_path: str = "dvf_immobilier.db") -> dict:
    """
    Retourne les informations sur la structure de la base de données.
    
    Parameters
    ----------
    db_path : str
        Chemin vers la base de données
        
    Returns
    -------
    dict
        Informations sur les tables, vues et leurs colonnes
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    info = {"tables": {}, "views": {}}
    
    # Tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = [row[0] for row in cursor.fetchall()]
    
    for table in tables:
        cursor.execute(f"PRAGMA table_info({table})")
        columns = [(row[1], row[2]) for row in cursor.fetchall()]
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        info["tables"][table] = {"columns": columns, "count": count}
    
    # Vues
    cursor.execute("SELECT name FROM sqlite_master WHERE type='view' ORDER BY name")
    views = [row[0] for row in cursor.fetchall()]
    info["views"] = views
    
    conn.close()
    return info


def print_database_stats(db_path: str = "dvf_immobilier.db"):
    """
    Affiche les statistiques de la base de données.
    
    Parameters
    ----------
    db_path : str
        Chemin vers la base de données
    """
    info = get_database_info(db_path)
    
    print("\n" + "="*60)
    print(f"STATISTIQUES DE LA BASE : {db_path}")
    print("="*60)
    
    print("\nTABLES:")
    for table, data in info["tables"].items():
        print(f"  - {table}: {data['count']:,} enregistrements")
    
    print("\nVUES DISPONIBLES:")
    for view in info["views"]:
        print(f"  - {view}")
    
    print("\n" + "="*60)


def explore_table(db_path: str, table_name: str, limit: int = 5):
    """
    Affiche un aperçu d'une table.
    
    Parameters
    ----------
    db_path : str
        Chemin vers la base de données
    table_name : str
        Nom de la table à explorer
    limit : int
        Nombre de lignes à afficher
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Colonnes
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [row[1] for row in cursor.fetchall()]
    
    # Données
    cursor.execute(f"SELECT * FROM {table_name} LIMIT {limit}")
    rows = cursor.fetchall()
    
    conn.close()
    
    print(f"\nTable: {table_name}")
    print(f"Colonnes: {', '.join(columns)}")
    print(f"Aperçu ({limit} premières lignes):")
    for row in rows:
        print(f"  {row}")


def run_query(db_path: str, query: str):
    """
    Exécute une requête SQL et retourne les résultats.
    
    Parameters
    ----------
    db_path : str
        Chemin vers la base de données
    query : str
        Requête SQL à exécuter
        
    Returns
    -------
    tuple
        (colonnes, résultats)
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description] if cursor.description else []
    conn.close()
    return columns, results


# =============================================================================
# QUERIES EXAMPLES
# =============================================================================

def run_example_queries(db_path: str = "dvf_immobilier.db"):
    """
    Exécute des exemples de requêtes sur la base de données.
    
    Parameters
    ----------
    db_path : str
        Chemin vers la base de données
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("\n" + "="*60)
    print("EXEMPLES DE REQUÊTES")
    print("="*60)
    
    # Exemple 1: Prix moyen par type de bien
    print("\n1. Prix moyen par type de bien:")
    cursor.execute("""
        SELECT 
            b.type_local, 
            COUNT(*) AS nb_ventes,
            ROUND(AVG(m.valeur_fonciere), 2) AS prix_moyen,
            ROUND(AVG(m.prix_par_m2_habitable), 2) AS prix_m2_moyen
        FROM mutations m
        JOIN biens b ON m.id_bien = b.id_bien
        GROUP BY b.type_local
        ORDER BY prix_moyen DESC
    """)
    for row in cursor.fetchall():
        print(f"   {row}")
    
    # Exemple 2: Top 5 départements les plus chers
    print("\n2. Top 5 départements les plus chers (prix/m2):")
    cursor.execute("""
        SELECT 
            d.code_departement,
            COUNT(*) AS nb_ventes,
            ROUND(AVG(m.prix_par_m2_habitable), 2) AS prix_m2_moyen
        FROM mutations m
        JOIN communes c ON m.code_commune = c.code_commune
        JOIN departements d ON c.code_departement = d.code_departement
        WHERE m.prix_par_m2_habitable IS NOT NULL
        GROUP BY d.code_departement
        ORDER BY prix_m2_moyen DESC
        LIMIT 5
    """)
    for row in cursor.fetchall():
        print(f"   {row}")
    
    # Exemple 3: Évolution par année
    print("\n3. Evolution du prix moyen par annee:")
    cursor.execute("""
        SELECT 
            strftime('%Y', date_mutation) AS annee,
            COUNT(*) AS nb_ventes,
            ROUND(AVG(valeur_fonciere), 2) AS prix_moyen
        FROM mutations
        GROUP BY annee
        ORDER BY annee
    """)
    for row in cursor.fetchall():
        print(f"   {row}")
    
    # Exemple 4: Corrélation avec les indicateurs économiques
    print("\n4. Prix moyen par niveau de taux d'interet:")
    cursor.execute("""
        SELECT 
            CASE 
                WHEN i.taux_hors_renegociations < 2 THEN 'Taux < 2%'
                WHEN i.taux_hors_renegociations < 3 THEN 'Taux 2-3%'
                WHEN i.taux_hors_renegociations < 4 THEN 'Taux 3-4%'
                ELSE 'Taux >= 4%'
            END AS tranche_taux,
            COUNT(*) AS nb_ventes,
            ROUND(AVG(m.valeur_fonciere), 2) AS prix_moyen
        FROM mutations m
        JOIN indicateurs_economiques i ON m.date_mutation = i.date_indicateur
        GROUP BY tranche_taux
        ORDER BY tranche_taux
    """)
    for row in cursor.fetchall():
        print(f"   {row}")
    
    conn.close()
