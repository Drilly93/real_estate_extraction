"""
==============================================================================
DATABASE DEFERLA (Annonces Immobilières)
==============================================================================

1. Create a SQLite database with a normalised schema
2. Insert data from a json file
3. Explore the database

SCHEMA :
    ANNONCES (1) ──→ DIAGNOSTICS (1)
        │
        ▼ 1:N
     IMAGES
"""

import sqlite3
import json
from pathlib import Path


# SCHÉMA SQL

SQL_SCHEMA = """
-- ============================================
-- SCHÉMA RELATIONNEL DEFERLA (Annonces Immobilières)
-- Base de données SQLite
-- ============================================

-- Suppression des tables existantes (dans l'ordre des dépendances)
DROP TABLE IF EXISTS images;
DROP TABLE IF EXISTS diagnostics;
DROP TABLE IF EXISTS annonces;

-- ============================================
-- TABLE: annonces
-- Informations principales des biens immobiliers
-- ============================================
CREATE TABLE annonces (
    id TEXT PRIMARY KEY,
    url TEXT,
    date_publication DATE,
    type TEXT CHECK(type IN ('Appartement', 'Maison', 'Maison de ville', 'Duplex', 'Loft', 'Chalet', 'Terrain', 'Parking', 'Box', 'Commerce', 'Immeuble')),
    titre TEXT,
    prix INTEGER,
    honoraires INTEGER,
    surface REAL,
    pieces INTEGER,
    chambres INTEGER,
    etage INTEGER,
    ascenseur INTEGER DEFAULT 0,
    ville TEXT,
    code_postal TEXT,
    latitude REAL,
    longitude REAL,
    etat TEXT CHECK(etat IN ('Neuf', 'Excellent état', 'Bon état', 'À rafraîchir', 'À rénover', NULL)),
    salle_de_bain INTEGER,
    chauffage_type TEXT,
    chauffe_eau TEXT,
    exposition TEXT,
    charges_annuelles REAL,
    description TEXT,
    image_principale TEXT
);

CREATE INDEX idx_annonce_date ON annonces(date_publication);
CREATE INDEX idx_annonce_type ON annonces(type);
CREATE INDEX idx_annonce_prix ON annonces(prix);
CREATE INDEX idx_annonce_surface ON annonces(surface);
CREATE INDEX idx_annonce_ville ON annonces(ville);
CREATE INDEX idx_annonce_cp ON annonces(code_postal);
CREATE INDEX idx_annonce_etat ON annonces(etat);
CREATE INDEX idx_annonce_coords ON annonces(latitude, longitude);

-- ============================================
-- TABLE: diagnostics
-- Diagnostics énergétiques (DPE et GES)
-- ============================================
CREATE TABLE diagnostics (
    id_diagnostic INTEGER PRIMARY KEY AUTOINCREMENT,
    id_annonce TEXT NOT NULL UNIQUE,
    dpe_valeur REAL,
    dpe_lettre TEXT CHECK(dpe_lettre IN ('A', 'B', 'C', 'D', 'E', 'F', 'G', 'Not applicable', 'In progress', NULL)),
    ges_valeur REAL,
    ges_lettre TEXT CHECK(ges_lettre IN ('A', 'B', 'C', 'D', 'E', 'F', 'G', 'Not applicable', 'In progress', NULL)),
    FOREIGN KEY (id_annonce) REFERENCES annonces(id)
);

CREATE INDEX idx_diag_annonce ON diagnostics(id_annonce);
CREATE INDEX idx_diag_dpe ON diagnostics(dpe_lettre);
CREATE INDEX idx_diag_ges ON diagnostics(ges_lettre);

-- ============================================
-- TABLE: images
-- URLs des images des annonces
-- ============================================
CREATE TABLE images (
    id_image INTEGER PRIMARY KEY AUTOINCREMENT,
    id_annonce TEXT NOT NULL,
    url_image TEXT NOT NULL,
    est_principale INTEGER DEFAULT 0,
    FOREIGN KEY (id_annonce) REFERENCES annonces(id)
);

CREATE INDEX idx_image_annonce ON images(id_annonce);

-- ============================================
-- VUES UTILES
-- ============================================

-- Vue complète des annonces avec diagnostics
CREATE VIEW vue_annonces_complete AS
SELECT 
    a.id,
    a.url,
    a.date_publication,
    a.type,
    a.titre,
    a.prix,
    a.honoraires,
    a.surface,
    a.pieces,
    a.chambres,
    a.etage,
    a.ascenseur,
    a.ville,
    a.code_postal,
    a.latitude,
    a.longitude,
    a.etat,
    a.salle_de_bain,
    a.chauffage_type,
    a.chauffe_eau,
    a.exposition,
    a.charges_annuelles,
    a.image_principale,
    -- Diagnostics
    d.dpe_valeur,
    d.dpe_lettre,
    d.ges_valeur,
    d.ges_lettre,
    -- Calculs
    ROUND(a.prix / a.surface, 2) AS prix_m2
FROM annonces a
LEFT JOIN diagnostics d ON a.id = d.id_annonce;

-- Vue des statistiques par ville
CREATE VIEW vue_stats_ville AS
SELECT 
    ville,
    code_postal,
    COUNT(id) AS nb_annonces,
    ROUND(AVG(prix), 2) AS prix_moyen,
    ROUND(AVG(prix / surface), 2) AS prix_m2_moyen,
    ROUND(AVG(surface), 2) AS surface_moyenne,
    MIN(prix) AS prix_min,
    MAX(prix) AS prix_max
FROM annonces
WHERE surface > 0
GROUP BY ville, code_postal
ORDER BY nb_annonces DESC;

-- Vue des statistiques par type de bien
CREATE VIEW vue_stats_type AS
SELECT 
    type,
    COUNT(id) AS nb_annonces,
    ROUND(AVG(prix), 2) AS prix_moyen,
    ROUND(AVG(prix / surface), 2) AS prix_m2_moyen,
    ROUND(AVG(surface), 2) AS surface_moyenne,
    ROUND(AVG(pieces), 1) AS pieces_moyenne
FROM annonces
WHERE surface > 0
GROUP BY type
ORDER BY nb_annonces DESC;

-- Vue des statistiques par DPE
CREATE VIEW vue_stats_dpe AS
SELECT 
    d.dpe_lettre,
    COUNT(a.id) AS nb_annonces,
    ROUND(AVG(a.prix), 2) AS prix_moyen,
    ROUND(AVG(a.prix / a.surface), 2) AS prix_m2_moyen
FROM annonces a
JOIN diagnostics d ON a.id = d.id_annonce
WHERE d.dpe_lettre IS NOT NULL 
  AND d.dpe_lettre NOT IN ('Not applicable', 'In progress')
  AND a.surface > 0
GROUP BY d.dpe_lettre
ORDER BY d.dpe_lettre;
"""



# DATABASE CREATION

def create_database(db_path: str = "deferla.db") -> str:
    """
    Crée la base de données SQLite avec le schéma défini.
    
    Parameters
    ----------
    db_path : str
        Chemin vers le fichier de base de données à créer
        
    Returns
    -------
    str
        Chemin vers la base de données créée
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.executescript(SQL_SCHEMA)
    conn.commit()
    conn.close()
    
    print(f"✓ Base de données créée : {db_path}")
    return db_path


# INSERTION DES DONNÉES DEPUIS JSON

def insert_data_from_json(json_path: str, db_path: str = "deferla.db"):
    """
    Insère les données du fichier JSON dans la base SQLite.
    
    Parameters
    ----------
    json_path : str
        Chemin vers le fichier JSON contenant les annonces
        
    db_path : str
        Chemin vers la base de données SQLite
    """
    # Charger les données JSON
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Activer les foreign keys
    cursor.execute("PRAGMA foreign_keys = ON;")
    
    print(f"Insertion de {len(data)} annonces...")
    
    for item in data:
        # Convertir charges_annuelles en float si présent
        charges = None
        if item.get("charges_annuelles"):
            try:
                charges = float(item["charges_annuelles"])
            except (ValueError, TypeError):
                charges = None
        
        # Gérer le cas où prix est un dictionnaire
        prix = item.get("prix")
        if isinstance(prix, dict):
            # Essayer d'extraire la valeur ou la commission
            prix = prix.get("value") or prix.get("commission")
        
        # Gérer le cas où honoraires est un dictionnaire
        honoraires = item.get("honoraires")
        if isinstance(honoraires, dict):
            honoraires = honoraires.get("value")
        
        # -----------------------------------------------------------------
        # Insérer l'annonce
        # -----------------------------------------------------------------
        cursor.execute("""
            INSERT OR REPLACE INTO annonces 
            (id, url, date_publication, type, titre, prix, honoraires,
             surface, pieces, chambres, etage, ascenseur, ville, code_postal,
             latitude, longitude, etat, salle_de_bain, chauffage_type,
             chauffe_eau, exposition, charges_annuelles, description, image_principale)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            item["id"],
            item["url"],
            item["date_publication"],
            item["type"],
            item["titre"],
            prix,
            honoraires,
            item["surface"],
            item["pieces"],
            item["chambres"],
            item["etage"],
            1 if item["ascenseur"] else 0,
            item["ville"],
            item["code_postal"],
            item["latitude"],
            item["longitude"],
            item["etat"],
            item["salle_de_bain"],
            item["chauffage_type"],
            item["chauffe_eau"],
            item["exposition"],
            charges,
            item["description"],
            item["image_principale"]
        ))
        
        # -----------------------------------------------------------------
        # Insérer les diagnostics
        # -----------------------------------------------------------------
        cursor.execute("""
            INSERT OR REPLACE INTO diagnostics 
            (id_annonce, dpe_valeur, dpe_lettre, ges_valeur, ges_lettre)
            VALUES (?, ?, ?, ?, ?)
        """, (
            item["id"],
            item["dpe_valeur"],
            item["dpe_lettre"],
            item["ges_valeur"],
            item["ges_lettre"]
        ))
        
        # -----------------------------------------------------------------
        # Insérer les images
        # -----------------------------------------------------------------
        # Image principale
        if item["image_principale"]:
            cursor.execute("""
                INSERT INTO images (id_annonce, url_image, est_principale)
                VALUES (?, ?, 1)
            """, (item["id"], item["image_principale"]))
        
        # Autres images
        for img_url in item.get("image_urls", []):
            if img_url:
                cursor.execute("""
                    INSERT INTO images (id_annonce, url_image, est_principale)
                    VALUES (?, ?, 0)
                """, (item["id"], img_url))
    
    conn.commit()
    conn.close()
    
    print(f"✓ {len(data)} annonces insérées dans {db_path}")


# FONCTIONS D'EXPLORATION

def get_database_info(db_path: str = "deferla.db") -> dict:
    """
    Retourne les informations sur la structure de la base de données.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    info = {"tables": {}, "views": []}
    
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
    info["views"] = [row[0] for row in cursor.fetchall()]
    
    conn.close()
    return info


def print_database_stats(db_path: str = "deferla.db"):
    """
    Affiche les statistiques de la base de données.
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
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [row[1] for row in cursor.fetchall()]
    
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
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description] if cursor.description else []
    conn.close()
    return columns, results


# QUERIES EXAMPLE

def run_example_queries(db_path: str = "deferla.db"):
    """
    Exécute des exemples de requêtes sur la base de données.
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
            type, 
            COUNT(*) AS nb_annonces,
            ROUND(AVG(prix), 2) AS prix_moyen,
            ROUND(AVG(prix / surface), 2) AS prix_m2_moyen
        FROM annonces
        WHERE surface > 0
        GROUP BY type
        ORDER BY nb_annonces DESC
    """)
    for row in cursor.fetchall():
        print(f"   {row}")
    
    # Exemple 2: Top 5 villes les plus chères
    print("\n2. Top 5 villes les plus chères (prix/m2):")
    cursor.execute("""
        SELECT 
            ville,
            COUNT(*) AS nb_annonces,
            ROUND(AVG(prix / surface), 2) AS prix_m2_moyen
        FROM annonces
        WHERE surface > 0
        GROUP BY ville
        HAVING nb_annonces >= 3
        ORDER BY prix_m2_moyen DESC
        LIMIT 5
    """)
    for row in cursor.fetchall():
        print(f"   {row}")
    
    # Exemple 3: Répartition par DPE
    print("\n3. Répartition par classe DPE:")
    cursor.execute("""
        SELECT 
            d.dpe_lettre,
            COUNT(*) AS nb_annonces,
            ROUND(AVG(a.prix), 2) AS prix_moyen
        FROM annonces a
        JOIN diagnostics d ON a.id = d.id_annonce
        WHERE d.dpe_lettre IS NOT NULL 
          AND d.dpe_lettre NOT IN ('Not applicable', 'In progress')
        GROUP BY d.dpe_lettre
        ORDER BY d.dpe_lettre
    """)
    for row in cursor.fetchall():
        print(f"   {row}")
    
    # Exemple 4: Annonces avec ascenseur vs sans
    print("\n4. Comparaison ascenseur vs sans ascenseur:")
    cursor.execute("""
        SELECT 
            CASE WHEN ascenseur = 1 THEN 'Avec ascenseur' ELSE 'Sans ascenseur' END AS type_immeuble,
            COUNT(*) AS nb_annonces,
            ROUND(AVG(prix), 2) AS prix_moyen,
            ROUND(AVG(prix / surface), 2) AS prix_m2_moyen
        FROM annonces
        WHERE surface > 0 AND type = 'Appartement'
        GROUP BY ascenseur
    """)
    for row in cursor.fetchall():
        print(f"   {row}")
    
    # Exemple 5: Evolution par mois
    print("\n5. Evolution du nombre d'annonces par mois:")
    cursor.execute("""
        SELECT 
            strftime('%Y-%m', date_publication) AS mois,
            COUNT(*) AS nb_annonces,
            ROUND(AVG(prix), 2) AS prix_moyen
        FROM annonces
        GROUP BY mois
        ORDER BY mois DESC
        LIMIT 6
    """)
    for row in cursor.fetchall():
        print(f"   {row}")
    
    conn.close()
