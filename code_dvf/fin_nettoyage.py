from pathlib import Path
import os
CURRENT_FILE_PATH = Path(__file__).parent.resolve()
os.chdir(CURRENT_FILE_PATH)

from config import PATH_DIR_DF_FINAL,DISTANCE_POINT_INTERET,PATH_FICHIER_DF_VF_OSM_ECO_INSEE
import polars as pl
import matplotlib.pyplot as plt
import numpy as np

from pyproj import Transformer




from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler


COLUMN_DISTANCE = ['distance_min_gares','distance_min_commerces',
                   'distance_min_education','distance_min_espaces_verts',
                   'distance_min_sante','distance_min_pharmacies',
                   'distance_min_aeroports','distance_min_routes_principales',
                   'distance_min_industries']



def nombre_valeur_null_par_colonne(df):
    for col in df.columns:
        print(col,':',df[col].null_count())
    
def reglage_null_colonne_distance(df):
    for col in COLUMN_DISTANCE:
        df = df.with_columns([
        # Indicateur binaire : 1 si null
        pl.col(col).is_null().cast(pl.Int8).alias(col + "_manquante"),
    
        # Remplacement des nulls
        pl.when(pl.col(col).is_null())
        .then(DISTANCE_POINT_INTERET[col[len("distances_min"):]])
        .otherwise(pl.col(col))
        .alias(col)])
        
    return df


def reglage_null_revenu_median_commune(df):
    
    # Extraire le code département correctement (2 ou 3 chiffres)
    df = df.with_columns([
        pl.when(pl.col("code_commune").str.slice(0,2).is_in(["97", "98"]))
        .then(pl.col("code_commune").str.slice(0,3))
        .otherwise(pl.col("code_commune").str.slice(0,2))
        .alias("code_departement_temporaire")
    ])

    # Calculer la moyenne du revenu_median_2021_commune par département
    revenu_moyen_dep = (
        df.group_by("code_departement_temporaire")
        .agg(pl.col("revenu_median_2021_commune").mean().alias("revenu_median_dep"))
    )

    # Joindre la moyenne au DataFrame principal
    df = df.join(revenu_moyen_dep, on="code_departement_temporaire", how="left")

    # Remplacer les nulls dans revenu_median_2021_commune
    df = df.with_columns([
        pl.when(pl.col("revenu_median_2021_commune").is_null())
        .then(pl.col("revenu_median_dep"))
        .otherwise(pl.col("revenu_median_2021_commune"))
        .alias("revenu_median_2021_commune")
    ])

    # Nettoyer : supprimer les colonnes temporaires
    df = df.drop(["code_departement_temporaire", "revenu_median_dep"])
    return df


def reglage_null_nombre_menage_commune(df):
    # Données extraites manuellement sur internet (donnée manquante : ville : nbr menage). On a ici le nombre d h'abitant, on divisera par 2.2 (source insee...)
    data = [
        ("13215", 14497), ("75115", 227746), ("13207", 12888), ("75120", 189805),
        ("64541", 310), ("13210", 9619), ("13204", 20094), ("85041", 302),
        ("75105", 56841), ("75116", 162061), ("75108", 35123), ("13211", 57924),
        ("13206", 39647), ("85271", 85000), ("75107", 47947), ("75101", 15919),
        ("75106", 48905), ("13208", 82609), ("49321", 390), ("75103", 32793),
        ("75114", 136368), ("75104", 28324), ("13202", 23627), ("69382", 30485),
        ("69384", 35603), ("69381", 29016), ("75119", 181616), ("13213", 92261),
        ("69385", 48711), ("69152", 10515), ("13205", 45449), ("75111", 142583),
        ("75117", 164413), ("13209", 77106), ("13212", 20829), ("75118", 188446),
        ("13203", 53115), ("75112", 140954), ("13216", 15487), ("75110", 95394),
        ("69386", 52007), ("69383", 101302), ("75102", 21119), ("75109", 58951),
        ("13214", 59948), ("69387", 85897), ("86231", 478), ("69388", 86326),
        ("13201", 39436), ("75113", 178350), ("69389", 52903)
    ]

    df_donnee_manquante = pl.DataFrame({"code_commune": [code for code, _ in data],"population": [pop for _, pop in data]})
    df_donnee_manquante = df_donnee_manquante.with_columns([(pl.col("population") / 2.2).round(0).cast(pl.Int32).alias("nb_menages_estime")])
    
    df = df.join(df_donnee_manquante.select(["code_commune", "nb_menages_estime"]),on="code_commune",how="left")
    
    df = df.with_columns([
        pl.when(pl.col("nb_menages_2021_commune").is_null())
        .then(pl.col("nb_menages_estime"))
        .otherwise(pl.col("nb_menages_2021_commune"))
        .alias("nb_menages_2021_commune")
    ])
    
    df = df.drop("nb_menages_estime")
    
    return df
    

def reglage_null(df):
    

    df = df.filter(pl.col('nature_mutation') == 'Vente')
    df = reglage_null_colonne_distance(df)
    df = reglage_null_revenu_median_commune(df)
    df = reglage_null_nombre_menage_commune(df)
    df.write_parquet(PATH_DIR_DF_FINAL / "df_final_propre.parquet")
    return df
    

def nettoyer_valeur_fonciere(df, seuil_min=None, seuil_max_quantile=0.999, seuil_min_quantile=0.001):

    # Extraire les valeurs
    valeurs = df.select("valeur_fonciere").to_numpy().flatten()
    valeurs = valeurs[~np.isnan(valeurs)]

    # Calcul des seuils
    seuil_max = np.quantile(valeurs, seuil_max_quantile)
    seuil_min_auto = np.quantile(valeurs, seuil_min_quantile)

    # Utiliser un seuil_min personnalisé si fourni, sinon on prend celui du quantile
    seuil_min = seuil_min if seuil_min is not None else seuil_min_auto

    print("Seuil 0.001 : ",seuil_min_auto)
    print("Seuil 0.999 : ",seuil_max)

    # Filtrage dans le DataFrame Polars
    df = df.filter(
        (pl.col("valeur_fonciere") >= seuil_min) &
        (pl.col("valeur_fonciere") <= seuil_max)
    )




    # Projection des Latitudes/Longitudes Pour pourvoir parler de metres.
    transformer = Transformer.from_crs("EPSG:4326", "EPSG:2154", always_xy=True)
    lon = df.select("longitude").cast(pl.Float64).to_numpy().flatten()
    lat = df.select("latitude").cast(pl.Float64).to_numpy().flatten()
    x_proj, y_proj = transformer.transform(lon, lat)
     
    # On projete
    df = df.with_columns([
        pl.Series("x_proj", x_proj),
        pl.Series("y_proj", y_proj)
    ])
     
    # On log les valeurs foncieres, parfois bcp trop grand. Peut aider ds des modeles
    df = df.with_columns([
        pl.col("valeur_fonciere").log().alias("valeur_fonciere_log")
    ])


    # One-Hot Encoding de `type_local` :  Aussi autre : ds decision tree implementer ds sckit learn, pas si performant que sa, considere tt comme des variables continues... donc chaud un peu quand on a des classes
    type_uniques = df.select("type_local").unique().to_series().to_list()
    for val in type_uniques:
        df = df.with_columns([
            (pl.col("type_local") == val).cast(pl.Int8).alias("type_local__" + str(val))
        ])
    

    df.write_parquet(PATH_DIR_DF_FINAL  / "df_final_propre.parquet") # Pouvoir verifier si on a bien gerer les valeurs aberante. On ne supprime pas l'ancien fichier

def nettoyage_final():
    df = pl.read_parquet(PATH_FICHIER_DF_VF_OSM_ECO_INSEE)
    df = reglage_null(df)
    nettoyer_valeur_fonciere(df,seuil_min = 1000)
    

if __name__ == "__main__":
    nettoyage_final()  

    # Pour des raisons d'envoi, nous allons reduire le nombre de ligne du df final propre a 10 000
    df = pl.read_parquet(PATH_DIR_DF_FINAL  / "df_final_propre.parquet")
    df_reduit = df.sample(n=10000, with_replacement=False, seed=42)
    df_reduit.write_parquet(PATH_DIR_DF_FINAL  / "df_final_propre_reduit.parquet")
