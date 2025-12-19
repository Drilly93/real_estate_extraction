import os
from pathlib import Path

#-----------------------------------------------------------#
#         Change le répertoire de travail courant
#  pour être sûr que tous les chemins relatifs pointent ici
#-----------------------------------------------------------#
CURRENT_FILE_PATH = Path(__file__).parent.resolve()
os.chdir(CURRENT_FILE_PATH)



#-----------------------------------------------------------#
#                      IMPORT  CONSTANTE                    #
#-----------------------------------------------------------#
from config import PATH_FICHIER_ECO, PATH_DIR_ECO#Fichier ECO
from config import PATH_FICHIER_INSEE_DONNEE_COMMUNE,PATH_FICHIER_INSEE_DONNEE_DEPARTEMENT,PATH_DIR_DF_VF_OSM_ECO_INSEE # Fichier INSEE

from config import PATH_FICHIER_DF_VF_OSM #le fichier df_vf_osm.parquet
from config import PATH_FICHIER_DF_VF_OSM_ECO_INSEE # fichier de sortie fusion avec donnee eco et insee


#-----------------------------------------------------------#
#                       GESTION DONNEE                      #
#-----------------------------------------------------------#
import polars as pl 


def fusion_eco():
    os.makedirs(PATH_DIR_ECO, exist_ok=True)

    df_eco = pl.read_csv(PATH_FICHIER_ECO, separator=";")
    df_vf = pl.read_parquet(PATH_FICHIER_DF_VF_OSM)



    #Convertir str en float64

    df_eco = df_eco.with_columns([pl.col("Crédits à l'habitat hors renégociations").str.replace(',','.').cast(pl.Float64),
                                  pl.col('Taux hors renégociations').str.replace(',','.').cast(pl.Float64),
                                  pl.col("Variations d'encours mensuelles cvs").str.replace(',','.').cast(pl.Float64),
                                  pl.col("Date").str.strptime(pl.Date, "%Y-%m-%d"),
                                  pl.col("Date").str.strptime(pl.Date, "%Y-%m-%d").dt.strftime("%Y-%m").alias("year_month")
                                  ])

    df_vf = df_vf.with_columns([
        pl.col("date_mutation").dt.strftime("%Y-%m").alias("year_month")
    ])

    df_joined = df_vf.join(df_eco, on="year_month", how="left")
    df_joined = df_joined.drop(["year_month","Date"])
    
    return df_joined


def fusion_insee(df_vf_eco):
    df_commune = pl.read_csv(PATH_FICHIER_INSEE_DONNEE_COMMUNE, separator=";",
                             skip_rows=2,schema_overrides={"Code": pl.Utf8},
                             null_values=["N/A - résultat non disponible"])

    df_commune = df_commune.rename({"Code": "code_commune", 
                                    "Libellé": "nom_commune", 
                                    "Nb de ménages 2021": "nb_menages_2021_commune", 
                                    "Médiane du niveau de vie 2021": "revenu_median_2021_commune"})
    df_commune = df_commune.with_columns([pl.col("nb_menages_2021_commune").cast(pl.Float64()),
                                          pl.col("revenu_median_2021_commune").cast(pl.Float64)])



    df_departement =  pl.read_csv(PATH_FICHIER_INSEE_DONNEE_DEPARTEMENT, separator=";",
                             skip_rows=2,schema_overrides={"Code": pl.Utf8},
                             null_values=["N/A - résultat non disponible"])

    df_departement = df_departement.rename({"Code": "code_departement", 
                                    "Libellé": "nom_departement", 
                                    "Nb de ménages 2021": "nb_menages_2021_departement", 
                                    "Médiane du niveau de vie 2021": "revenu_median_2021_departement",
                                    "Salaire net horaire moyen 2022" : "salaire_net_horaire_moyen_2022_departement",
                                    "Taux de chômage annuel moyen 2023" : "taux_chomage_2023_departement"})
    df_departement = df_departement.with_columns([pl.col("nb_menages_2021_departement").cast(pl.Float64()),
                                          pl.col("revenu_median_2021_departement").cast(pl.Float64),
                                          pl.col("salaire_net_horaire_moyen_2022_departement").cast(pl.Float64),
                                          pl.col("taux_chomage_2023_departement").cast(pl.Float64)])



    df_vf_eco = df_vf_eco.join(df_commune, on="code_commune", how="left")
    df_vf_eco = df_vf_eco.join(df_departement, on="code_departement", how = "left")


    df_vf_eco = df_vf_eco.drop('nom_commune_right')
    return df_vf_eco


def fusion_total():
    os.makedirs(PATH_DIR_DF_VF_OSM_ECO_INSEE, exist_ok=True)
    fusion_insee(fusion_eco()).write_parquet(PATH_FICHIER_DF_VF_OSM_ECO_INSEE)


if __name__ == "__main__":
    print("Fusion des données ECO et INSEE avec le DataFrame VF+OSM en cours...")
    fusion_total()
    print("==== FIN ---")


