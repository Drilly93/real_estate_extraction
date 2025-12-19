from pathlib import Path
import polars as pl

CURRENT_DIR = Path(__file__).resolve().parent

BASE_PATH = CURRENT_DIR.parent
BASE_PATH_DATA = BASE_PATH / "data"


#----------------FICHIER DATA----------------#


# Dossier / Fichier csv departements francais : code_deparement,nom_departement
PATH_DIR_DEP_FR = BASE_PATH_DATA /"departements_france"
PATH_FICHIER_DEP_FR = BASE_PATH_DATA /"departements_france" / "departements_france.csv" 

# Chemin vers les fichiers csv des Valeurs Foncieres de chaque departements qu'on recoltera sur internet
PATH_DIR_VAL_FONCIERE_DEP = BASE_PATH_DATA / "ValeursFoncieresParDepartement"



# Chemin qui contiendra le fichier d'OpenStreetMap (OSM) de la France Entiere (version telecharger le 07/12/2025)
# Ce fichier contient toute les donnees cartographiques d'OSM (fichier de type .pbf)
PATH_DIR_OSM = BASE_PATH_DATA / "OpenStreetMap"
PATH_FICHIER_OSM = PATH_DIR_OSM / "france-latest.osm.pbf"
PATH_FICHIER_OSM_LIGHT = PATH_DIR_OSM / "guyane-latest.osm.pbf" # Fichier Test Pas Trop Lourd
PATH_FICHIER_OSM_MEDIUM= PATH_DIR_OSM / "ile-de-france-latest.osm.pbf" # Fichier Test Pas Trop Lourd


PATH_DIR_OSM_TRIEE = PATH_DIR_OSM / "donnee_triee" #Contiendra que les gares, aeroports, routes etc...



#Donnee eco
PATH_DIR_ECO = BASE_PATH_DATA / "Economie_global"
PATH_FICHIER_ECO = PATH_DIR_ECO / "df_eco.csv"


#Donnee INSEE 2021
#Pas d'infi dispo au dela. On prend donc une photo figee  comme représentation du contexte socio-éco local
#On l’argumentera comme une approximation stable dans le temps (ce qui est raisonnable pour ce type de données).
PATH_DIR_INSEE = BASE_PATH_DATA / "INSEE_2021"
PATH_FICHIER_INSEE_DONNEE_COMMUNE = PATH_DIR_INSEE  / "niv_vie_commune_2021.csv"
PATH_FICHIER_INSEE_DONNEE_DEPARTEMENT = PATH_DIR_INSEE  / "niv_vie_departement.csv"

#----------------FICHIER DATAFRAME_FINAL----------------#
PATH_DIR_DF_FINAL = BASE_PATH_DATA / "DataFrameFinal"


PATH_DIR_DF_VF = PATH_DIR_DF_FINAL / "DataFrame_VF"
PATH_FICHIER_DF_VF = PATH_DIR_DF_VF / "df_vf.parquet"

PATH_DIR_DF_VF_OSM = PATH_DIR_DF_FINAL / "DataFrame_VF+OMS"
PATH_FICHIER_DF_VF_OSM = PATH_DIR_DF_VF_OSM / "data_vf_oms.parquet"


PATH_DIR_DF_VF_OSM_ECO_INSEE = PATH_DIR_DF_FINAL / "DataFrame_VF+OMS+Eco+INSEE"
PATH_FICHIER_DF_VF_OSM_ECO_INSEE = PATH_DIR_DF_VF_OSM_ECO_INSEE / "df_vf_oms_eco_insee.parquet"


#----------------  AUTRE CONSTANTE POUR LES DONNEES  ----------------#

# URL pour charger les valeurs foncieres de chaque departement
URL_VAL_FONCIERE = "https://dvf-api.data.gouv.fr/dvf/csv/?dep="
URL_OSM = "https://download.geofabrik.de/europe/france.html"

# Dictionnaire des types des colonnes des csv recup de internet
# On met tous en chaine de caractere, pour faciliter ensuite le nettoyage.
# Si pas en chaine de caractere, polars et pandas generent warning : ils ont du mal a inferer le type
TYPE_COLUMN_CSV_SALE = {
    "id_mutation" : pl.Utf8,
    "date_mutation" : pl.Utf8, # aaaa-mm-jj
    "numero_disposition" : pl.Utf8,
    "nature_mutation" : pl.Utf8,
    "valeur_fonciere" : pl.Utf8,
    "adresse_numero" : pl.Utf8,
    "adresse_suffixe" : pl.Utf8,
    "adresse_nom_voie" : pl.Utf8,
    "adresse_code_voie" : pl.Utf8,
    "code_postal" : pl.Utf8,
    "code_commune" : pl.Utf8,
    "nom_commune" : pl.Utf8,
    "code_departement" : pl.Utf8,
    "ancien_code_commune" : pl.Utf8 ,
    "ancien_nom_commune" : pl.Utf8,
    "id_parcelle" : pl.Utf8 ,
    "ancien_id_parcelle" : pl.Utf8,
    "numero_volume" : pl.Utf8,
    "lot1_numero" : pl.Utf8 ,
    "lot1_surface_carrez" : pl.Utf8,
    "lot2_numero" : pl.Utf8,
    "lot2_surface_carrez" : pl.Utf8,
    "lot3_numero" : pl.Utf8,
    "lot3_surface_carrez" : pl.Utf8,
    "lot4_numero" : pl.Utf8,
    "lot4_surface_carrez" : pl.Utf8,
    "lot5_numero" : pl.Utf8,
    "lot5_surface_carrez" : pl.Utf8,
    "nombre_lots" : pl.Utf8,
    "code_type_local" : pl.Utf8,
    "type_local" : pl.Utf8,
    "surface_reelle_bati" : pl.Utf8,
    "nombre_pieces_principales" : pl.Utf8,
    "code_nature_culture" : pl.Utf8,
    "nature_culture" : pl.Utf8,
    "code_nature_culture_speciale" : pl.Utf8,
    "nature_culture_speciale" : pl.Utf8,
    "surface_terrain" : pl.Utf8,
    "longitude" : pl.Utf8,
    "latitude" : pl.Utf8,
    "section_prefixe" : pl.Utf8
}



# Le Nettoyage ne gardera que certain features et convertira en type voulu.
# Apres avoir tester modele, y a une features importante qu'on a mise de cote : "nombre_pieces_principales"
# Il y'avait bcp de Nan/None, dans cette colonne, on l'a donc mise de cote. 
# On aurait pu peut etre traite ces Nan d'une certaine maniere.
# Idee : premier modele si assez d'element qui predise nbr_piece_principale, puis utiliser le modele pour remplir.
COLUMN_FINAL = ['date_mutation','valeur_fonciere','code_postal','nom_commune',"code_commune",
                'nature_mutation','code_departement','id_parcelle','type_local',
                'surface_reelle_bati','surface_terrain','longitude','latitude',"adresse_numero",
                "adresse_suffixe","adresse_nom_voie","adresse_code_voie"]


TYPE_COLUMN_CSV_PROPRE = {
    "date_mutation": pl.Date,              
    "valeur_fonciere": pl.Float64,         
    "surface_reelle_bati": pl.Float64,     
    "code_postal": pl.Utf8,
    "code_commune":pl.Utf8,
    "adresse_numero" : pl.Utf8,
    "adresse_suffixe" : pl.Utf8,
    "adresse_nom_voie" : pl.Utf8,
    "adresse_code_voie" : pl.Utf8,                
    "nom_commune": pl.Utf8,                
    "nature_mutation": pl.Utf8,            
    "code_departement": pl.Utf8,           
    "id_parcelle": pl.Utf8,                
    "type_local": pl.Utf8,                 
    "surface_terrain": pl.Float64,         
    "longitude": pl.Float64,               
    "latitude": pl.Float64,                
    "prix_par_m2_habitable" : pl.Float64,
    "prix_par_m2_terrain" : pl.Float64,
    "annee" : pl.Int32,
    "annee_mois": pl.Utf8
}


# Constante de OpenStreetMap Utile

POINT_INTERET = ["gares","commerces","education","espaces_verts","sante",
                 "pharmacies","aeroports","routes_principales","industries"]

POINT_INTERET_FICHIER = {
    "gares": PATH_DIR_OSM_TRIEE / "gares.parquet",
    "commerces": PATH_DIR_OSM_TRIEE / "commerces.parquet",
    "education": PATH_DIR_OSM_TRIEE / "education.parquet",
    "espaces_verts": PATH_DIR_OSM_TRIEE / "espaces_verts.parquet",
    "sante": PATH_DIR_OSM_TRIEE / "sante.parquet",
    "pharmacies": PATH_DIR_OSM_TRIEE / "pharmacies.parquet",
    "aeroports": PATH_DIR_OSM_TRIEE / "aeroports.parquet",
    "routes_principales": PATH_DIR_OSM_TRIEE / "routes_principales.parquet",
    "industries": PATH_DIR_OSM_TRIEE / "industries.parquet"
}

DISTANCE_POINT_INTERET = {
    "gares": 1500,
    "commerces": 500,
    "education": 1000,
    "espaces_verts": 500,
    "sante": 1000,
    "pharmacies": 500,
    "aeroports": 30000,
    "routes_principales": 1500,
    "industries": 1000
}


# Voir https://wiki.openstreetmap.org/wiki/Map_features pour plus d'information

TAGS_UTILISE = TAGS_UTILISE = {
    "gares": {"public_transport": ["station"]},
    "commerces": {"shop": ["supermarket", "convenience"]},
    "education": {"amenity": ["school", "university"]},
    "espaces_verts": {"leisure": ["park"]},
    "sante": {"amenity": ["hospital", "clinic",'doctors']},
    "pharmacies": {"amenity": ["pharmacy"]},
    "aeroports": {"aeroway": ["aerodrome"]},
    "routes_principales": {"highway": ["motorway", "trunk"]},
    "industries": {"man_made": ["works"]}
}

PROJECTION_EPSG_INITIAL = 4326
PROJECTION_EPSG_FINAL = 2154

POINT_INTERET_LOURD_ANCIENNE_VERSION_1 = ["commerces","industries","espaces_verts","education"]
POINT_INTERET_LOURD = []
