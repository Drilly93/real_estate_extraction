import os
from tqdm import tqdm
from pathlib import Path

CURRENT_FILE_PATH = Path(__file__).parent.resolve()
os.chdir(CURRENT_FILE_PATH)


from config import PATH_DIR_OSM_TRIEE, PATH_DIR_DF_VF_OSM
from config import PATH_FICHIER_OSM, PATH_FICHIER_OSM_LIGHT,PATH_FICHIER_OSM_MEDIUM,PATH_FICHIER_DF_VF
from config import POINT_INTERET, POINT_INTERET_FICHIER, DISTANCE_POINT_INTERET, TAGS_UTILISE
from config import PROJECTION_EPSG_INITIAL, PROJECTION_EPSG_FINAL
from config import PATH_FICHIER_DF_VF_OSM

from config import POINT_INTERET_LOURD #Point d'interet dont les fichiers sont trop lourd

import osmium # Permet la lecture du fichier de OpenStreetMap (.pbf)

# Traitement numérique
import numpy as np 
import polars as pl 

# Traitement spatial
import geopandas as gpd # Gerer les donnees spatiales (pas possible avec polars ou pandas)
from pyproj import Transformer # Pour projeter des coordonnees geographiques (lat/lon devient  x/y en metres)
from scipy.spatial import cKDTree # Structure de donnees qui permet une recherche rapide pour les recherches spatiales (regarder les point proches)
from shapely.geometry import Point, LineString, Polygon #Pour manipuler les objets du fichier OSM : Point / Way / Area



# Parallelisation et affichage progression            #
from joblib import Parallel, delayed  # Parallelisation du code (Multiprocessing et pas Threads !)
from tqdm import tqdm # Affichage Progression
import time



# IMPORTANT A LIRE 
#
# ============================
# Deux parties dans ce code :
# ============================

# Partie 1 : Nettoyage du gros fichier OpenStreetMap (OSM) qui est une extension .pbf
#            téléchargé depuis : https://download.geofabrik.de/europe/france.html
#
# Objectif : extraire les points d’intérêt (POI) utiles pour notre analyse
#    - Transports : stations de bus, métro, tram, RER
#    - Commerces
#    - Écoles : crèches, maternelles, primaires, collèges, lycées, universités
#    - Santé : hôpitaux, cliniques, médecins
#    - Pharmacies
#    - Aéroports
#    - Routes principales : autoroutes et voies rapides
#    - Zones industrielles
#    (Pas sur qu'on utilisera tout car faudra associe a chaque vente, les donnees qu'on veut (nbr de de POI dans un certain rayon et distance min entre un poi et l'adresse de vente ))
#    On verra ceci, au moment de faire les associations
#
#   Pour chaque type de POI, on extrait les données et on les sauvegarde dans un fichier `.parquet` séparé.
#   Chaque POI est un soit un Point, soit un Area, soit un Polygone dns le fichier de OSM.
#   Tout est décrit dans ce lien : wiki.openstreetmap.org/wiki/Map_features 


#   On travaille avec des objets géographiques (points, polygones et area) : on utilise GeoPandas.
#   On transforme les coordonnées longitude/latitude qui sont des degre  en projection EPSG:2154 correspondant a des mètres
#   pour pouvoir faire des calculs de distance fiables (notamment pour la détection de doublons).

#   Une fois les POI extraits, on nettoie les doublons potentiels :
#   - Exemple : deux POI "Carrefour Market" à quelques mètres l’un de l’autre : même lieu
#   - Pour ça on regroupe par nom + proximité géographique (distance < seuil)




# RAPPEL POINT_INTERET = ["gares","commerces","education","espaces_verts","sante", "pharmacies","aeroports","routes_principales","industries"]
# Pour le reste des constante se referer au fichier config
# POINT_INTERET_FICHIER / DISTANCE_POINT_INTERET / TAGS_UTILISE / PROJECTION_EPSG_INITIAL / PROJECTION_EPSG_FINAL




def nettoyage_fichier_open_street_map(fichier_open_street_map,extraction_OSM = True,supprimer_doublons = True):
    # Fonction qui gere le nettoyage des donnees OpenStreetMap en parallèle
    print("======NETTOYAGE FICHIER OSM EN COURS======")
    os.makedirs(PATH_DIR_OSM_TRIEE, exist_ok=True) # On creer le dossier qui va contenir les fichiers (.parquet) de chaque point d'interet
    
    
    
    if(extraction_OSM):
        print("\nTriage Point Interet ...")
        extraire_tous_les_points_interet(fichier_open_street_map)
    
    if(supprimer_doublons):
        print("\nMultiProcessing Supprimer Doublons ...")
        #On supprime les doublons des (.parquet) produit pour chaque point d'interet
        Parallel(n_jobs=-1,verbose=1)(delayed(supprimer_les_doublons)(POINT_INTERET_FICHIER[poi]) for poi in POINT_INTERET) # n_jobs=-1 : utilise tous les coeurs dispo du processeur et  verbose=1 :  affiche une barre de progression simple dans la console 
        
    
    time.sleep(1) #juste pour affichage clean 
    print("\n==========FIN NETTOYAGE OSM==========")
   

def extraire_tous_les_points_interet(fichier_pbf):
    #  osmium.Handler permet de traiter plus rapidement le gros fichier .pbf
    #  Ne charge pas tout en mémoire !!!! contrairement a pyrosm  qui nous fait des out of memory
    
    # Attention on a des latitudes et Longitudes On doit pas oublier de faire une projection 
    # Projection EPSG:4326 en EPSG:2154
    # EPSG:4326 => latitude/longitude (degrés)
    # EPSG:2154 => (Projection Lambert-93) un système en mètres utilisé en France.


    # Dictionnaire qui stockera les objets par type de POI
    poi_data = {poi: [] for poi in POINT_INTERET}

    
    #Documentation : https://docs.osmcode.org/pyosmium/latest/
    class Handler(osmium.SimpleHandler):
        def node(self, n):
            for poi in POINT_INTERET:
                tags = TAGS_UTILISE[poi]
                for tag, valeurs in tags.items():
                    if tag in n.tags and (valeurs is True or n.tags[tag] in valeurs):
                        poi_data[poi].append({
                            'id': n.id,
                            'name': n.tags.get('name', ''),
                            'location': Point(n.location.lon, n.location.lat),
                        })
                        break

        def way(self, w):
            for poi in POINT_INTERET:
                tags = TAGS_UTILISE[poi]
                for tag, valeurs in tags.items():
                    if tag in w.tags and (valeurs is True or w.tags[tag] in valeurs):
                        try:
                            coords = [(n.lon, n.lat) for n in w.nodes if n.location.valid()]
                            if len(coords) >= 3:
                                geom = Polygon(coords)
                            elif len(coords) >= 2:
                                geom = LineString(coords)
                            else:
                                continue
                            poi_data[poi].append({
                                'id': w.id,
                                'name': w.tags.get('name', ''),
                                'location': geom,
                            })
                        except:
                            pass
                        break

        def area(self, a):
            for poi in POINT_INTERET:
                tags = TAGS_UTILISE[poi]
                for tag, valeurs in tags.items():
                    if tag in a.tags and (valeurs is True or a.tags[tag] in valeurs):
                        try:
                            rings = list(a.outer_rings())
                            if rings and len(rings[0]) >= 3:
                                exterior = [(n.lon, n.lat) for n in rings[0]]
                                poly = Polygon(exterior)
                                if poly.is_valid:
                                    poi_data[poi].append({
                                        'id': a.id,
                                        'name': a.tags.get('name', ''),
                                        'location': poly,
                                    })
                        except:
                            pass
                        break

    Handler().apply_file(fichier_pbf, locations=True)

    print("Sauvegarde des fichiers .parquet...")

    for poi, objets in poi_data.items():
        path = POINT_INTERET_FICHIER[poi]
        if not objets:
            gpd.GeoDataFrame(geometry=[], crs="EPSG:" + str(PROJECTION_EPSG_INITIAL)).to_parquet(path)
            continue

        df = gpd.GeoDataFrame(objets, geometry="location", crs="EPSG:" + str(PROJECTION_EPSG_INITIAL))
        df_proj = df.to_crs(epsg=PROJECTION_EPSG_FINAL) # On fait la projection, sinon pas possible de travailler avec lat et lon
        df["x_proj"] = df_proj.centroid.x # Si area ou way on prend le centre .... ok pour les nodes
        df["y_proj"] = df_proj.centroid.y
        df.to_parquet(path) #Fichier qui possede encore les doublons. On traitera ca ensuite.
        # Doublon dans le sens ou certain poi sont a la fois des node / polygone etc... Ou certain apparaissent deux fois, mais a une dizaine de metre de difference...
        # Nettoyage en sortie


def supprimer_les_doublons(chemin_fichier, seuil=25):
    # Regroupe les points ayant le même nom et proches géographiquement (seuil en metres)
    # - Garde un seul point représentatif par groupe (le premier trouve)
    # - Evite les doublons lies à des POIs repetes (ex : "Pharmacie" copiee 3 fois au meme endroit)
    
    
    #On lancera la fonction sur chacun des pois.parquet qu'on a eu grace a la fonction extraire_tous_les_points_interet()
    
    gdf = gpd.read_parquet(chemin_fichier)
    
    if gdf.empty :
        # Utile pour les petit fichiers test de OSM de guyane : PATH_FICHIER_OSM_LIGHT ...
        # Pas de route principal en guyane (autoroute/grand axe)
        return
    
    lignes = []

    for nom, group_df in gdf.groupby("name"): #gdf.groufby("name", nous donne un objet GeoDataFrameGroupBy sur lequel on peut iterer : pour chaque "name" x, on a le sous df, contenant tt les lignes dont le features "name" est x.
        coords = group_df[["x_proj", "y_proj"]].to_numpy() # Creer une matrice (ligne : observation   et colonnes : les projections)
        pris = np.full(len(group_df), False)  # marqueurs pour eviter les doublons

        for i in range(len(coords)):
            if pris[i]:
                continue
            dists = np.linalg.norm(coords - coords[i], axis=1)  # distances à tous les autres
            proches = np.where(dists <= seuil)[0] # recup indice des doublons proche
            lignes.append(group_df.iloc[i])  # garde un seul point (le premier du groupe)
            pris[proches] = True  # on marque tous ceux qui ont ete pris

    gdf_resultat = gpd.GeoDataFrame(lignes, geometry="location", crs=gdf.crs)  # GeoDataFrame nettoye
    gdf_resultat.to_parquet(chemin_fichier)


# Partie 2 : On enrichie la base de donnees de valeur fonciere avec ces nouveaux features geographique !
#            Il sont issues des points d’interet extraits en Partie 1.


# Pour chaque point (latitude, longitude) de la base de donnee des valeurs foncieres, on peut mainteant 
#      -Compter le nombre de point d'interet dans un rayon donne
#      -Determiner la distance au point d'interet le plus proche
#
# Les distances sont fiables car on travaille dans le système projete (EPSG:2154)



def projeter_biens(df_valeurs_fonciere):
    transformer = Transformer.from_crs(PROJECTION_EPSG_INITIAL, PROJECTION_EPSG_FINAL, always_xy=True)
    lon = df_valeurs_fonciere["longitude"].to_numpy()
    lat = df_valeurs_fonciere["latitude"].to_numpy()
    x_proj, y_proj = transformer.transform(lon, lat)

    return df_valeurs_fonciere.select(["latitude", "longitude"]).with_columns([
            pl.Series("x_proj", x_proj),
            pl.Series("y_proj", y_proj)])



def construire_kdtrees(point_interet_dict_bdd):
    #On construit un kdtree pour chaque poi.parquet qu'on a
    #On voudra par la suite trouver tous les poi proche d'un certain rayons, 
    #et cette structure de donnee et plus efficace qu'un parcours lineaire du fichier
    
    #==Input==
    # point_interet_dict_bdd : dictionnaire contenant les fichiers de chaque points d'interets 
    #     cle = "commerces", "gares", etc... Ce sont les elements de POINT_INTERET
    #     valeur = GeoDataFrame contenant les point d'interet (déjà projetés en EPSG:2154 dans la partie 1 du code) 

    
    kdtree_dict = {} #Dictionnaire dont la cle seront les elements de POINT_INTERET (nos pois) et la valeur l'arbre associe  au lieu du GeoDataFrame 
    coords_dict = {} #Dictionnaire dont la cle seront les elements de POINT_INTERET (nos pois) et une matrice dont chaque ligne est un poi particulier, et les colonnes, les projections en metre 

    for poi_name, gdf in point_interet_dict_bdd.items():
        if "x_proj" in gdf.columns and "y_proj" in gdf.columns and not gdf.empty:
            coords = np.column_stack((gdf["x_proj"].values, gdf["y_proj"].values))
            coords_dict[poi_name] = coords
            kdtree_dict[poi_name] = cKDTree(coords)
            # Les indices retournés par le KDTree correspondent bien aux lignes de coords !
        else:
            coords_dict[poi_name] = None
            kdtree_dict[poi_name] = None

    return kdtree_dict, coords_dict



def rajout_features_un_element(lat,lon,x,y,kdtree_dict,coords_dict):
    # Pour un bien donné (lat/lon), calcule le nombre et la distance minimale aux POIs proches pour chaque catégorie
    
    
    result = {"latitude": lat, "longitude": lon}

    for poi_name, tree in kdtree_dict.items():
        rayon = DISTANCE_POINT_INTERET[poi_name]

        if tree is None:
            result["nb_"+str(poi_name)] = 0
            result["distance_min_"+str(poi_name)] = np.nan #crash avec None, polars infere mal sinon
            continue
        
        
        
        indices = tree.query_ball_point([x, y], r=rayon) # Donne tous les indices des pois dans un certains rayons
        nb_proches = len(indices)
        result["nb_"+str(poi_name)] = nb_proches
    
        if nb_proches > 0:
            dists = np.linalg.norm(coords_dict[poi_name][indices] - np.array([x, y]), axis=1)
            result["distance_min_"+str(poi_name)] = float(round(dists.min(), 2))
        else:
            result["distance_min_"+str(poi_name)] = np.nan #crash avec None

    return result


def rajout_features_base_entiere(chemin_df_parquet,kdtree_dict, coords_dic):
    # Applique l’enrichissement spatial à tous les biens : pour chaque bien du fichier, calcule le nb et la distance aux POIs
    # A voir on va peut etre donner juste la distance min entre deux biens, car fichier poi.parquet parfois assez gros 
    
    
    # === Inputs ===
    # chemin_df_parquet : chemin vers le fichier .parquet contenant les biens (avec latitude / longitude)
    # kdtree_dict : dictionnaire {poi_name: cKDTree} pour les recherches spatiales
    # coords_dic : dictionnaire {poi_name: coords numpy array}, pour calculer les distances

    # On lit le gros fichier des valeurs foncieres (Celui avec des milions de lignes) une seule fois avant le multiprocessing (plus efficace)
    # Et on fait une seul projection, des lat/long. Plus rapide que si on devait le faire a chaque fois pour chaque vente.
    df = pl.read_parquet(chemin_df_parquet)
    df_proj = projeter_biens(df)
    


    # Conversion en liste de dictionnaires (1 dict = 1 ligne du fichier de valeur fonciere)
    list_dict_row_df = df_proj.to_dicts()



    # Rajoute pour chaque bien les nouvelles features geographiques ( nb de poi PROCHE ET distance min pour chaque poi)
    results = [
    rajout_features_un_element(
        row["latitude"], row["longitude"], row["x_proj"], row["y_proj"],
        kdtree_dict, coords_dic
    )
    for row in tqdm(list_dict_row_df)
]


    # Résultats = liste de dictionnaires :  on les convertit en DataFrame Polars
    df_enrichie = pl.DataFrame(results)

    # Fusion avec le DataFrame initial des valeurs fonciere : on ajoute les colonnes calculées
    # Ok car joblib conserve l'ordre des lignes traitees.....
    new_df = df.with_columns([df_enrichie[col] for col in df_enrichie.columns])

    os.makedirs(PATH_DIR_DF_VF_OSM, exist_ok=True)
    new_df.write_parquet(PATH_FICHIER_DF_VF_OSM) 
    
if __name__ == "__main__":
    fichier_osm = PATH_FICHIER_OSM  #PATH_FICHIER_OSM_LIGHT  #PATH_FICHIER_OSM_MEDIUM   
    # nettoyage_fichier_open_street_map(str(fichier_osm))   # A EXECUTER SUR GOOGLE CLOUD : LINUX et PARALLLELISATION NECESSAIRE ( Traietement long et volumineux )
        
    # Dictionnaire contenant les GeoDataFrame de chaque points d'interets 
    point_interet_dict_bdd = {}
    for i in tqdm(range(len(POINT_INTERET)), desc="Chargement des POI"):
        
        point_interet_dict_bdd[POINT_INTERET[i]] = gpd.read_parquet(POINT_INTERET_FICHIER[POINT_INTERET[i]])
        #kdtree_dict : Dictionnaire dont la cle seront les elements de POINT_INTERET (nos pois) et la valeur l'arbre associe  au lieu du GeoDataFrame 
        #coords_dict : Dictionnaire dont la cle seront les elements de POINT_INTERET (nos pois) et une matrice dont chaque ligne est un poi particulier, et les colonnes, les projections en metre 
        kdtree_dict, coords_dict = construire_kdtrees(point_interet_dict_bdd)    
        
    # Nouvelle Base de Donnee avec les features geographiques
    rajout_features_base_entiere(PATH_FICHIER_DF_VF,kdtree_dict, coords_dict)
    print("FIN RAJOUT FEATURES")

