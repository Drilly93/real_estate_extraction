import scrapy
import json

class DeferlaSpider(scrapy.Spider):
    name = 'deferla'
    start_urls = ['https://immobilier.altelis.com/deferla']

    def parse(self, response):
            data = json.loads(response.text)
            
            for bien in data:
                ref = bien.get('Bien_Reference')
                images = []
                for i in range(0, 2):
                    img_key = f"Image_{i}"
                    img_url = bien.get(img_key)
                    if img_url:
                        images.append(img_url)

                yield {
                    # ID and URL
                    'id': ref,
                    'url': f"https://www.deferla.com/bien?id={ref}",
                    'date_publication': bien.get('Bien_Date'),
                    
                    # Property features
                    'type': bien.get('Bien_Type'),
                    'titre': bien.get('Bien_Libellés'),
                    'prix': bien.get('Bien_Prix'),
                    'honoraires': bien.get('Bien_Honoraires'),
                    'surface': bien.get('Bien_Surface'),
                    'pieces': bien.get('Bien_Nb_Pieces'),
                    'chambres': bien.get('Bien_Nb_Chambres'),
                    'etage': bien.get('Bien_Etage_Immeuble'),
                    'ascenseur': bien.get('Bien_Ascenseur'),
                    
                    # Localization
                    'ville': bien.get('Bien_Ville'),
                    'code_postal': bien.get('Bien_Code_Postal'),
                    'latitude': bien.get('Bien_Latitude'),
                    'longitude': bien.get('Bien_Longitude'),
                    
                    # Condition of the property
                    'etat': bien.get('Bien_Etat'),
                    'salle_de_bain': bien.get('Bien_Nb_Sdb'),
                    'chauffage_type': bien.get('Bien_Chauffage_Nature_Type'),
                    'chauffe_eau': bien.get('Bien_Chauffe_Eau_Type'),
                    'exposition': bien.get('Bien_Exposition'),
                    
                    # Energy performance
                    'dpe_valeur': bien.get('Bien_DPE'),
                    'dpe_lettre': bien.get('Bien_DPE_Letter'),
                    'ges_valeur': bien.get('Bien_GES'),
                    'ges_lettre': bien.get('Bien_GES_Letter'),
                    'charges_annuelles': bien.get('Bien_Charges_Courantes_Provisionnelles'),
                    
                    # Personal data so we don't scrape that for RGPD compliance
                    #'nom_conseiller': bien.get('Bien_Conseiller_Nom'),
                    #'email_conseiller': bien.get('Bien_Conseiller_Email'),
                    #'telephone_conseiller': bien.get('Bien_Conseiller_Telephone'),
                    #'agence_nom': bien.get('Bien_Conseiller_Agence'),
                    
                    #  Image and description
                    'description': bien.get('Bien_Description'),
                    'image_principale': bien.get('Bien_Image_1'),
                    'image_urls': images
                    
                }