import requests

agences = [
    "https://www.arcenciel-immo.com",
    "https://www.mycasa-immobilier.com",
    "https://www.deferla.com"

]

def check_robots_txt(url):
    robots_url = f"{url.rstrip('/')}/robots.txt"
    headers = {
        'User-Agent': 'ProjetEtudeScrapy (Contact: votre-email@exemple.com)'
    }
    
    try:
        response = requests.get(robots_url, headers=headers, timeout=10)
        if response.status_code == 200:
            print(f"\nOK {url} - robots.txt trouvé")
            lines = response.text.split('\n')
            disallows = [line for line in lines if "Disallow" in line]
            
            if not disallows:
                print("Aucune restriction 'Disallow' trouvée.")
            else:
                for d in disallows[:5]: 
                    print(f"Restriction trouvée : {d}")
        elif response.status_code == 404:
            print(f"\n--- [LIBRE] {url} ---")

    except Exception as e:
        print(f"\n--- [ERREUR CONNEXION] {url} : {e} ---")

for agence in agences:
    check_robots_txt(agence)