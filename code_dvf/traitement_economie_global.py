import pandas as pd
from config import PATH_DIR_ECO

def convert_french_date(date_str):
    """
    Convertit une chaîne de caractère au format "abréviation_mois année" en datetime.
    Exemple : "janv. 2009" -> 2009-01-01
    """
    mapping = {
        "janv.": "01", "févr.": "02", "mars": "03", "avr.": "04",
        "mai": "05", "juin": "06", "juil.": "07", "août": "08",
        "sept.": "09", "oct.": "10", "nov.": "11", "déc.": "12"
    }
    
    # Sépare la chaîne en deux parties : le mois et l'année
    parts = date_str.split()
    if len(parts) != 2:
        raise ValueError(f"Format de date non reconnu : {date_str}")
    
    month_str, year_str = parts
    month_num = mapping.get(month_str)
    if not month_num:
        raise ValueError(f"Mois non reconnu : {month_str}")
    
    # Construit une date avec le premier jour du mois
    date_formatted = f"{year_str}-{month_num}-01"
    return pd.to_datetime(date_formatted, format="%Y-%m-%d")

def convert_dates_dataframe(df, column_name):
    """
    Applique la conversion de date à une colonne d'un DataFrame.
    La colonne doit contenir des dates sous forme de chaînes au format "abréviation_mois année".
    """
    df[column_name] = df[column_name].apply(convert_french_date)
    return df


df_production = pd.read_csv(PATH_DIR_ECO / 'production_credit_habitat.csv', sep=';')
df_taux_credit = pd.read_csv(PATH_DIR_ECO / 'taux_des_credit.csv', sep=';')
df_variation_encours = pd.read_csv(PATH_DIR_ECO / 'variation_encours_credit.csv' ,   sep=';')

print(df_production.head())
# Exemple d'utilisation sur votre DataFrame df_production :
df_production = convert_dates_dataframe(df_production, 'Category')
print(df_production.head())


df_production = df_production[df_production['Category']>= '2015-01-01']
print(df_production.head())
df_taux_credit = convert_dates_dataframe(df_taux_credit, 'Category')
df_taux_credit = df_taux_credit[df_taux_credit['Category']>= '2015-01-01']
print(df_taux_credit.head())

df_variation_encours = convert_dates_dataframe(df_variation_encours, 'Category')
df_variation_encours = df_variation_encours[df_variation_encours['Category']>= '2015-01-01']
print(df_variation_encours.head())







df_final = pd.merge(df_production, df_taux_credit, on='Category', how='inner')
df_final = pd.merge(df_final, df_variation_encours, on='Category', how='inner')
df_final = df_final.rename(columns={'Category': 'Date'})
df_final = df_final.drop(columns=["Crédit à l'habitat total","Crédits à l'habitat renégociés"])

df_inflation = pd.read_csv(PATH_DIR_ECO / 'inflation.csv')
df_inflation['Date'] = pd.to_datetime(df_inflation['Date'], format='%Y-%m')
df_inflation = df_inflation[df_inflation['Date'] >= '2015-01-01']

print(df_inflation.head())
df_final = pd.merge(df_final, df_inflation, on='Date', how='inner')
df_final.to_csv(PATH_DIR_ECO / 'df_eco.csv', sep=';', index=False)
print(df_final.head())