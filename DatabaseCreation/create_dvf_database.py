import polars as pl
from dvf_database import create_database, insert_data_from_polars, print_database_stats

# 1. Créer la structure
create_database("dvf_immobilier.db")

# 2. Insérer les données
df = pl.read_parquet("df_final_propre.parquet")
insert_data_from_polars(df, "dvf_immobilier.db")

# 3. Explorer
print_database_stats("dvf_immobilier.db")