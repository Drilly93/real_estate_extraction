import polars as pl
from dvf_database import create_database, insert_data_from_polars, print_database_stats
from config_db import *

# 1. Créer la structure
create_database(BASE_PATH / "final_dvf_immobilier.db")

# 2. Insérer les données
df = pl.read_parquet(BASE_PATH_DATA_FINAL)
insert_data_from_polars(df, BASE_PATH / "final_dvf_immobilier.db")
# 3. Explorer
print_database_stats(BASE_PATH / "final_dvf_immobilier.db")