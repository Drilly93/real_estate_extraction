from deferla_database import create_database, insert_data_from_json

# 1. Créer la base
create_database("deferla.db")

# 2. Insérer les données JSON
insert_data_from_json("deferla.json", "deferla.db")

# 3. Explorer
from deferla_database import print_database_stats
print_database_stats("deferla.db")