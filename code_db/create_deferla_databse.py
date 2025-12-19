from deferla_database import create_database, insert_data_from_json
from deferla_database import print_database_stats
from config_db import *

create_database(BASE_PATH / "final_deferla.db")

insert_data_from_json(BASE_PATH / "deferla.json", BASE_PATH / "final_deferla.db")

print_database_stats(BASE_PATH / "final_deferla.db")