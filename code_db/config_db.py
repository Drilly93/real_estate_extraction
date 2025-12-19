from pathlib import Path
import polars as pl

CURRENT_DIR = Path(__file__).resolve().parent

BASE_PATH = CURRENT_DIR.parent
BASE_PATH_DATA = BASE_PATH / "data"
BASE_PATH_DATA_FINAL = BASE_PATH_DATA / "DataFrameFinal" / "df_final_propre_reduit.parquet"

PATH_FILE_TO_JSON_DEFERLA = BASE_PATH / "deferla" / "immo_project" / "results" / "deferla.json"

