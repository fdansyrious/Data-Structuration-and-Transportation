from utils.db_func import create_db, create_tables, list_tables
from env import DATABASE_FOLDER, TABLE_PROPERTIES_PATH
import logging
import json

logging.basicConfig(
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
    level=logging.INFO
)

db_name = "school_mngt_db"
create_db(db_location=DATABASE_FOLDER, db_name=db_name)

with open(TABLE_PROPERTIES_PATH, "r") as file:
    table_properties: dict = json.load(file)

create_tables(table_properties=table_properties, db_location=DATABASE_FOLDER, db_name=db_name)

tables = list_tables(db_location=DATABASE_FOLDER, db_name=db_name)
logging.info(f"Created tables are {tables}")            
        