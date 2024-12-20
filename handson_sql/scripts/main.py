from utils.db_func import create_db, create_tables, list_tables, insert_rows_with_pandas, insert_rows_with_sql
from utils.data_wrangler import read_datas
from env import DATABASE_FOLDER, TABLE_PROPERTIES_PATH
import logging
import json

logging.basicConfig(
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
    level=logging.INFO
)

# 1 - Database creation
db_name = "school_mngt_db"
create_db(db_location=DATABASE_FOLDER, db_name=db_name)


# 2 - Tables creation : 
# Complete the properties of the tables to be build.
# Read the create_tables function to make sure the query is correct
# Create the tables
with open(TABLE_PROPERTIES_PATH, "r") as file:
    table_properties: dict = json.load(file)

create_tables(table_properties=table_properties, db_location=DATABASE_FOLDER, db_name=db_name)

# 3 - Created tables listing
tables = list_tables(db_location=DATABASE_FOLDER, db_name=db_name)
logging.info(f"Created tables are {tables}")

# 4 - Created tables visualizing
# Use your datamangement tool to visualize the created tables : 
# are all the columns created?
# do they have the correct datatype


# 5 - Data source from datasets loading
# Read the data files corresponding to the created tables
datas = read_datas()

# 6 - Populating the database
# Populate the tables with the data from the data
# Below are examples of ways of inserting data into the tables. The insert_rows_with_sql function is to be created
insert_rows_with_pandas(datas["course"], db_location=DATABASE_FOLDER, db_name=db_name, table_name="course")
insert_rows_with_sql(datas["teacher"], db_location=DATABASE_FOLDER, db_name=db_name, table_name="teacher")      
# Populate the rest of the tables