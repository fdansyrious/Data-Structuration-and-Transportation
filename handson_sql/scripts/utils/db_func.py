from typing import Optional
import sqlite3
import logging
import pandas as pd

logging.basicConfig(
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
    level=logging.DEBUG
)

def create_db(db_location: str, db_name: str) -> None:
    """
    Creates SQLite database
    
    Parameters:
    db_location : location to store the database
    db_name : name of the dabase
    """
    db_location = db_location if db_location[-1] == "/" else db_location + "/"
    db_name = db_name if db_name[-3:] == ".db" else db_name+".db"
    path_to_db = db_location + db_name
    logging.info({f"Path to DB : {path_to_db}"})
    try:
        connection:sqlite3.connect  = sqlite3.connect(path_to_db)
        logging.info({f"Database {db_name} successfully created in {db_location}"})
        connection.close()
    except Exception as e:
        logging.debug({f"Failed to create database : {e}"})
        
def table_creation_query(table_properties: dict, table_name: str) -> Optional[str]:
    """
    Builds the query to run to create table_name.
    
    Parameters:
    table_properties : dict of all the tables and their attributes
    table_name : name of the table to be created
    
    Return:
    table creation query 
    """
    table_attributes: list[str] = table_properties.get(table_name, None)
    if table_attributes:
        query = f"""CREATE ... {table_name} 
        (
            {", ".join([" ".join(attribute) for attribute in table_attributes])}
            )"""
        return query
    
def create_tables(table_properties: dict, db_location: str, db_name: str)->None:
    """
    runs the query against the database to create the tables in table_properties.
    
    Parameters:
    table_properties : dict of all the tables and their attributes
    db_location : location to store the database
    db_name : name of the dabase
    """
    db_location = db_location if db_location[-1] == "/" else db_location + "/"
    db_name = db_name if db_name[-3:] == ".db" else db_name+".db"
    path_to_db = db_location + db_name
    connection = sqlite3.connect(path_to_db)
    cursor = connection.cursor()
    logging.info(f"Tables to create {list(table_properties.keys())}")
    for table_name in table_properties.keys():
        query = table_creation_query(table_properties=table_properties, table_name=table_name)
        logging.info(f"query to excecute: {query}")  
        try:
            cursor.executescript(query)
            logging.info({f"Tables {table_name} successfully created in {db_name}"})
        except Exception as e:
            logging.debug({f"Table Creation Failed : {e}"})
    connection.close()
            
def list_tables(db_location: str, db_name: str) -> list[tuple[str, ...]]:
    """
    Lists the existing table in db
    
    Parameters:
    db_location : location to store the database
    db_name : name of the dabase
    """
    db_location = db_location if db_location[-1] == "/" else db_location + "/"
    db_name = db_name if db_name[-3:] == ".db" else db_name+".db"
    path_to_db = db_location + db_name
    connection = sqlite3.connect(path_to_db)
    cursor = connection.cursor()
    tables = cursor.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    connection.close()
    return tables

def insert_rows_with_sql(df: pd.DataFrame, db: str) -> None:
    """
    Inserts rows in existing database
    
    Parameters:
    df : pandas dataframe with the rows to add to the database
    db : name of the database to connect with
    table : table to add rows in
    """  
    

def insert_rows_with_pandas(df: pd.DataFrame, db: str, table: str) -> None:
    """
    Inserts rows in existing database using pandas
    
    Parameters:
    df : pandas dataframe with the rows to add to the database
    db : name of the database to connect with
    table : table to add rows in
    """  
    