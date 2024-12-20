import os

PROJECT_FOLDER = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DATA_FOLDER = os.path.join(PROJECT_FOLDER, 'data')
DATABASE_FOLDER = os.path.join(PROJECT_FOLDER, 'db')
CONFIG_FOLDER = os.path.join(PROJECT_FOLDER, 'config')

TABLE_PROPERTIES_PATH = os.path.join(CONFIG_FOLDER, 'table_properties.json')