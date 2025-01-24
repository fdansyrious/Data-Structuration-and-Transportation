import os

PROJECT_FOLDER = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DATA_FOLDER = os.path.join(PROJECT_FOLDER, 'data')
DATABASE_FOLDER = os.path.join(PROJECT_FOLDER, 'db')
REPORTS_FOLDER = os.path.join(PROJECT_FOLDER, 'reports')
DB_NAME = "school_mngt_db.db"
