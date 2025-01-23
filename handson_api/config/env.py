import os
from sqlmodel import Session, create_engine

PROJECT_FOLDER = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DATABASE_FOLDER = os.path.join(PROJECT_FOLDER, 'db')
CONFIG_FOLDER = os.path.join(PROJECT_FOLDER, 'config')
ROUTERS_FOLDER = os.path.join(PROJECT_FOLDER, 'endpoints')
DATABASE_NAME = "school_mngt_db.db"
DATABASE_URL = f"sqlite:///{DATABASE_FOLDER}/{DATABASE_NAME}"


CONNECTION_ARGS = {"check_same_thread": False}
ENGINE = create_engine(DATABASE_URL, connect_args=CONNECTION_ARGS)

def get_session():
    with Session(ENGINE) as session:
        yield session