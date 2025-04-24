from dotenv import load_dotenv
from sqlalchemy import create_engine
import os

load_dotenv()
SRC_POSTGRES_DB = os.getenv("SRC_POSTGRES_DB")
SRC_POSTGRES_USER= os.getenv("SRC_POSTGRES_USER")
SRC_POSTGRES_PASSWORD= os.getenv("SRC_POSTGRES_PASSWORD")
SRC_POSTGRES_PORT= os.getenv("SRC_POSTGRES_PORT")


STG_POSTGRES_DB= os.getenv("STG_POSTGRES_DB")
STG_POSTGRES_USER = os.getenv("STG_POSTGRES_USER")
STG_POSTGRES_PASSWORD = os.getenv("STG_POSTGRES_PASSWORD")
STG_POSTGRES_PORT = os.getenv("STG_POSTGRES_PORT")

DWH_POSTGRES_DB = os.getenv("DWH_POSTGRES_DB")
DWH_POSTGRES_USER = os.getenv("DWH_POSTGRES_USER")
DWH_POSTGRES_PASSWORD = os.getenv("DWH_POSTGRES_PASSWORD")
DWH_POSTGRES_PORT = os.getenv("DWH_POSTGRES_PORT")

LOG_POSTGRES_DB = os.getenv("LOG_POSTGRES_DB")
LOG_POSTGRES_USER = os.getenv("LOG_POSTGRES_USER")
LOG_POSTGRES_PASSWORD = os.getenv("LOG_POSTGRES_PASSWORD")
LOG_POSTGRES_PORT = os.getenv("LOG_POSTGRES_PORT")

HOST = os.getenv("HOST")


def init_engine(engine_name: str):
    try:
        if engine_name.lower() == "source":
            conn_src = create_engine(f"postgresql://{SRC_POSTGRES_USER}:{SRC_POSTGRES_PASSWORD}@{HOST}:{SRC_POSTGRES_PORT}/{SRC_POSTGRES_DB}")

            return conn_src

        elif engine_name.lower() == "staging":
            conn_stg = create_engine(f"postgresql://{STG_POSTGRES_USER}:{STG_POSTGRES_PASSWORD}@{HOST}:{STG_POSTGRES_PORT}/{STG_POSTGRES_DB}")
            
            return conn_stg
        
        elif engine_name.lower() == "warehouse":
            conn_dwh = create_engine(f"postgresql://{DWH_POSTGRES_USER}:{DWH_POSTGRES_PASSWORD}@{HOST}:{DWH_POSTGRES_PORT}/{DWH_POSTGRES_DB}")
            
            return conn_dwh
        
        elif engine_name.lower() == "log":
            conn_log = create_engine(f"postgresql://{LOG_POSTGRES_USER}:{LOG_POSTGRES_PASSWORD}@{HOST}:{LOG_POSTGRES_PORT}/{LOG_POSTGRES_DB}")
            
            return conn_log
        
        else:
            raise Exception("Unknown engine name!")
        
    except Exception as e:
        raise Exception(e)