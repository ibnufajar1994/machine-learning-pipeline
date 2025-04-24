import pandas as pd
from src.utils.engine import init_engine
from src.utils.load_log import LOAD_LOG
from datetime import datetime



def extract_db_staging(table_name: str) -> pd.DataFrame:
    try:

        src_engine = init_engine("staging")

        df_data = pd.read_sql(sql = f"select * from {table_name}",
                              con = src_engine)

        log_msg = {
            "step" : "warehouse",
            "component":"extraction",
            "status": "success!",
            "table_name": table_name,
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),  # Current timestamp
        }

        return df_data

    except Exception as e:

        log_msg = {
            "step" : "staging",
            "component":"extraction",
            "status": "failed",
            "table_name": table_name,
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 
            "error_msg": str(e)
        }

    finally:
        LOAD_LOG(log_msg)
