import pandas as pd
from src.utils.load_log import LOAD_LOG
from src.utils.engine import init_engine
from datetime import datetime


def load_to_warehouse(data: pd.DataFrame, table_name: str) -> None:
    try:
        stg_engine = init_engine("warehouse")

        data = data.copy()

        data.to_sql(name = table_name,
                        con = stg_engine,
                        if_exists = "append",
                        index = False)
        
        log_msg = {
            "step" : "warehouse",
            "component":"load",
            "status": "success!",
            "table_name": table_name,
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),  # Current timestamp
        }
        
    
    except Exception as e:
        log_msg = {
            "step" : "warehouse",
            "component":"load",
            "status": "failed!",
            "table_name": table_name,
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),  # Current timestamp
            "error_msg": str(e)
        }

    finally:
        LOAD_LOG(log_msg)
        stg_engine.dispose()