import pandas as pd
from src.utils.engine import init_engine
from src.utils.load_log import LOAD_LOG
from datetime import datetime

def extract_warehouse(table_name: str) -> pd.DataFrame:
    try:
        # Initialize database engine
        src_engine = init_engine("warehouse")

        # Extract data from SQL table
        df_data = pd.read_sql(sql=f"select * from {table_name}",
                              con=src_engine)

        # Exclude the last column ('created_at')
        df_data = df_data.iloc[:, :-1]  # Or use df_data.drop(columns=["created_at"])

        # Log success message
        log_msg = {
            "step": "modelling",
            "component": "extraction",
            "status": "success!",
            "table_name": table_name,
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

        return df_data

    except Exception as e:
        # Log failure message in case of an error
        log_msg = {
            "step": "modelling",
            "component": "extraction",
            "status": "failed",
            "table_name": table_name,
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "error_msg": str(e)
        }

    finally:
        # Save the log
        LOAD_LOG(log_msg)
