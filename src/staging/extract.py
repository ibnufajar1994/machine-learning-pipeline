import pandas as pd
import os
from dotenv import load_dotenv
from src.utils.helper import auth_gspread
from src.utils.engine import init_engine
from src.utils.load_log import LOAD_LOG
from datetime import datetime
import requests

load_dotenv()

GS_KEY = os.getenv("GS_KEY")
CRED_PATH = os.getenv("CRED_PATH")
worksheet_name = os.getenv("WORKSHEET_NAME")
link_api = os.getenv("LINK_API")

def extract_spreadsheet():
    try:

        gc = auth_gspread()
            
        # init spreadsheet by key
        sheet_result = gc.open_by_key(GS_KEY)
                
        # read spreadsheet data
        worksheet_result = sheet_result.worksheet(worksheet_name)

        # convert it to dataframe
        df_result = pd.DataFrame(worksheet_result.get_all_values())
                
        # set first rows as headers columns
        df_result.columns = df_result.iloc[0]
                
        # get all the rest of the values
        df_result = df_result[1:].copy()

        log_msg = {
            "step" : "staging",
            "component":"extraction",
            "status": "success!",
            "table_name": "brand_car",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),  # Current timestamp
        }
        
        return df_result
    
    except Exception as e:
        log_msg = {
            "step" : "staging",
            "component":"extraction",
            "status": "failed!",
            "table_name": "brand_car",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),  # Current timestamp
            "error_msg": str(e)
        }

    finally:
        LOAD_LOG(log_msg)
        
def extract_db_source(table_name: str) -> pd.DataFrame:
    try:

        src_engine = init_engine("source")

        df_data = pd.read_sql(sql = f"select * from {table_name}",
                              con = src_engine)

        log_msg = {
            "step" : "staging",
            "component":"extraction",
            "status": "success!",
            "table_name": "car_sales",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),  # Current timestamp
        }

        return df_data

    except Exception as e:

        log_msg = {
            "step" : "staging",
            "component":"extraction",
            "status": "failed",
            "table_name": "car_sales",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 
            "error_msg": str(e)
        }

    finally:
        LOAD_LOG(log_msg)



def extract_api():
    try:
        response = requests.get(link_api)
        
        data = response.json()
        
        # Jika data memiliki kunci 'regions', gunakan itu
        if 'regions' in data:
            df = pd.DataFrame(data['regions'])
        else:
            # Jika tidak, gunakan seluruh data
            df = pd.DataFrame(data)
        
        log_msg = {
            "step": "staging",
            "component": "extraction",
            "status": "success!",
            "table_name": "us_state",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

        return df
    
    except Exception as e:
        log_msg = {
            "step": "staging",
            "component": "extraction",
            "status": "failed",
            "table_name": "us_state",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 
            "error_msg": str(e)
        }

    finally:
        LOAD_LOG(log_msg)