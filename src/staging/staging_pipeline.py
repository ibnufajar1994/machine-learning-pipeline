# Import relevant Module
def Staging_Pipeline():
    from src.staging.extract import extract_db_source, extract_spreadsheet, extract_api
    from src.staging.load import load_to_staging

    # Get data from various sources
    df_db = extract_db_source("car_sales")
    df_spreadsheet = extract_spreadsheet()
    df_api = extract_api()

    # Load data to staging database
    load_to_staging(data=df_db, table_name="car_sales")
    load_to_staging(data=df_spreadsheet, table_name="car_brand")
    load_to_staging(data=df_api, table_name="us_state")