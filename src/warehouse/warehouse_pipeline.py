def Warehouse_Pipeline():

    #Import relevant Module
    from src.warehouse.extract import extract_db_staging
    from src.warehouse.profiling import Profiling
    from src.warehouse.transform import Transformation
    from src.warehouse.load import load_to_warehouse

 #---------------------------------EXTRACT DATABASE------------------------------------#
    print('-------------------Start Extracting Data-------------------')
    df1 = extract_db_staging("car_sales")
    df2 = extract_db_staging("car_brand")
    df3 = extract_db_staging("us_state")

    print('-------------------Finish Extracting Data-------------------')
#--------------------------------------------------------------------------------------#


#---------------------------------PROFILING DATA---------------------------------------#
    print('-------------------Start Profiling Data-------------------')

    df1_profiling = Profiling(data= df1, table_name="car_sales")
    data_type = df1_profiling.get_columns()
    duplicate_value_col = ["id_sales"]
    unique_value_col = ["brand_car", "body", "transmission", "state", "color", "interior"]
    missing_value_col = data_type
    negative_value_col = ["year", "condition", "odometer", "mmr", "sellingprice"]

    df1_profiling.selected_columns(data_type, duplicate_value_col, unique_value_col, missing_value_col, negative_value_col)
    df1_profiling.reporting()

    print('-------------------Finish Profiling Data-------------------')
#--------------------------------------------------------------------------------------#

#---------------------------------Transform DATA---------------------------------------#
    print('-------------------Start Transforming Data-------------------')

    df1_transform = Transformation(data=df1, table_name="car_sales")
    df2_transform = Transformation(data=df2, table_name="car_brand")
    df3_transform = Transformation(data=df3, table_name="us_state")

    ## Transformation of df1 (car_sales data)
    missing_value_col = ["odometer", "mmr", "condition"]
    invalid_value_col = ["brand_car", "model", "trim", "body", "transmission", "vin", "state", "color", "interior", "seller"]
    invalid_value = ["", "—","3vwd17aj5fm219943", "3vwd17aj5fm297123"]
    to_lowercase_col = ["brand_car", "model", "trim", "body", "transmission", "color", "interior", "seller"]

    df1_clean = df1_transform.drop_missing_value(missing_value_col)
    df1_clean = df1_transform.drop_invalid_value(invalid_value_col, invalid_value)
    df1_clean = df1_transform.to_lower_case(to_lowercase_col)

    ## Transformation of df2 (car_brand data)
    to_lowercase_col2 = ["brand_name"]
    df2_clean = df2_transform.to_lower_case(to_lowercase_col2)

    ## Transformation of df3 (us_state data)
    to_lowercase_col3 = ["name"]
    df3_clean = df3_transform.to_lower_case(to_lowercase_col3)

    ## Merged Dataframe df1, df2 and df3
    df1_clean = df1_transform.join_data(df2_clean, df3_clean)

    # Select only necessary column based on data warehouse schema
    df1_clean = df1_transform.select_merged_columns()


    # Cast column based on data warehouse schema
    df1_clean = df1_transform.cast_columns()

    # Renaming column based on data warehouse schema
    df1_clean = df1_transform.rename_columns()

    print('-------------------Finish Transforming Data-------------------')

#--------------------------------------------------------------------------------------#


#---------------------------------LOAD  DATA---------------------------------------#
    print('-------------------Start Loading Data to Warehouse-------------------')

    load_to_warehouse(data=df1_clean, table_name="car_sales")

    print('-------------------Finish Loading Data to Warehouse-------------------')

#--------------------------------------------------------------------------------------#