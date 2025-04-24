from src.modelling.extract import extract_warehouse
from src.modelling.Car_Price_Model import CarPriceModel

def Modelling_Pipeline():

    print("------------Start Ectract Data from Warehouse-------------------")

    df = extract_warehouse(table_name="car_sales")

    print("------------Finish Ectract Data from Warehouse-------------------")
    print("=================================================================")
    print("------------Start Modelling Data --------------------------------")

    df_model = CarPriceModel(data=df)

    metrics = df_model.run_pipeline(store=True)

    print("------------Finish Modelling Data -------------------------------")