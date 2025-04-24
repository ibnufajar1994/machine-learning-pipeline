from src.staging.staging_pipeline import Staging_Pipeline
from src.warehouse.warehouse_pipeline import Warehouse_Pipeline
from src.modelling.modelling_pipeline import Modelling_Pipeline
if __name__ == "__main__":
    Staging_Pipeline()
    Warehouse_Pipeline()
    Modelling_Pipeline()
