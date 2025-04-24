import pandas as pd
import json
from dotenv import load_dotenv
import os
from datetime import datetime
load_dotenv()

class Profiling:
    def __init__(self, data, table_name) -> None:

            
        self.data = data
        self.table_name = table_name
        self.list_columns = list(data.columns)

    
    def get_columns(self):
        return self.list_columns
    
    def selected_columns(self, data_type, duplicate_value, unique_value, missing_value, negative_value):
        self.data_type = data_type
        self.duplicate_value = duplicate_value
        self.unique_value = unique_value
        self.missing_value = missing_value
        self.negative_value = negative_value
        self.column_profiling = list(set(self.duplicate_value + self.data_type + self.unique_value + self.missing_value + self.negative_value))
            


    def check_data_type(self, col_name: str):
        """
        Check data type of column
        """
        data_type = self.data[col_name].dtypes
        return str(data_type)
    
    def check_duplicate_value(self, col_name: str):
        """
        Check duplicate value of column
        """    
        total_duplicate_value = self.data.duplicated(subset=[col_name]).sum()

        return total_duplicate_value
    
    def check_unique_value(self, col_name: str):
        """
        Check unique value of column
        """
        list_unique_value = list(self.data[col_name].unique())

        return list_unique_value

    def check_missing_value(self, col_name: str):
        """
        Check missing value of column
        """

        return self.data[col_name].isnull().sum()

    def check_negative_value(self, col_name):
        """
        Check negative value of column
        """

        col_name = pd.to_numeric(self.data[col_name], errors='coerce')
        
        # Hitung jumlah nilai negatif (abaikan NaN)
        return (col_name < 0).sum()
    

    def save_report(self):
        """
        Save report to a JSON file in the path specified in .env file
        
        Returns:
        str: Path of the saved file
        """
        # Get path from .env file
        base_path = os.getenv("FILE_PATH", ".")  # Default to current directory if not found
        
        current_date = datetime.now().strftime("%Y-%m-%d")
        file_name = f"{self.table_name}_{current_date}.json"
        file_path = os.path.join(base_path, file_name)
        
        # Make sure directory exists
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        # Write to file
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(self.dict_report, f, indent=4, ensure_ascii=False, default=str)
        
        return f"Report saved as {file_path}"

    def reporting(self):
        """
        Generate profling report
        """

        self.dict_report = {
            "created_at": datetime.now().strftime("%Y-%m-%d"),
            "report": {}
        }

        for col in self.list_columns:
            self.dict_report["report"][col] = {}

            if col in self.data_type:
                self.dict_report["report"][col]["data_type"] = self.check_data_type(col)

            if col in self.duplicate_value:
                self.dict_report["report"][col]["duplicate_value"] = self.check_duplicate_value(col)

            if col in self.unique_value:
                self.dict_report["report"][col]["unique_value"] = self.check_unique_value(col)

            if col in self.missing_value:
                self.dict_report["report"][col]["missing_value"] = self.check_missing_value(col)

            if col in self.negative_value:
                self.dict_report["report"][col]["negative_value"] = self.check_negative_value(col)

        print(self.dict_report)
        save_result = self.save_report()

        return self.dict_report
    

