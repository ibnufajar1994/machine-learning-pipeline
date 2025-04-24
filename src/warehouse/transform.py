from src.utils.load_log import LOAD_LOG
from datetime import datetime

import pandas as pd
class Transformation():


    def __init__(self, data: pd.DataFrame, table_name: str) -> None:
        self.data = data
        self.table_name = table_name
        self.columns = data.columns

    
    def drop_missing_value(self, col_name):
        try:
            if isinstance(col_name, str):
                col_name = [col_name]
            
            self.data = self.data.dropna(subset=col_name)

            # Update column information
            self.columns = self.data.columns

                    
            log_msg = {
                    "step" : "Warehouse",
                    "component":"Transformation (Drop Missing Value)",
                    "status": "success!",
                    "table_name": self.table_name,
                    "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Current timestamp
                }
                

            return self.data
        
        except Exception as e:
            
            log_msg = {
                    "step" : "Warehouse",
                    "component":"Transformation (Drop Missing Value)",
                    "status": "Failed!",
                    "table_name": self.table_name,
                    "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),  # Current timestamp
                    "error_msg": str(e)
                }

            
        finally:
            LOAD_LOG(log_msg)

    
    def drop_invalid_value(self, col_names: list, invalid_values: list):
        try:

            # Iterasi setiap kolom yang ada dalam col_names
            for col_name in col_names:
                # Hapus baris yang memiliki nilai invalid pada kolom ini
                self.data = self.data[~self.data[col_name].isin(invalid_values)]

            # Update column information
            self.columns = self.data.columns

            # Logging message
            log_msg = {
                    "step" : "Warehouse",
                    "component": "Transformation (Drop Invalid Value)",
                    "status": "success!",
                    "table_name": self.table_name,
                    "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Current timestamp
                }

            return self.data

        except Exception as e:
            # Logging error message
            log_msg = {
                    "step" : "Warehouse",
                    "component": "Transformation (Drop Invalid Value)",
                    "status": "Failed!",
                    "table_name": self.table_name,
                    "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),  # Current timestamp
                    "error_msg": str(e)
                }

        finally:
            # Ensure that log is written regardless of success or failure
            LOAD_LOG(log_msg)


    def to_lower_case(self, col_names: list):
        try:

            # Ubah semua nilai di kolom yang ditentukan menjadi huruf kapital
            for col_name in col_names:
                self.data[col_name] = self.data[col_name].str.lower()

            # Update column information
            self.columns = self.data.columns

            # Logging message
            log_msg = {
                    "step" : "Warehouse",
                    "component": "Transformation (To Lower Case)",
                    "status": "success!",
                    "table_name": self.table_name,
                    "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Current timestamp
                }

            return self.data

        except Exception as e:
            # Logging error message
            log_msg = {
                    "step" : "Warehouse",
                    "component": "Transformation (To Lower Case)",
                    "status": "Failed!",
                    "table_name": self.table_name,
                    "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),  # Current timestamp
                    "error_msg": str(e)
                }

        finally:
            # Ensure that log is written regardless of success or failure
            LOAD_LOG(log_msg)


    def join_data(self, df1: pd.DataFrame, df2: pd.DataFrame):
        try:
            # Simpan dataframe asli (self.data) sebagai df
            df = self.data
            
            # Join df dengan df1 berdasarkan brand_car dan brand_name
            merged_df = df.merge(
                df1,
                left_on="brand_car",
                right_on="brand_name",
                how="left"
            )
            
            # Join hasil merge pertama dengan df2 berdasarkan state dan code
            final_df = merged_df.merge(
                df2,
                left_on="state",
                right_on="code",
                how="left"
            )
            
            # Update dataframe internal
            self.data = final_df
            
            # Update column information
            self.columns = self.data.columns
            
            # Logging message
            log_msg = {
                "step": "Warehouse",
                "component": "Transformation (Join Data)",
                "status": "success!",
                "table_name": self.table_name,
                "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Current timestamp
            }
            
            return self.data
            
        except Exception as e:
            # Logging error message
            log_msg = {
                "step": "Warehouse",
                "component": "Transformation (Join Data)",
                "status": "Failed!",
                "table_name": self.table_name,
                "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),  # Current timestamp
                "error_msg": str(e)
            }
            
        finally:
            # Ensure that log is written regardless of success or failure
            LOAD_LOG(log_msg)


    def select_merged_columns(self):
        try:
            # Daftar kolom yang dipilih (TANPA brand_car!)
            selected_columns = [
                "id_sales", "year", "brand_car_id", "transmission", "id_state", "odometer", 
                "condition", "color", "interior", "mmr", "sellingprice"
            ]
            
            # Cek kolom yang tersedia
            for col in selected_columns:
                if col not in self.data.columns:
                    print(f"Warning: Column '{col}' not found in dataframe")
            
            # Filter hanya kolom yang benar-benar ada
            valid_columns = [col for col in selected_columns if col in self.data.columns]
            
            # Memperbarui self.data secara internal
            self.data = self.data[valid_columns]
            
            # Update column information
            self.columns = self.data.columns
            
            
            log_msg = {
                "step": "Warehouse",
                "component": "Transformation (Select Merged Columns)",
                "status": "success!",
                "table_name": self.table_name,
                "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            
            return self.data
            
        except Exception as e:
            print(f"Error in select_merged_columns: {str(e)}")
            log_msg = {
                "step": "Warehouse",
                "component": "Transformation (Select Merged Columns)",
                "status": "Failed!",
                "table_name": self.table_name,
                "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "error_msg": str(e)
            }
            
        finally:
            LOAD_LOG(log_msg)

    def cast_columns(self):
        try:
            # Dictionary mapping tipe data string ke tipe data pandas
            type_mapping = {
                "integer": "int64",
                "float": "float64",
                "string": "object"
            }
            
            # Dictionary kolom dan tipe data target
            data_types = {
                "id_sales": "integer",
                "year": "integer",
                "brand_car_id": "integer",
                "transmission": "string",
                "id_state": "integer",
                "condition": "float", 
                "odometer": "float",
                "color": "string",
                "interior": "string",
                "mmr": "float",
                "sellingprice": "float"
            }
            
            # Seleksi hanya kolom yang ada di dataframe
            existing_columns = [col for col in data_types.keys() if col in self.data.columns]
            
            # Lakukan casting untuk setiap kolom
            for column in existing_columns:
                try:
                    target_type = type_mapping[data_types[column]]
                    
                    # Tangani nilai yang mungkin error saat konversi
                    if data_types[column] in ["integer", "float"]:
                        # Konversi ke numerik, dengan coercing errors menjadi NaN
                        self.data[column] = pd.to_numeric(self.data[column], errors='coerce')
                    
                    # Terapkan tipe data
                    self.data[column] = self.data[column].astype(target_type)
                    
                except Exception as e:
                    print(f"Error casting column {column}: {str(e)}")
            
            # Logging message
            log_msg = {
                "step": "Warehouse",
                "component": "Transformation (Cast Columns)",
                "status": "success!",
                "table_name": self.table_name,
                "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            
            return self.data
            
        except Exception as e:
            # Logging error message
            log_msg = {
                "step": "Warehouse",
                "component": "Transformation (Cast Columns)",
                "status": "Failed!",
                "table_name": self.table_name,
                "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "error_msg": str(e)
            }
            
        finally:
            # Ensure that log is written regardless of success or failure
            LOAD_LOG(log_msg)

    def rename_columns(self):
        try:
            # Dictionary mapping nama kolom original ke nama kolom baru
            column_mapping = {
                "id_sales": "id_sales_nk",
                "sellingprice": "selling_price"
            }
            
            # Filter hanya kolom yang ada di dataframe
            valid_columns = {k: v for k, v in column_mapping.items() if k in self.data.columns}
            
            # Rename kolom
            if valid_columns:
                self.data = self.data.rename(columns=valid_columns)
                
                # Update column information
                self.columns = self.data.columns
            
            # Logging message
            log_msg = {
                "step": "Warehouse",
                "component": "Transformation (Rename Columns)",
                "status": "success!",
                "table_name": self.table_name,
                "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            
            return self.data
            
        except Exception as e:
            # Logging error message
            log_msg = {
                "step": "Warehouse",
                "component": "Transformation (Rename Columns)",
                "status": "Failed!",
                "table_name": self.table_name,
                "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "error_msg": str(e)
            }
            
        finally:
            # Ensure that log is written regardless of success or failure
            LOAD_LOG(log_msg)


        
