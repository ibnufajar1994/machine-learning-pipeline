import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.model_selection import GridSearchCV
import numpy as np
from datetime import datetime
import joblib
import os
from minio import Minio
from src.utils.load_log import LOAD_LOG

class CarPriceModel:
    def __init__(self, data: pd.DataFrame) -> None:
        """
        Initialize the car price prediction model
        
        Parameters:
        -----------
        data : pd.DataFrame
            DataFrame containing car data
        """
        self.data = data
        self.features = ['odometer_log', 'condition', 'car_age', 'brand_car_id', 'transmission', 'color', 'mmr']
        self.target = 'selling_price'
        self.model = None
        self.scaler = StandardScaler()
        self.X_train = None
        self.X_test = None
        self.y_train = None
        self.y_test = None
        self.X_encoded = None
        self.y = None
        
    def feature_engineering(self):
        """
        Create new features needed for the model
        """       
        try:


            # Calculate the car age based on the reference year 2015
            self.data['car_age'] = 2015 - self.data['year']
            
            # Log transformation for the odometer
            self.data['odometer_log'] = np.log1p(self.data['odometer'])

            # Log success message
            log_msg = {
                "step": "modelling",
                "component": "Feature Engineering",
                "status": "success!",
                "table_name": "car_price",
                "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }

            
            return self
        
        except Exception as e:
            # Log failure message in case of an error
            log_msg = {
                "step": "modelling",
                "component": "Feature Engineering",
                "status": "failed",
                "table_name": "car_price",
                "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "error_msg": str(e)
            }

        finally:
            # Save the log
            LOAD_LOG(log_msg)
    
    def prepare_data(self):
        """
        Prepare data for modeling, including encoding categorical features
        """
        try:
            # One-hot encoding for categorical variables
            self.X_encoded = pd.get_dummies(self.data[self.features], drop_first=True)
            
            # Target is 'selling_price'
            self.y = self.data[self.target]
            # Log success message
            log_msg = {
                "step": "modelling",
                "component": "Prepare Data",
                "status": "success!",
                "table_name": "car_price",
                "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
            
            return self
        
        except Exception as e:
            # Log failure message in case of an error
            log_msg = {
                "step": "modelling",
                "component": "Prepare Data",
                "status": "failed",
                "table_name": "car_price",
                "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "error_msg": str(e)
            }

        finally:
            # Save the log
            LOAD_LOG(log_msg)
    
    def scale_features(self):
        """
        Normalize features using StandardScaler
        """
        try:

            self.X_scaled = self.scaler.fit_transform(self.X_encoded)

            log_msg = {
                "step": "modelling",
                "component": "Scale Features",
                "status": "success!",
                "table_name": "car_price",
                "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }

            return self
        
        except Exception as e:
            # Log failure message in case of an error
            log_msg = {
                "step": "modelling",
                "component": "Scale Feature",
                "status": "failed",
                "table_name": "car_price",
                "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "error_msg": str(e)
            }

        finally:
            # Save the log
            LOAD_LOG(log_msg)
    
    def split_data(self, test_size=0.2, random_state=42):
        """
        Split data into training and testing sets
        
        Parameters:
        -----------
        test_size : float, default=0.2
            Proportion of dataset to be used for testing
        random_state : int, default=42
            Random state for reproducibility
        """
        try:
            self.X_train, self.X_test, self.y_train, self.y_test = train_test_split(
                self.X_scaled, self.y, test_size=test_size, random_state=random_state
            )

            log_msg = {
                "step": "modelling",
                "component": "Splitting Data",
                "status": "success!",
                "table_name": "car_price",
                "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }

            return self
        
        except Exception as e:
            # Log failure message in case of an error
            log_msg = {
                "step": "modelling",
                "component": "Splitting Data",
                "status": "failed",
                "table_name": "car_price",
                "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "error_msg": str(e)
            }

        finally:
            # Save the log
            LOAD_LOG(log_msg)

    
    def train_model(self, n_estimators=100, random_state=42):
        """
        Train the RandomForestRegressor model
        
        Parameters:
        -----------
        n_estimators : int, default=100
            Number of trees in the random forest
        random_state : int, default=42
            Random state for reproducibility
        """
        try:
            self.model = RandomForestRegressor(n_estimators=n_estimators, random_state=random_state)
            self.model.fit(self.X_train, self.y_train)

            log_msg = {
                "step": "modelling",
                "component": "Train Model",
                "status": "success!",
                "table_name": "car_price",
                "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }

            return self

        except Exception as e:
            # Log failure message in case of an error
            log_msg = {
                "step": "modelling",
                "component": "Train Model",
                "status": "failed",
                "table_name": "car_price",
                "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "error_msg": str(e)
            }

        finally:
            # Save the log
            LOAD_LOG(log_msg)


    def evaluate_model(self):
        """
        Evaluate the model using test data
        
        Returns:
        --------
        dict
            Dictionary containing model evaluation metrics (MSE and R²)
        """
        try:

            log_msg = {
                "step": "modelling",
                "component": "Evaluate Model",
                "status": "success!",
                "table_name": "car_price",
                "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }

            y_pred = self.model.predict(self.X_test)
            
            mse = mean_squared_error(self.y_test, y_pred)
            r2 = r2_score(self.y_test, y_pred)
            
            metrics = {
                'mean_squared_error': mse,
                'r2_score': r2
            }
            
            return metrics
        
        except Exception as e:
            # Log failure message in case of an error
            log_msg = {
                "step": "modelling",
                "component": "Evaluate Model",
                "status": "failed",
                "table_name": "car_price",
                "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "error_msg": str(e)
            }

        finally:
            # Save the log
            LOAD_LOG(log_msg)
    
    def tune_hyperparameters(self, param_grid=None):
        """
        Perform hyperparameter tuning using GridSearchCV
        
        Parameters:
        -----------
        param_grid : dict, default=None
            Grid of parameters for tuning
        
        Returns:
        --------
        dict
            Best parameters and best score
        """
        try:
            if param_grid is None:
                param_grid = {
                    'n_estimators': [50, 100, 200],
                    'max_depth': [None, 10, 20, 30],
                    'min_samples_split': [2, 5, 10],
                    'min_samples_leaf': [1, 2, 4]
                }
            
            grid_search = GridSearchCV(
                RandomForestRegressor(random_state=42),
                param_grid=param_grid,
                cv=5,
                scoring='neg_mean_squared_error',
                n_jobs=-1
            )
            
            grid_search.fit(self.X_train, self.y_train)
            
            self.model = grid_search.best_estimator_

            log_msg = {
                "step": "modelling",
                "component": "Tune Hyperparameter",
                "status": "success!",
                "table_name": "car_price",
                "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
           
            
            return {
                'best_params': grid_search.best_params_,
                'best_score': -grid_search.best_score_  # Negation because scoring is neg_mean_squared_error
            }
        

        except Exception as e:
            # Log failure message in case of an error
            log_msg = {
                "step": "modelling",
                "component": "Tune Hyperparameter",
                "status": "failed",
                "table_name": "car_price",
                "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "error_msg": str(e)
            }

        finally:
            # Save the log
            LOAD_LOG(log_msg) 

    def predict(self, X):
        """
        Make predictions with the trained model
        
        Parameters:
        -----------
        X : array-like
            Features for prediction
        
        Returns:
        --------
        array
            Predicted car price values
        """
        try:
        # Ensure the given features are consistent with the features used for training
            if isinstance(X, pd.DataFrame):
                # If X is a DataFrame, perform feature engineering
                X_copy = X.copy()
                X_copy['car_age'] = 2015 - X_copy['year']
                X_copy['odometer_log'] = np.log1p(X_copy['odometer'])
                
                # One-hot encoding
                X_encoded = pd.get_dummies(X_copy[self.features], drop_first=True)
                
                # Ensure all columns used during training are present
                missing_cols = set(self.X_encoded.columns) - set(X_encoded.columns)
                for col in missing_cols:
                    X_encoded[col] = 0
                X_encoded = X_encoded[self.X_encoded.columns]
                
                # Scaling
                X_scaled = self.scaler.transform(X_encoded)
            else:
                # If X is already an array, directly transform
                X_scaled = self.scaler.transform(X)

            log_msg = {
                "step": "modelling",
                "component": "Predict",
                "status": "success!",
                "table_name": "car_price",
                "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }

            return self.model.predict(X_scaled)
        
        except Exception as e:
            # Log failure message in case of an error
            log_msg = {
                "step": "modelling",
                "component": "Predict",
                "status": "failed",
                "table_name": "car_price",
                "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "error_msg": str(e)
            }

        finally:
            # Save the log
            LOAD_LOG(log_msg) 
    
    def store_model(self, model_filename="car_price_model.pkl", bucket_name="car-sales-modelling", 
                    minio_host="localhost:9001", minio_access_key=None, minio_secret_key=None):
        """
        Save the machine learning model to MinIO object storage
        
        Parameters:
        -----------
        model_filename : str, default="car_price_model.pkl"
            File name for saving the model
        bucket_name : str, default="car_sales_modelling"
            Bucket name in MinIO
        minio_host : str, default="localhost:9000"
            MinIO server host and port
        minio_access_key : str, default=None
            Access key for MinIO authentication (if None, will be taken from environment variables)
        minio_secret_key : str, default=None
            Secret key for MinIO authentication (if None, will be taken from environment variables)
            
        Returns:
        --------
        bool
            True if the model is successfully saved to MinIO, False if failed
        """
        if self.model is None:
            print("The model is not trained. Please train the model first with the train_model() method.")
            return False
        
        try:
            # Get credentials from environment variables if not provided
            if minio_access_key is None:
                minio_access_key = os.getenv('MINIO_ACCESS_KEY')
            if minio_secret_key is None:
                minio_secret_key = os.getenv('MINIO_SECRET_KEY')
                
            if minio_access_key is None or minio_secret_key is None:
                print("Error: MINIO_ACCESS_KEY and MINIO_SECRET_KEY must be provided")
                return False
                
            # Save model to a local file first
            print(f"Saving model to local file: {model_filename}")
            joblib.dump(self.model, model_filename)
            
            # Also save the scaler since it's required for prediction
            scaler_filename = f"scaler_{model_filename}"
            joblib.dump(self.scaler, scaler_filename)
            
            # Save the column information for one-hot encoding
            columns_filename = f"columns_{model_filename}.pkl"
            joblib.dump(self.X_encoded.columns, columns_filename)
            
            # Initialize MinIO client
            print(f"Connecting to MinIO server: {minio_host}")
            client = Minio(
                minio_host,
                access_key=minio_access_key,
                secret_key=minio_secret_key,
                secure=False  # Set to True if using HTTPS
            )
            
            # Check and create the bucket if not exists
            if not client.bucket_exists(bucket_name):
                print(f"Bucket '{bucket_name}' not found. Creating new bucket.")
                client.make_bucket(bucket_name)
                
            # Date for versioning
            current_date = datetime.now().strftime("%Y%m%d_%H%M%S")
            model_version_filename = f"{current_date}_{model_filename}"
            
            # Upload model to MinIO
            print(f"Uploading model to bucket '{bucket_name}' with file name '{model_version_filename}'")
            client.fput_object(bucket_name, model_version_filename, model_filename)
            
            # Upload scaler to MinIO
            scaler_version_filename = f"{current_date}_scaler_{model_filename}"
            client.fput_object(bucket_name, scaler_version_filename, scaler_filename)
            
            # Upload column information to MinIO
            columns_version_filename = f"{current_date}_columns_{model_filename}"
            client.fput_object(bucket_name, columns_version_filename, columns_filename)
            
            # Remove local files after upload
            os.remove(model_filename)
            os.remove(scaler_filename)
            os.remove(columns_filename)
            
            print("Model, scaler, and column information successfully saved to MinIO.")

            log_msg = {
                "step": "modelling",
                "component": "Store Model",
                "status": "success!",
                "table_name": "car_price",
                "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }

            return True
        
        
        except Exception as e:
            # Log failure message in case of an error
            log_msg = {
                "step": "modelling",
                "component": "Predict",
                "status": "failed",
                "table_name": "car_price",
                "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "error_msg": str(e)
            }

            print(f"Error saving model to MinIO: {str(e)}")
            return False

        finally:
            # Save the log
            LOAD_LOG(log_msg) 
    
    def run_pipeline(self, tune=False, store=False):
        """
        Run the entire modeling pipeline from start to finish
        
        Parameters:
        -----------
        tune : bool, default=False
            Whether to perform hyperparameter tuning
        store : bool, default=False
            Whether to save the model to MinIO after training
            
        Returns:
        --------
        dict
            Model evaluation metrics
        """
        self.feature_engineering()
        self.prepare_data()
        self.scale_features()
        self.split_data()
        
        if tune:
            tuning_results = self.tune_hyperparameters()
            print(f"Best parameters: {tuning_results['best_params']}")
            print(f"Best MSE score: {tuning_results['best_score']}")
        else:
            self.train_model()
        
        metrics = self.evaluate_model()
        print(f"Mean Squared Error: {metrics['mean_squared_error']}")
        print(f"R-squared: {metrics['r2_score']}")
        
        if store:
            self.store_model()
        
        return metrics
