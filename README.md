
![machine-learning-logo-design-png-5308](https://github.com/user-attachments/assets/2f2d6a0a-d6e5-42ee-8adf-d8f2466ae47b)

# Machine Learning Data Pipeline
## Build Integrated ETL Pipeline for Machine Learning Model 
---
### Table of Contents
[1. Overview](#1-overview)  
[2. Source of Dataset](#2-source-of-dataset)  
[3. Problem Description](#3-problem-description)  
[4. Proposed Solution](#4-proposed-solution)  
[5. Data Transformation Rule](#5-data-transformation-rule)  
[6. Data Pipeline Design](#6-pipeline-design)  
[7. How to Use This Repository](#7-how-to-use-this-repository)    
   - [Preparations](#preparations)    
   - [Running the Project](#running-the-project)    
   - [Access the Jupyter Notebook](#access-the-jupyter-notebook)  
---

## 1. Overview
This project sets up a comprehensive Data Pipeline and Machine Learning Pipeline for car sales transaction data. It automates the process of extracting, transforming, and loading (ETL) data from various sources, stores the cleaned data in a Data Warehouse, and develops a Machine Learning model for predictive analysis. The trained model is then saved in a MinIO object storage service for use in production.

## 2. Source of Dataset
- Cars Sales Transaction Data ([Repository](https://github.com/Kurikulum-Sekolah-Pacmann/data_pipeline_exercise_4))
- Car Brand Data ([Spreadsheet](https://docs.google.com/spreadsheets/d/1nQi-YDX9KRs5mmqT-iAC67nz4QyKLUyLOUpQatY5wOI/edit?gid=0#gid=0))
- Us State Information ([API](https://raw.githubusercontent.com/Kurikulum-Sekolah-Pacmann/us_states_data/refs/heads/main/us_states.json))

## 3. Problem Description
The car sales data is spread across various sources, making it difficult to use in its raw form for Machine Learning modeling. To ensure the data is suitable for analysis, it requires thorough cleaning, transformation, and mapping. These steps are essential to prepare the data for building predictive models. Given the complexity and volume of the data, a reliable and automated data pipeline is necessary to streamline the entire process, ensuring efficient extraction, transformation, and loading of the data, ultimately supporting the creation of accurate predictive models.

## 4. Proposed Solution
The solution approach begins by extracting data from multiple sources, including PostgreSQL, Google Spreadsheet, and REST APIs. The raw extracted data is then loaded into a Staging PostgreSQL database for further processing. Once in the staging area, the data extracted w. The workflow of ETL pipeline design is shown bellow:

![ETL PIPELINE](https://github.com/user-attachments/assets/829f8a88-502b-44a8-a564-900f4ac0fcd0)

