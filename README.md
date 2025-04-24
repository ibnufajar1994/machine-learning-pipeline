![Machine Learning Logo](https://github.com/user-attachments/assets/2f2d6a0a-d6e5-42ee-8adf-d8f2466ae47b)

# Machine Learning Data Pipeline
## Build an Integrated ETL Pipeline for Predictive Models

---

### Table of Contents

1. [Overview](#1-overview)  
2. [Source of Dataset](#2-source-of-dataset)  
3. [Problem Description](#3-problem-description)  
4. [Proposed Solution](#4-proposed-solution)  
5. [Data Pipeline Design](#5-data-pipeline-design)  
6. [How to Use This Repository](#6-how-to-use-this-repository)  
   - [Preparations](#preparations)  
   - [Running the Project](#running-the-project)  

---

## 1. Overview

Welcome to the **Machine Learning Data Pipeline** project! This repository presents a comprehensive solution for managing car sales transaction data and creating a predictive model. The pipeline automates the **ETL (Extract, Transform, Load)** process by pulling data from multiple sources, cleaning, transforming, and storing it in a warehouse. We then use this cleaned data to build a **linear regression model** for predictive analysis. The trained model is saved in **MinIO object storage**, ready for use in production systems.

This project is designed to streamline the process of turning raw data into actionable insights, with an emphasis on automation, scalability, and simplicity.

## 2. Source of Dataset

The dataset is sourced from multiple platforms to ensure a diverse and rich collection of data:

- **Car Sales Transaction Data**  
  - [Repository](https://github.com/Kurikulum-Sekolah-Pacmann/data_pipeline_exercise_4)
  
- **Car Brand Data**  
  - [Google Spreadsheet](https://docs.google.com/spreadsheets/d/1nQi-YDX9KRs5mmqT-iAC67nz4QyKLUyLOUpQatY5wOI/edit?gid=0#gid=0)
  
- **US State Information**  
  - [API Endpoint](https://raw.githubusercontent.com/Kurikulum-Sekolah-Pacmann/us_states_data/refs/heads/main/us_states.json)

## 3. Problem Description

The car sales data is scattered across different platforms, making it difficult to work with in its raw form. In order to use this data for Machine Learning, it requires a series of transformations and cleaning steps. These include:

- **Data Extraction** from PostgreSQL, Google Spreadsheets, and REST APIs.
- **Data Transformation** to clean, format, and structure the data for analysis.
- **Data Loading** into a Data Warehouse for efficient storage and retrieval.

This project tackles these challenges by creating an automated pipeline that extracts, transforms, and loads the data into a centralized system, ensuring that the data is ready for predictive modeling.

## 4. Proposed Solution

### Our approach involves the following steps:

1. **Data Extraction**:  
   We pull data from various sources (PostgreSQL, Google Spreadsheets, and REST APIs).
   
2. **Data Transformation**:  
   The raw data is staged in a PostgreSQL database. Here, we perform data profiling to identify issues and inconsistencies. After that, we clean and transform the data into a suitable format for analysis.

3. **Data Loading**:  
   The cleaned data is loaded into a Data Warehouse for easy access and management.

4. **Machine Learning**:  
   We use a **linear regression model** to predict car sales trends. The trained model is saved in **MinIO object storage** for deployment in production environments.

### Technology Stack:
- **Python**: For the pipeline code and machine learning.
- **PostgreSQL**: For storing raw and transformed data.
- **Jupyter Notebooks**: For data analysis and model training.
- **Google Spreadsheet & API**: For sourcing car brand data.
- **scikit-learn**: For building the linear regression model.
- **MinIO**: For object storage of the model.

## 5. Data Pipeline Design

Here’s a visual overview of how the ETL pipeline works:

![ETL PIPELINE](https://github.com/user-attachments/assets/829f8a88-502b-44a8-a564-900f4ac0fcd0)

The pipeline flows from data extraction through transformation, and finally to model training and storage.

## 6. How to Use This Repository

### Preparations

1. **Clone the repository:**

   First, clone this repository to your local machine or server:
   ```bash
   git clone https://github.com/ibnufajar1994/pyspark-pipeline.git
