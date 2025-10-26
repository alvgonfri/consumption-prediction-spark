# ⚡ Automated Energy Consumption Prediction System

[![License](https://img.shields.io/badge/License-Apache_2.0-red.svg)](https://opensource.org/licenses/Apache-2.0)
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/alvgonfri/consumption-prediction-spark)

Automated system for predicting household energy consumption, developed as part of a Master’s Thesis. The project integrates **Apache Airflow** for workflow orchestration and **Apache Spark** for distributed data processing and machine learning, providing a scalable and reproducible end-to-end solution.

The project implements a fully automated pipeline for short-term energy consumption forecasting. It covers all stages of the machine learning lifecycle: from data ingestion and preprocessing to model training, evaluation, and prediction.

---

## 🛠️ Technologies used

The key technologies employed in this project include:

- **Python**: Main programming language of the project, used for its ease of integration with data science libraries and compatibility with Big Data tools like Spark or Airflow. Version 3.12.3 was used in this work.

- **Apache Spark**: Distributed processing platform designed for processing large volumes of data. Its Spark SQL module is used for structured data processing, and Spark MLlib is used for model training and evaluation. It was chosen due to the project’s focus on the Big Data paradigm. Version 4.0.0 was used in this work.

- **Apache Airflow**: Workflow orchestration tool that allows managing and monitoring data processing tasks through its web interface. Its architecture is based on Directed Acyclic Graphs (DAGs), and it promotes process automation and reproducibility. Version 3.0.3 was used in this work.

---

## 🗂️ Project structure

The project is organized into the following main directories and files:

- `airflow/dags/`: Contains the Airflow DAG definitions that orchestrate the entire pipeline.
- `dashboard/`: Contains the Streamlit application for visualizing predictions and model performance.
- `data/`: Directory for storing raw and processed datasets.
- `db/`: Directory for the SQLite database used for audit and traceability of model results.
- `models/`: Directory for saving trained machine learning models.
- `notebooks/`: Jupyter notebook used for exploratory data analysis.
- `scripts/`: Contains auxiliary python scripts for various tasks.
- `spark/`: Contains Spark job definitions for various pipeline stages.
- `.env.example`: Example environment configuration file.
- `.gitignore`: Specifies files and directories to be ignored by Git.
- `LICENSE`: The license under which the project is distributed.
- `README.md`: This file, providing an overview of the project.
- `requirements.txt`: Lists the Python dependencies required for the project.

---

## 📈 Dataset

The dataset used in this work, publicly available [here](https://fordatis.fraunhofer.de/handle/fordatis/215), contains information on energy consumption, measured in kilowatt-hours (kWh), from 499 anonymous customers located in Spain. The dataset covers the entire year of 2019, with observations recorded every hour.

In addition to energy consumption, meteorological data is also provided, specifically the outdoor temperature in the region of each customer, also measured on an hourly basis. Each customer is assigned one of 68 predefined customer profiles, such as private households, restaurants, or factories, which enables segmentation and analysis based on these categories.

---

## 🧩 Pipeline stages

The pipeline is composed of modular stages that automate the entire forecasting process:

- **Data Ingestion:** Loads and filters raw energy consumption data.  
- **Preprocessing:** Cleans and transforms data, creating time-based and statistical features.  
- **Model Training:** Trains multiple regression models using Spark MLlib.  
- **Model Evaluation:** Assesses predictive performance with standard regression metrics, storing results in a database for traceability.
- **Model Selection:** Automatically identifies the best-performing model, depending on evaluation metrics and computational efficiency.
- **Prediction:** Generates hourly consumption forecasts for the next 24 hours.

An alternative pipeline version is incorporates a **clustering** step before model training is also implemented. This versions uses K-Means to group households based on their consumption patterns. Separate models are trained for each cluster, potentially enhancing forecast accuracy by capturing group-specific behaviors.

---

## 📊 Visualization

An interactive **Streamlit dashboard** enables visual comparison between real and predicted energy consumption, both at an aggregate and individual level. It also allows switching between standard and cluster-based results, providing insights into model performance and forecast accuracy.
