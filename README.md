# Lab Report 1

The Goal of this lab was to build a serverless AI-System that predicts the Air Quality for Copenhagen.

## Dashboard

🔗 [View the Dashboard](https://adrianthees.github.io/mlfs-book/)

## How to run

### Backfill
`make features`

### Training
`make train`

### Inference
`make inference`

### Whole Project
`make all`

## Key Objectives
1. Predict PM2.5 air quality levels 7-10 days in advance
2. Support multiple sensors across the city
3. Compare model performance with and without temporal features
4. Automate daily feature updates and inference
5. Provide visual dashboards for monitoring and validation

## Pipeline

#### 1. Feature Backfill (`1_air_quality_feature_backfill.py`)
- Loads historical CSV data for all sensors
- Fetches historical weather data from Open-Meteo API
- Creates and populates feature groups in Hopsworks
- Calculates lagged features (1, 2, 3 days)
- Validates data using Great Expectations

#### 2. Daily Data Gathering (`2_air_quality_feature_pipeline.py`)
- **Scheduled:** Daily at 06:00 UTC via GitHub Actions
- Fetches yesterday's air quality from AQICN API
- Retrieves 10-day weather forecast
- Updates lagged features for all sensors
- Inserts new data into feature store

#### 3. Training (`3_air_quality_training_pipeline.py`)
- Creates feature views joining air quality and weather data
- Trains two models:
  - **Standard Model:** Weather features only
  - **Lagged Model:** Weather + historical PM2.5 values
- Evaluates using MSE and R² metrics
- Generates feature importance plots
- Registers models in Hopsworks Model Registry

#### 4. Batch Inference (`4_air_quality_batch_inference.py`)
- **Scheduled:** Daily after feature pipeline
- Downloads trained models from registry
- Generates predictions for each sensor
- Creates forecast plots (7-10 days ahead)
- Creates hindcast plots (predictions vs. actual)
- Uploads results to Hopsworks and GitHub Pages
