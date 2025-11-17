import datetime
import json
import sys
from pathlib import Path

import hopsworks
import pandas as pd
from xgboost import XGBRegressor

from mlfs import config
from mlfs.airquality import util

# Normalize the root directory path to handle execution from different subdirectories
root_dir = Path().absolute()
if root_dir.parts[-1:] == ('airquality',):
    root_dir = Path(*root_dir.parts[:-1])
if root_dir.parts[-1:] == ('notebooks',):
    root_dir = Path(*root_dir.parts[:-1])
root_dir = str(root_dir)
print("Local environment")

# Add the root directory to the PYTHONPATH to import the mlfs module
if root_dir not in sys.path:
    sys.path.append(root_dir)
print(f"Added the following directory to the PYTHONPATH: {root_dir}")

# Load configuration from .env file
settings = config.HopsworksSettings(_env_file=f"{root_dir}/.env")  # type: ignore

today = datetime.datetime.now() - datetime.timedelta(0)
tomorrow = today + datetime.timedelta(days=1)

project = hopsworks.login()
fs = project.get_feature_store()

# Retrieve sensor location information from Hopsworks secrets
secrets = hopsworks.get_secrets_api()
if not secrets:
    exit()
location_str = secrets.get_secret("SENSOR_LOCATION_JSON").value
if not location_str:
    exit()
location = json.loads(location_str)
country = location['country']
city = location['city']
street = location['street']

mr = project.get_model_registry()

# Retrieve the trained model from the model registry
retrieved_model = mr.get_model(
    name="air_quality_xgboost_model",
    version=1,
)

fv = retrieved_model.get_feature_view()

# Download the saved model artifacts to a local directory
saved_model_dir = retrieved_model.download()

# Load the XGBoost model from the JSON file
retrieved_xgboost_model = XGBRegressor()
retrieved_xgboost_model.load_model(saved_model_dir + "/model.json")

# Get weather data for future dates (today onwards) to make predictions
weather_fg = fs.get_feature_group(
    name='weather',
    version=1,
)
batch_data = weather_fg.filter(weather_fg.date >= today).read()
batch_data.sort_values(by=['date'], ascending=False)

# Use the model to predict PM2.5 values based on weather features
batch_data['predicted_pm25'] = retrieved_xgboost_model.predict(
    batch_data[['temperature_2m_mean', 'precipitation_sum', 'wind_speed_10m_max', 'wind_direction_10m_dominant']]
)

batch_data.info()

# Add location metadata to the predictions
batch_data['street'] = street
batch_data['city'] = city
batch_data['country'] = country
# Track how many days in advance this forecast was made (1 = tomorrow, 2 = day after, etc.)
batch_data['days_before_forecast_day'] = range(1, len(batch_data) + 1)
batch_data = batch_data.sort_values(by=['date'])

batch_data.info()

# Generate and save forecast plot
pred_file_path = f"{root_dir}/docs/air-quality/assets/img/pm25_forecast.png"
plt = util.plot_air_quality_forecast(city, street, batch_data, pred_file_path)

plt.show()

# Store predictions in feature store for monitoring and analysis
monitor_fg = fs.get_or_create_feature_group(
    name='aq_predictions',
    description='Air Quality prediction monitoring',
    version=1,
    primary_key=['city', 'street', 'date', 'days_before_forecast_day'],
    event_time="date",
)

monitor_fg.insert(batch_data, wait=True)
# Get predictions made 1 day in advance for model performance monitoring
monitoring_df = monitor_fg.filter(monitor_fg.days_before_forecast_day == 1).read()

# Retrieve actual air quality measurements to compare against predictions
air_quality_fg = fs.get_feature_group(name='air_quality', version=1)
air_quality_df = air_quality_fg.read()

# Merge predictions with actual measurements for model evaluation
outcome_df = air_quality_df[['date', 'pm25']]
preds_df = monitoring_df[['date', 'predicted_pm25']]

hindcast_df = pd.merge(preds_df, outcome_df, on="date")
hindcast_df = hindcast_df.sort_values(by=['date'])

# If no matching data exists, backfill historical predictions for monitoring
if len(hindcast_df) == 0:
    hindcast_df = util.backfill_predictions_for_monitoring(
        weather_fg, air_quality_df, monitor_fg, retrieved_xgboost_model
    )

# Generate and save hindcast plot (actual vs predicted values from 1-day-ahead forecasts)
hindcast_file_path = f"{root_dir}/docs/air-quality/assets/img/pm25_hindcast_1day.png"
plt = util.plot_air_quality_forecast(city, street, hindcast_df, hindcast_file_path, hindcast=True)
plt.show()

# Upload generated plots to Hopsworks for easy access and sharing
dataset_api = project.get_dataset_api()
str_today = today.strftime("%Y-%m-%d")
if not dataset_api.exists("Resources/airquality"):
    dataset_api.mkdir("Resources/airquality")
dataset_api.upload(pred_file_path, f"Resources/airquality/{city}_{street}_{str_today}", overwrite=True)
dataset_api.upload(hindcast_file_path, f"Resources/airquality/{city}_{street}_{str_today}", overwrite=True)

proj_url = project.get_url()
print(f"See images in Hopsworks here: {proj_url}/settings/fb/path/Resources/airquality")
