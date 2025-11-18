import datetime
import json
import logging
import sys

import hopsworks
import pandas as pd
from xgboost import XGBRegressor

from mlfs import config
from mlfs.airquality import util

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Load configuration from .env file
settings = config.HopsworksSettings(_env_file=".env")  # type: ignore

today = datetime.datetime.now() - datetime.timedelta(0)
tomorrow = today + datetime.timedelta(days=1)

project = hopsworks.login()
fs = project.get_feature_store()

# Retrieve sensor location information from Hopsworks secrets
secrets = hopsworks.get_secrets_api()
if not secrets:
    logger.error("Error when retrieving secrets from hopsworks")
    sys.exit(1)

location_str = secrets.get_secret("SENSOR_LOCATION_JSON")
if not location_str:
    logger.error("Error when retrieving Location string from hopsworks")
    sys.exit(1)
location_str = location_str.value
if not location_str:
    logger.error("Error when extracting value string for Location string")
    sys.exit(1)
location = json.loads(location_str)

country = location["country"]
city = location["city"]
street = location["street"]

mr = project.get_model_registry()

# Retrieve the trained model from the model registry
retrieved_model = mr.get_model(
    name="air_quality_xgboost_model",
    version=1,
)

retrieved_lagged_model = mr.get_model(
    name="air_quality_lagged_xgboost_model",
    version=1,
)

# Get weather data for future dates (today onwards) to make predictions
weather_fg = fs.get_feature_group(
    name="weather",
    version=1,
)
batch_data = weather_fg.filter(weather_fg.date >= today).read()


def inference(model, name, batch_data):
    # Download the saved model artifacts to a local directory
    saved_model_dir = model.download()

    # Load the XGBoost model from the JSON file
    retrieved_xgboost_model = XGBRegressor()
    retrieved_xgboost_model.load_model(saved_model_dir + "/model.json")

    predict_columns = [
        "temperature_2m_mean",
        "precipitation_sum",
        "wind_speed_10m_max",
        "wind_direction_10m_dominant",
    ]

    if name == "air_quality_lagged":
        # Get lagged air quality features for prediction
        air_quality_lagged_fg = fs.get_feature_group(
            name="air_quality_lagged",
            version=1,
        )

        # Get the most recent lagged features
        # For forecasting, we use the latest available lagged values
        lagged_data = air_quality_lagged_fg.read()
        lagged_data = lagged_data.sort_values(by=["date"], ascending=False)

        # Get the most recent lagged values for this city
        latest_lags = lagged_data[lagged_data["city"] == city].head(1)

        if len(latest_lags) > 0:
            pm25_lag1 = latest_lags["pm25_lag1"].values[0]
            pm25_lag2 = latest_lags["pm25_lag2"].values[0]
            pm25_lag3 = latest_lags["pm25_lag3"].values[0]

            # Add lagged features to batch data
            batch_data["pm25_lag1"] = pm25_lag1
            batch_data["pm25_lag2"] = pm25_lag2
            batch_data["pm25_lag3"] = pm25_lag3

            logger.info(
                f"Using lagged features - lag1: {pm25_lag1:.2f}, lag2: {pm25_lag2:.2f}, lag3: {pm25_lag3:.2f}"
            )
        else:
            logger.info(
                "Warning: No lagged features found. Using zeros for lagged features."
            )
            batch_data["pm25_lag1"] = 0
            batch_data["pm25_lag2"] = 0
            batch_data["pm25_lag3"] = 0

        predict_columns.extend(["pm25_lag1", "pm25_lag2", "pm25_lag3"])

    # Use the model to predict PM2.5 values based on weather features (and lagged pm25)
    batch_data["predicted_pm25"] = retrieved_xgboost_model.predict(
        batch_data[predict_columns]
    )

    batch_data.info()

    # Add location metadata to the predictions
    batch_data["street"] = street
    batch_data["city"] = city
    batch_data["country"] = country
    # Track how many days in advance this forecast was made (1 = tomorrow, 2 = day after, etc.)
    batch_data["days_before_forecast_day"] = range(1, len(batch_data) + 1)
    batch_data = batch_data.sort_values(by=["date"])

    batch_data.info()

    # Generate and save forecast plot
    pred_file_path = f"./docs/{name}/assets/img/pm25_forecast.png"
    plt = util.plot_air_quality_forecast(city, street, batch_data, pred_file_path)

    plt.show()

    # Store predictions in feature store for monitoring and analysis
    monitor_fg = fs.get_or_create_feature_group(
        name=f"{name}_predictions",
        description=f"{name.replace('_', ' ').title()} prediction monitoring",
        version=1,
        primary_key=["city", "street", "date", "days_before_forecast_day"],
        event_time="date",
    )

    monitor_fg.insert(batch_data, wait=True)
    # Get predictions made 1 day in advance for model performance monitoring
    monitoring_df = monitor_fg.filter(monitor_fg.days_before_forecast_day == 1).read()

    # Retrieve actual air quality measurements to compare against predictions
    air_quality_fg = fs.get_feature_group(name=name, version=1)
    air_quality_df = air_quality_fg.read()

    # If we're working with lagged data, also get PM2.5 from normal air_quality
    if name == "air_quality_lagged":
        normal_air_quality_fg = fs.get_feature_group(name="air_quality", version=1)
        normal_air_quality_df = normal_air_quality_fg.read()
        # Join the pm25 value from normal air_quality
        air_quality_df = air_quality_df.merge(
            normal_air_quality_df[["date", "city", "pm25"]],
            on=["date", "city"],
            how="left",
            suffixes=("_lagged", ""),
        )

    # Merge predictions with actual measurements for model evaluation
    outcome_df = air_quality_df[["date", "pm25"]]
    preds_df = monitoring_df[["date", "predicted_pm25"]]

    hindcast_df = pd.merge(preds_df, outcome_df, on="date")
    hindcast_df = hindcast_df.sort_values(by=["date"])

    # If no matching data exists, backfill historical predictions for monitoring
    if len(hindcast_df) == 0:
        hindcast_df = util.backfill_predictions_for_monitoring(
            name, weather_fg, air_quality_df, monitor_fg, retrieved_xgboost_model
        )

    # Generate and save hindcast plot (actual vs predicted values from 1-day-ahead forecasts)
    hindcast_file_path = f"./docs/{name}/assets/img/pm25_hindcast_1day.png"
    plt = util.plot_air_quality_forecast(
        city, street, hindcast_df, hindcast_file_path, hindcast=True
    )
    plt.show()

    # Upload generated plots to Hopsworks for easy access and sharing
    dataset_api = project.get_dataset_api()
    str_today = today.strftime("%Y-%m-%d")
    if not dataset_api.exists(f"Resources/{name}"):
        dataset_api.mkdir(f"Resources/{name}")
    dataset_api.upload(
        pred_file_path,
        f"Resources/{name}/{city}_{street}_{str_today}",
        overwrite=True,
    )
    dataset_api.upload(
        hindcast_file_path,
        f"Resources/{name}/{city}_{street}_{str_today}",
        overwrite=True,
    )

    proj_url = project.get_url()
    logger.info(
        f"See images in Hopsworks here: {proj_url}/settings/fb/path/Resources/{name}"
    )


inference(retrieved_model, "air_quality", batch_data)
inference(retrieved_lagged_model, "air_quality_lagged", batch_data)
