import datetime
import logging
import os

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


def inference(model, name, sensor, batch_data):
    """Run inference for a single sensor"""
    logger.info(f"Running inference for {name} - {sensor['city']}/{sensor['street']}")

    # Download the saved model artifacts to a local directory
    saved_model_dir = model.download()

    # Load the XGBoost model from the JSON file
    retrieved_xgboost_model = XGBRegressor()
    retrieved_xgboost_model.load_model(saved_model_dir + "/model.json")

    # Create a copy of batch_data for this sensor
    sensor_batch_data = batch_data.copy()

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

        # Get the most recent lagged values for this specific sensor
        latest_lags = lagged_data[
            (lagged_data["city"] == sensor["city"])
            & (lagged_data["street"] == sensor["street"])
        ].head(1)

        if len(latest_lags) > 0:
            pm25_lag1 = latest_lags["pm25_lag1"].values[0]
            pm25_lag2 = latest_lags["pm25_lag2"].values[0]
            pm25_lag3 = latest_lags["pm25_lag3"].values[0]

            # Add lagged features to batch data
            sensor_batch_data["pm25_lag1"] = pm25_lag1
            sensor_batch_data["pm25_lag2"] = pm25_lag2
            sensor_batch_data["pm25_lag3"] = pm25_lag3

            logger.info(
                f"Using lagged features for {sensor['street']} - lag1: {pm25_lag1:.2f}, lag2: {pm25_lag2:.2f}, lag3: {pm25_lag3:.2f}"
            )
        else:
            logger.warning(
                f"No lagged features found for {sensor['street']}. Using zeros for lagged features."
            )
            sensor_batch_data["pm25_lag1"] = 0
            sensor_batch_data["pm25_lag2"] = 0
            sensor_batch_data["pm25_lag3"] = 0

        predict_columns.extend(["pm25_lag1", "pm25_lag2", "pm25_lag3"])

    # Use the model to predict PM2.5 values based on weather features (and lagged pm25)
    sensor_batch_data["predicted_pm25"] = retrieved_xgboost_model.predict(
        sensor_batch_data[predict_columns]
    )

    # Add location metadata to the predictions
    sensor_batch_data["street"] = sensor["street"]
    sensor_batch_data["city"] = sensor["city"]
    sensor_batch_data["country"] = sensor["country"]
    # Track how many days in advance this forecast was made (1 = tomorrow, 2 = day after, etc.)
    sensor_batch_data["days_before_forecast_day"] = range(1, len(sensor_batch_data) + 1)
    sensor_batch_data = sensor_batch_data.sort_values(by=["date"])

    # Generate and save forecast plot
    pred_file_path = f"./docs/{name}/assets/img/{sensor['street']}_pm25_forecast.png"

    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(pred_file_path), exist_ok=True)

    plt = util.plot_air_quality_forecast(
        sensor["city"], sensor["street"], sensor_batch_data, pred_file_path
    )
    plt.close()

    # Store predictions in feature store for monitoring and analysis
    monitor_fg = fs.get_or_create_feature_group(
        name=f"{name}_predictions",
        description=f"{name.replace('_', ' ').title()} prediction monitoring",
        version=1,
        primary_key=["city", "street", "date", "days_before_forecast_day"],
        event_time="date",
    )

    monitor_fg.insert(sensor_batch_data, wait=True)

    # Get predictions made 1 day in advance for model performance monitoring
    monitoring_df = monitor_fg.filter(
        (monitor_fg.days_before_forecast_day == 1)
        & (monitor_fg.street == sensor["street"])
    ).read()

    # Retrieve actual air quality measurements to compare against predictions
    air_quality_fg = fs.get_feature_group(name="air_quality", version=1)

    air_quality_df = air_quality_fg.read()
    # Filter for this specific sensor
    air_quality_df = air_quality_df[
        (air_quality_df["city"] == sensor["city"])
        & (air_quality_df["street"] == sensor["street"])
    ]

    # Merge predictions with actual measurements for model evaluation
    outcome_df = air_quality_df[["date", "pm25"]]
    preds_df = monitoring_df[["date", "predicted_pm25"]]

    hindcast_df = pd.merge(preds_df, outcome_df, on="date")
    hindcast_df = hindcast_df.sort_values(by=["date"])

    # If no matching data exists, backfill historical predictions for monitoring
    if len(hindcast_df) == 0:
        logger.info(
            f"No hindcast data found for {sensor['street']}, attempting backfill..."
        )
        hindcast_df = util.backfill_predictions_for_monitoring(
            name, weather_fg, air_quality_df, monitor_fg, retrieved_xgboost_model
        )

    # Generate and save hindcast plot (actual vs predicted values from 1-day-ahead forecasts)
    hindcast_file_path = (
        f"./docs/{name}/assets/img/{sensor['street']}_pm25_hindcast_1day.png"
    )

    if len(hindcast_df) > 0:
        plt = util.plot_air_quality_forecast(
            sensor["city"],
            sensor["street"],
            hindcast_df,
            hindcast_file_path,
            hindcast=True,
        )
        plt.close()
    else:
        logger.warning(f"No hindcast data available for {sensor['street']}")

    # Upload generated plots to Hopsworks for easy access and sharing
    dataset_api = project.get_dataset_api()
    str_today = today.strftime("%Y-%m-%d")

    resource_path = f"Resources/{name}/{sensor['city']}_{sensor['street']}_{str_today}"

    if not dataset_api.exists(f"Resources/{name}"):
        dataset_api.mkdir(f"Resources/{name}")

    dataset_api.upload(
        pred_file_path,
        resource_path,
        overwrite=True,
    )

    if len(hindcast_df) > 0:
        dataset_api.upload(
            hindcast_file_path,
            resource_path,
            overwrite=True,
        )

    proj_url = project.get_url()
    logger.info(
        f"See images for {sensor['street']} in Hopsworks here: {proj_url}/settings/fb/path/Resources/{name}"
    )


if __name__ == "__main__":
    today = datetime.datetime.now() - datetime.timedelta(0)
    tomorrow = today + datetime.timedelta(days=1)

    project = hopsworks.login()
    fs = project.get_feature_store()
    mr = project.get_model_registry()

    # Retrieve the trained models from the model registry
    retrieved_model = mr.get_model(
        name="air_quality_xgboost_model",
        version=2,
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
    # Run inference for all sensors
    logger.info(f"Running inference for {len(config.SENSORS)} sensors")

    for sensor in config.SENSORS:
        logger.info(f"\n{'='*80}")
        logger.info(f"Processing sensor: {sensor['city']}/{sensor['street']}")
        logger.info(f"{'='*80}")

        try:
            # Run both models for each sensor
            inference(retrieved_model, "air_quality", sensor, batch_data.copy())
            inference(
                retrieved_lagged_model, "air_quality_lagged", sensor, batch_data.copy()
            )
        except Exception as e:
            logger.error(f"Error processing sensor {sensor['street']}: {e}")
            import traceback

            traceback.print_exc()

    logger.info(f"\n{'='*80}")
    logger.info("Inference complete for all sensors!")
    logger.info(f"{'='*80}")
