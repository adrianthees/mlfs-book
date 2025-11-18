import json
import logging
import os
import sys
from datetime import datetime

import hopsworks
from sklearn.metrics import mean_squared_error, r2_score
from xgboost import XGBRegressor, plot_importance

from mlfs import config
from mlfs.airquality import util

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Load configuration from .env file
settings = config.HopsworksSettings(_env_file=".env")  # type: ignore


# Set Hopsworks API key as environment variable if provided in .env
if settings.HOPSWORKS_API_KEY is not None:
    api_key = settings.HOPSWORKS_API_KEY.get_secret_value()
    os.environ["HOPSWORKS_API_KEY"] = api_key
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


# Retrieve feature groups
air_quality_fg = fs.get_feature_group(
    name="air_quality",
    version=1,
)
weather_fg = fs.get_feature_group(
    name="weather",
    version=1,
)
air_quality_lagged_fg = fs.get_feature_group(
    name="air_quality_lagged",
    version=1,
)

# Join air quality, weather data, and lagged air quality features
selected_features = air_quality_fg.select(["pm25", "date"]).join(
    weather_fg.select_features(), on=["city"]
)
# The lagged features contain pm25 values from 1, 2, and 3 days prior
selected_features_lagged = (
    air_quality_fg.select(["pm25", "date"])
    .join(weather_fg.select_features(), on=["city"])
    .join(
        air_quality_lagged_fg.select(["pm25_lag1", "pm25_lag2", "pm25_lag3"]),
        on=["country", "city", "street"],
    )
)

# Create a feature view that defines which features to use for training
# pm25 is the label (target variable) we want to predict
# Now includes lagged pm25 features (1, 2, 3 days prior) in addition to weather features
feature_view = fs.get_or_create_feature_view(
    name="air_quality_fv",
    description="weather features with air quality as the target",
    version=1,
    labels=["pm25"],
    query=selected_features,
)

feature_view_lagged = fs.get_or_create_feature_view(
    name="air_quality_lagged_fv",
    description="weather features and lagged pm25 with air quality as the target",
    version=1,
    labels=["pm25"],
    query=selected_features_lagged,
)

# Split data into train/test sets using a temporal split (data before 2025-05-01 for training)
start_date_test_data = "2025-05-01"
test_start = datetime.strptime(start_date_test_data, "%Y-%m-%d")


def main(feature_view, name):
    logger.info(f"Running training for {name}")
    X_train, X_test, y_train, y_test = feature_view.train_test_split(
        test_start=test_start
    )

    # Remove date column as it shouldn't be used as a feature for prediction
    X_features = X_train.drop(columns=["date"])
    X_test_features = X_test.drop(columns=["date"])

    # Log feature information
    logger.info(f"Features used for training: {X_features.columns.tolist()}")
    logger.info(f"Number of features: {len(X_features.columns)}")
    logger.info(f"Training set size: {len(X_features)}")
    logger.info(f"Test set size: {len(X_test_features)}")

    # Creating an instance of the XGBoost Regressor
    xgb_regressor = XGBRegressor()

    # Fitting the XGBoost Regressor to the training data
    xgb_regressor.fit(X_features, y_train)

    # Predicting target values on the test set
    y_pred = xgb_regressor.predict(X_test_features)

    # Evaluate model performance using MSE and R² metrics
    mse = mean_squared_error(y_test.iloc[:, 0], y_pred)
    logger.info(f"MSE: {mse}")

    r2 = r2_score(y_test.iloc[:, 0], y_pred)
    logger.info(f"R squared: {r2}")

    # Create dataframe with actual and predicted values for visualization
    df = y_test
    df["predicted_pm25"] = y_pred

    df["date"] = X_test["date"]
    df = df.sort_values(by=["date"])
    logger.info(df.head(5))

    # Create directories for storing model artifacts and visualization images
    model_dir = f"{name}_model"
    if not os.path.exists(model_dir):
        os.mkdir(model_dir)
    images_dir = model_dir + "/images"
    if not os.path.exists(images_dir):
        os.mkdir(images_dir)

    # Generate and save hindcast plot (actual vs predicted values)
    file_path = images_dir + "/pm25_hindcast.png"
    plot = util.plot_air_quality_forecast(city, street, df, file_path, hindcast=True)
    plot.show()

    # Generate and save feature importance plot to understand which features most influence predictions
    plot_importance(xgb_regressor)
    feature_importance_path = images_dir + "/feature_importance.png"
    plot.savefig(feature_importance_path)
    plot.show()

    # Save the trained XGBoost model in JSON format
    xgb_regressor.save_model(model_dir + "/model.json")

    res_dict = {
        "MSE": str(mse),
        "R squared": str(r2),
    }

    mr = project.get_model_registry()

    # Register the trained model in Hopsworks model registry with metrics and feature view
    aq_model = mr.python.create_model(
        name=f"{name}_xgboost_model",
        metrics=res_dict,
        feature_view=feature_view,
        description=f"{name.replace('_', ' ').title()} (PM2.5) predictor",
    )

    # Upload model artifacts (model file, images) to the model registry for versioning and deployment
    aq_model.save(model_dir)


main(feature_view, "air_quality")
main(feature_view_lagged, "air_quality_lagged")
