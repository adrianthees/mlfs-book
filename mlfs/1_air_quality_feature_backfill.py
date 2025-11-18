import datetime
import logging
import sys

import hopsworks
import pandas as pd
from great_expectations import core as ge_core

from mlfs import config
from mlfs.airquality import util

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Load configuration from .env file
settings = config.HopsworksSettings(_env_file=".env")  # type: ignore


project = hopsworks.login()

# API key from .env file for accessing air quality data from aqicn.org
if settings.AQICN_API_KEY is None:
    logger.error("You need to set AQICN_API_KEY either in this cell or in ~/.env")
    sys.exit(1)

AQICN_API_KEY = settings.AQICN_API_KEY.get_secret_value()
logger.info(f"Found AQICN_API_KEY: {AQICN_API_KEY}")


secrets = hopsworks.get_secrets_api()

if not secrets:
    logger.error("Error when retrieving secrets from hopsworks")
    sys.exit(1)
# Store API key in Hopsworks secrets for use in feature pipelines
# Replace any existing secret with the new value
secret = secrets.get_secret("AQICN_API_KEY")
if secret is not None:
    secret.delete()
    logger.info("Replacing existing AQICN_API_KEY")

secrets.create_secret("AQICN_API_KEY", AQICN_API_KEY)


today = datetime.date.today()

all_aq_data = []
all_lagged_data = []

for sensor in config.SENSORS:
    # Load historical CSV for this sensor (if available)
    csv_file = f"./data/{sensor['city']}-{sensor['street']}.csv"
    util.check_file_path(csv_file)
    df = pd.read_csv(csv_file, parse_dates=["date"], skipinitialspace=True)
    if "median" in df.columns:
        df = df.rename(columns={"median": "pm25"})
    df_aq = df[["date", "pm25"]].dropna()
    df_aq["pm25"] = df_aq["pm25"].astype("float32")

    # Add location metadata
    df_aq["country"] = sensor["country"]
    df_aq["city"] = sensor["city"]
    df_aq["street"] = sensor["street"]
    df_aq["url"] = sensor["aqicn_url"]

    all_aq_data.append(df_aq)

# Combine all sensors
combined_aq_df = pd.concat(all_aq_data, ignore_index=True)

combined_aq_df.info()

# Fetch historical weather data matching the air quality data time range
earliest_aq_date = pd.Series.min(df_aq["date"])  # type: ignore
earliest_aq_date = earliest_aq_date.strftime("%Y-%m-%d")

weather_df = util.get_historical_weather(
    config.CITY,
    earliest_aq_date,
    str(today),
    config.CITY_LATITUDE,
    config.CITY_LONGITUDE,
)
weather_df.info()

# Define data validation rules for air quality data using Great Expectations
aq_expectation_suite = ge_core.ExpectationSuite(
    expectation_suite_name="aq_expectation_suite"
)

# PM2.5 values should be between 0 and 500 (valid range for air quality index)
aq_expectation_suite.add_expectation(
    ge_core.ExpectationConfiguration(
        expectation_type="expect_column_min_to_be_between",
        kwargs={
            "column": "pm25",
            "min_value": -0.1,
            "max_value": 500.0,
            "strict_min": True,
        },
    )
)

# Define data validation rules for weather data
weather_expectation_suite = ge_core.ExpectationSuite(
    expectation_suite_name="weather_expectation_suite"
)


# Helper function to add validation that weather metrics are non-negative
def expect_greater_than_zero(col):
    weather_expectation_suite.add_expectation(
        ge_core.ExpectationConfiguration(
            expectation_type="expect_column_min_to_be_between",
            kwargs={
                "column": col,
                "min_value": -0.1,
                "max_value": 1000.0,
                "strict_min": True,
            },
        )
    )


expect_greater_than_zero("precipitation_sum")
expect_greater_than_zero("wind_speed_10m_max")


fs = project.get_feature_store()


# Create feature group in Hopsworks for air quality data with validation rules
air_quality_fg = fs.get_or_create_feature_group(
    name="air_quality",
    description="Air Quality characteristics of each day",
    version=1,
    primary_key=["country", "city", "street"],
    event_time="date",
    expectation_suite=aq_expectation_suite,
)

# Insert historical air quality data into feature store
air_quality_fg.insert(combined_aq_df)

# Add feature descriptions for better documentation in the feature store
air_quality_fg.update_feature_description("date", "Date of measurement of air quality")
air_quality_fg.update_feature_description(
    "country",
    "Country where the air quality was measured (sometimes a city in acqcn.org)",
)
air_quality_fg.update_feature_description(
    "city", "City where the air quality was measured"
)
air_quality_fg.update_feature_description(
    "street", "Street in the city where the air quality was measured"
)
air_quality_fg.update_feature_description(
    "pm25",
    "Particles less than 2.5 micrometers in diameter (fine particles) pose health risk",
)

# Create feature group in Hopsworks for weather data with validation rules
weather_fg = fs.get_or_create_feature_group(
    name="weather",
    description="Weather characteristics of each day",
    version=1,
    primary_key=["city"],
    event_time="date",
    expectation_suite=weather_expectation_suite,
)

# Insert historical weather data into feature store
weather_fg.insert(weather_df, wait=True)


# Add feature descriptions for weather data
weather_fg.update_feature_description("date", "Date of measurement of weather")
weather_fg.update_feature_description(
    "city", "City where weather is measured/forecast for"
)
weather_fg.update_feature_description("temperature_2m_mean", "Temperature in Celsius")
weather_fg.update_feature_description(
    "precipitation_sum", "Precipitation (rain/snow) in mm"
)
weather_fg.update_feature_description(
    "wind_speed_10m_max", "Wind speed at 10m abouve ground"
)
weather_fg.update_feature_description(
    "wind_direction_10m_dominant", "Dominant Wind direction over the dayd"
)


# Create lagged air quality features
logger.info("Creating lagged air quality features...")
air_quality_df_with_lags = air_quality_fg.read()
air_quality_df_with_lags = air_quality_df_with_lags.sort_values(
    ["city", "street", "date"]
)

# Group by city and street to calculate lags correctly for each location
air_quality_df_with_lags["pm25_lag1"] = air_quality_df_with_lags.groupby(
    ["city", "street"]
)["pm25"].shift(1)
air_quality_df_with_lags["pm25_lag2"] = air_quality_df_with_lags.groupby(
    ["city", "street"]
)["pm25"].shift(2)
air_quality_df_with_lags["pm25_lag3"] = air_quality_df_with_lags.groupby(
    ["city", "street"]
)["pm25"].shift(3)

# Select only the lagged features and metadata
lagged_features_df = air_quality_df_with_lags[
    ["country", "city", "street", "date", "pm25_lag1", "pm25_lag2", "pm25_lag3"]
].dropna()

logger.info(f"Created {len(lagged_features_df)} rows of lagged features")

# Create feature group in Hopsworks for lagged air quality data
air_quality_lagged_fg = fs.get_or_create_feature_group(
    name="air_quality_lagged",
    description="Lagged air quality features (1, 2, 3 days prior)",
    version=1,
    primary_key=["country", "city", "street"],
    event_time="date",
)

# Insert lagged features into feature store
air_quality_lagged_fg.insert(lagged_features_df, wait=True)

# Add feature descriptions for lagged data
air_quality_lagged_fg.update_feature_description("date", "Date of measurement")
air_quality_lagged_fg.update_feature_description(
    "country", "Country where the air quality was measured"
)
air_quality_lagged_fg.update_feature_description(
    "city", "City where the air quality was measured"
)
air_quality_lagged_fg.update_feature_description(
    "street", "Street in the city where the air quality was measured"
)
air_quality_lagged_fg.update_feature_description(
    "pm25_lag1", "PM2.5 value from 1 day ago"
)
air_quality_lagged_fg.update_feature_description(
    "pm25_lag2", "PM2.5 value from 2 days ago"
)
air_quality_lagged_fg.update_feature_description(
    "pm25_lag3", "PM2.5 value from 3 days ago"
)

logger.info("Lagged features successfully created and stored!")
