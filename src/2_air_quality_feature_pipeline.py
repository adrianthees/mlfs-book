import datetime
import json
import logging
import sys

import hopsworks
import pandas as pd

from mlfs import config
from mlfs.airquality import util

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Load configuration from .env file
settings = config.HopsworksSettings(_env_file=".env")  # type: ignore

project = hopsworks.login()
fs = project.get_feature_store()
secrets = hopsworks.get_secrets_api()

if not secrets:
    logger.error("Error when retrieving secrets from hopsworks")
    sys.exit(1)

# Retrieve API key and sensor location from Hopsworks secrets (set in backfill script)
AQICN_API_KEY = secrets.get_secret("AQICN_API_KEY")
if not AQICN_API_KEY:
    logger.error("Error when retrieving AQICN_API_KEY from hopsworks")
    sys.exit(1)
AQICN_API_KEY = AQICN_API_KEY.value
if not AQICN_API_KEY:
    logger.error("Error when extracting value string for AQICN_API_KEY")
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

# Extract location details from the stored JSON
country = location["country"]
city = location["city"]
street = location["street"]
aqicn_url = location["aqicn_url"]
latitude = location["latitude"]
longitude = location["longitude"]

today = datetime.date.today()

# Retrieve feature groups
air_quality_fg = fs.get_feature_group(
    name="air_quality",
    version=1,
)
weather_fg = fs.get_feature_group(
    name="weather",
    version=1,
)


# Fetch today's air quality measurement from the AQICN API
aq_today_df = util.get_pm25(aqicn_url, country, city, street, today, AQICN_API_KEY)

aq_today_df.info()
air_quality_fg.insert(aq_today_df)

# Get hourly weather forecast data
hourly_df = util.get_hourly_weather_forecast(city, latitude, longitude)
hourly_df = hourly_df.set_index("date")

# Convert hourly forecast to daily by extracting only midday (12:00) values
# This gives us one forecast per day instead of 24 hourly forecasts
daily_df = hourly_df.between_time("11:59", "12:01")
daily_df = daily_df.reset_index()
# Normalize date to remove time component
daily_df["date"] = pd.to_datetime(daily_df["date"]).dt.date
daily_df["date"] = pd.to_datetime(daily_df["date"])
daily_df["city"] = city


daily_df.info()
weather_fg.insert(daily_df, wait=True)

logger.info("Updating lagged air quality features...")
air_quality_lagged_fg = fs.get_feature_group(
    name="air_quality_lagged",
    version=1,
)

# Read recent air quality data to calculate lags
air_quality_df_recent = air_quality_fg.read()
air_quality_df_recent = air_quality_df_recent.sort_values(["city", "street", "date"])

# Filter to only the most recent data needed for calculating today's lags
# We need the last 4 days (today + 3 days back for lag3)
air_quality_df_recent = air_quality_df_recent.groupby(["city", "street"]).tail(4)

# Calculate lagged features
air_quality_df_recent["pm25_lag1"] = air_quality_df_recent.groupby(["city", "street"])[
    "pm25"
].shift(1)
air_quality_df_recent["pm25_lag2"] = air_quality_df_recent.groupby(["city", "street"])[
    "pm25"
].shift(2)
air_quality_df_recent["pm25_lag3"] = air_quality_df_recent.groupby(["city", "street"])[
    "pm25"
].shift(3)

# Get only today's lagged features (the most recent row with complete lags)
lagged_today_df = air_quality_df_recent[
    ["country", "city", "street", "date", "pm25_lag1", "pm25_lag2", "pm25_lag3"]
].dropna()

# Filter to only today's date
lagged_today_df = lagged_today_df[
    lagged_today_df["date"] == pd.to_datetime(today).tz_localize("UTC")
]

if len(lagged_today_df) > 0:
    air_quality_lagged_fg.insert(lagged_today_df, wait=True)
    logger.info(f"Inserted {len(lagged_today_df)} rows of lagged features for today")
else:
    logger.info("Not enough historical data to create lagged features for today")
