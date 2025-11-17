import datetime
import json
import sys
from pathlib import Path

import hopsworks
import pandas as pd

from mlfs import config
from mlfs.airquality import util

# Normalize the root directory path to handle execution from different subdirectories
root_dir = Path().absolute()
if root_dir.parts[-1:] == ('airquality',):
    root_dir = Path(*root_dir.parts[:-1])
if root_dir.parts[-1:] == ('notebooks',):
    root_dir = Path(*root_dir.parts[:-1])
root_dir = str(root_dir)

# Add the root directory to the PYTHONPATH to import the mlfs module
if root_dir not in sys.path:
    sys.path.append(root_dir)
print(f"Added the following directory to the PYTHONPATH: {root_dir}")

# Load configuration from .env file
settings = config.HopsworksSettings(_env_file=f"{root_dir}/.env")  # type: ignore

project = hopsworks.login()
fs = project.get_feature_store()
secrets = hopsworks.get_secrets_api()

if not secrets:
    exit()

# Retrieve API key and sensor location from Hopsworks secrets (set in backfill script)
AQICN_API_KEY = secrets.get_secret("AQICN_API_KEY").value
location_str = secrets.get_secret("SENSOR_LOCATION_JSON").value
if not (AQICN_API_KEY and location_str):
    exit()
location = json.loads(location_str)

# Extract location details from the stored JSON
country = location['country']
city = location['city']
street = location['street']
aqicn_url = location['aqicn_url']
latitude = location['latitude']
longitude = location['longitude']

today = datetime.date.today()

# Retrieve feature groups
air_quality_fg = fs.get_feature_group(
    name='air_quality',
    version=1,
)
weather_fg = fs.get_feature_group(
    name='weather',
    version=1,
)


# Fetch today's air quality measurement from the AQICN API
aq_today_df = util.get_pm25(aqicn_url, country, city, street, today, AQICN_API_KEY)

aq_today_df.info()

# Get hourly weather forecast data
hourly_df = util.get_hourly_weather_forecast(city, latitude, longitude)
hourly_df = hourly_df.set_index('date')

# Convert hourly forecast to daily by extracting only midday (12:00) values
# This gives us one forecast per day instead of 24 hourly forecasts
daily_df = hourly_df.between_time('11:59', '12:01')
daily_df = daily_df.reset_index()
# Normalize date to remove time component
daily_df['date'] = pd.to_datetime(daily_df['date']).dt.date
daily_df['date'] = pd.to_datetime(daily_df['date'])
daily_df['city'] = city


daily_df.info()

air_quality_fg.insert(aq_today_df)


weather_fg.insert(daily_df, wait=True)
