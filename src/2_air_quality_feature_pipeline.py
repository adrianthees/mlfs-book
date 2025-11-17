import datetime
import json
import sys
from pathlib import Path

import hopsworks
import pandas as pd

from mlfs import config
from mlfs.airquality import util

root_dir = Path().absolute()
if root_dir.parts[-1:] == ('airquality',):
    root_dir = Path(*root_dir.parts[:-1])
if root_dir.parts[-1:] == ('notebooks',):
    root_dir = Path(*root_dir.parts[:-1])
root_dir = str(root_dir)

# Add the root directory to the `PYTHONPATH` to use the `recsys` Python module from the notebook.
if root_dir not in sys.path:
    sys.path.append(root_dir)
print(f"Added the following directory to the PYTHONPATH: {root_dir}")

settings = config.HopsworksSettings(_env_file=f"{root_dir}/.env")  # type: ignore

project = hopsworks.login()
fs = project.get_feature_store()
secrets = hopsworks.get_secrets_api()

if not secrets:
    exit()

# This line will fail if you have not registered the AQICN_API_KEY as a secret in Hopsworks
AQICN_API_KEY = secrets.get_secret("AQICN_API_KEY").value
location_str = secrets.get_secret("SENSOR_LOCATION_JSON").value
if not (AQICN_API_KEY and location_str):
    exit()
location = json.loads(location_str)

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


aq_today_df = util.get_pm25(aqicn_url, country, city, street, today, AQICN_API_KEY)

aq_today_df.info()


hourly_df = util.get_hourly_weather_forecast(city, latitude, longitude)
hourly_df = hourly_df.set_index('date')

# We will only make 1 daily prediction, so we will replace the hourly forecasts with a single daily forecast
# We only want the daily weather data, so only get weather at 12:00
daily_df = hourly_df.between_time('11:59', '12:01')
daily_df = daily_df.reset_index()
daily_df['date'] = pd.to_datetime(daily_df['date']).dt.date
daily_df['date'] = pd.to_datetime(daily_df['date'])
daily_df['city'] = city


daily_df.info()

air_quality_fg.insert(aq_today_df)


weather_fg.insert(daily_df, wait=True)
