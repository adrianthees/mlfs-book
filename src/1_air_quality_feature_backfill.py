import datetime
import json
import sys
import warnings
from pathlib import Path

import hopsworks
import pandas as pd
from great_expectations import core as ge_core

from mlfs import config
from mlfs.airquality import util

root_dir = Path().absolute()
if root_dir.parts[-1:] == ('airquality',):
    root_dir = Path(*root_dir.parts[:-1])
if root_dir.parts[-1:] == ('notebooks',):
    root_dir = Path(*root_dir.parts[:-1])
root_dir = str(root_dir)
print("Local environment")

print(f"Root dir: {root_dir}")

if root_dir not in sys.path:
    sys.path.append(root_dir)
    print(f"Added the following directory to the PYTHONPATH: {root_dir}")


settings = config.HopsworksSettings(_env_file=f"{root_dir}/.env")  # type: ignore


warnings.filterwarnings("ignore")

project = hopsworks.login()

today = datetime.date.today()

# csv_file=f"{root_dir}/data/tomtebo-vittervägen.csv"
csv_file = f"{root_dir}/data/copenhagen.csv"
util.check_file_path(csv_file)

# taken from ~/.env. You can also replace settings.AQICN_API_KEY with the api key value as a string "...."
if settings.AQICN_API_KEY is None:
    print("You need to set AQICN_API_KEY either in this cell or in ~/.env")
    sys.exit(1)

AQICN_API_KEY = settings.AQICN_API_KEY.get_secret_value()
aqicn_url = settings.AQICN_URL
country = settings.AQICN_COUNTRY
city = settings.AQICN_CITY
street = settings.AQICN_STREET

if not (aqicn_url and country and city and street):
    exit()

# If this API call fails (it fails in a github action), then set longitude and latitude explicitly - comment out next line
# latitude, longitude = util.get_city_coordinates(city)
# Uncomment this if API call to get longitude and latitude
latitude = "63.80818627371923"
longitude = "20.340626811846885"
latitude = "55.67518549863348"
longitude = "12.569506585991263"

print(f"Found AQICN_API_KEY: {AQICN_API_KEY}")

secrets = hopsworks.get_secrets_api()

if not secrets:
    exit()
# Replace any existing secret with the new value
secret = secrets.get_secret("AQICN_API_KEY")
if secret is not None:
    secret.delete()
    print("Replacing existing AQICN_API_KEY")

secrets.create_secret("AQICN_API_KEY", AQICN_API_KEY)

try:
    aq_today_df = util.get_pm25(aqicn_url, country, city, street, today, AQICN_API_KEY)
    aq_today_df.head()
except hopsworks.RestAPIError:
    print(
        "It looks like the AQICN_API_KEY doesn't work for your sensor. Is the API key correct? Is the sensor URL correct?"
    )

df = pd.read_csv(csv_file, parse_dates=['date'], skipinitialspace=True)


df_aq = df[['date', 'pm25']]
df_aq['pm25'] = df_aq['pm25'].astype('float32')

# Cast the pm25 column to be a float32 data type
df_aq.info()
df_aq.dropna(inplace=True)
df_aq['country'] = country
df_aq['city'] = city
df_aq['street'] = street
df_aq['url'] = aqicn_url

df_aq.info()

earliest_aq_date = pd.Series.min(df_aq['date'])  # type: ignore
earliest_aq_date = earliest_aq_date.strftime('%Y-%m-%d')

weather_df = util.get_historical_weather(city, earliest_aq_date, str(today), latitude, longitude)
weather_df.info()

aq_expectation_suite = ge_core.ExpectationSuite(expectation_suite_name="aq_expectation_suite")

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

weather_expectation_suite = ge_core.ExpectationSuite(expectation_suite_name="weather_expectation_suite")


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

dict_obj = {
    "country": country,
    "city": city,
    "street": street,
    "aqicn_url": aqicn_url,
    "latitude": latitude,
    "longitude": longitude,
}

# Convert the dictionary to a JSON string
str_dict = json.dumps(dict_obj)

# Replace any existing secret with the new value
secret = secrets.get_secret("SENSOR_LOCATION_JSON")
if secret is not None:
    secret.delete()
    print("Replacing existing SENSOR_LOCATION_JSON")

secrets.create_secret("SENSOR_LOCATION_JSON", str_dict)


air_quality_fg = fs.get_or_create_feature_group(
    name='air_quality',
    description='Air Quality characteristics of each day',
    version=1,
    primary_key=['country', 'city', 'street'],
    event_time="date",
    expectation_suite=aq_expectation_suite,
)

air_quality_fg.insert(df_aq)

air_quality_fg.update_feature_description("date", "Date of measurement of air quality")
air_quality_fg.update_feature_description(
    "country",
    "Country where the air quality was measured (sometimes a city in acqcn.org)",
)
air_quality_fg.update_feature_description("city", "City where the air quality was measured")
air_quality_fg.update_feature_description("street", "Street in the city where the air quality was measured")
air_quality_fg.update_feature_description(
    "pm25",
    "Particles less than 2.5 micrometers in diameter (fine particles) pose health risk",
)

# Get or create feature group
weather_fg = fs.get_or_create_feature_group(
    name='weather',
    description='Weather characteristics of each day',
    version=1,
    primary_key=['city'],
    event_time="date",
    expectation_suite=weather_expectation_suite,
)


weather_fg.insert(weather_df, wait=True)


weather_fg.update_feature_description("date", "Date of measurement of weather")
weather_fg.update_feature_description("city", "City where weather is measured/forecast for")
weather_fg.update_feature_description("temperature_2m_mean", "Temperature in Celsius")
weather_fg.update_feature_description("precipitation_sum", "Precipitation (rain/snow) in mm")
weather_fg.update_feature_description("wind_speed_10m_max", "Wind speed at 10m abouve ground")
weather_fg.update_feature_description("wind_direction_10m_dominant", "Dominant Wind direction over the dayd")
