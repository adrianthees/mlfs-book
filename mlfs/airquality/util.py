import datetime
import os
from pathlib import Path

import matplotlib.pyplot as plt
import openmeteo_requests
import pandas as pd
import requests
import requests_cache
from geopy.geocoders import Nominatim
from matplotlib.patches import Patch
from matplotlib.ticker import MultipleLocator
from retry_requests import retry


def get_historical_weather(city, start_date, end_date, latitude, longitude):
    # latitude, longitude = get_city_coordinates(city)

    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession(".cache", expire_after=-1)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    # Make sure all required weather variables are listed here
    # The order of variables in hourly or daily is important to assign them correctly below
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "daily": [
            "temperature_2m_mean",
            "precipitation_sum",
            "wind_speed_10m_max",
            "wind_direction_10m_dominant",
        ],
    }
    responses = openmeteo.weather_api(url, params=params)

    # Process first location. Add a for-loop for multiple locations or weather models
    response = responses[0]
    print(f"Coordinates {response.Latitude()}°N {response.Longitude()}°E")
    print(f"Elevation {response.Elevation()} m asl")
    print(f"Timezone {response.Timezone()} {response.TimezoneAbbreviation()}")
    print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")

    # Process daily data. The order of variables needs to be the same as requested.
    daily = response.Daily()
    daily_temperature_2m_mean = daily.Variables(0).ValuesAsNumpy()
    daily_precipitation_sum = daily.Variables(1).ValuesAsNumpy()
    daily_wind_speed_10m_max = daily.Variables(2).ValuesAsNumpy()
    daily_wind_direction_10m_dominant = daily.Variables(3).ValuesAsNumpy()

    daily_data = {
        "date": pd.date_range(
            start=pd.to_datetime(daily.Time(), unit="s"),
            end=pd.to_datetime(daily.TimeEnd(), unit="s"),
            freq=pd.Timedelta(seconds=daily.Interval()),
            inclusive="left",
        )
    }
    daily_data["temperature_2m_mean"] = daily_temperature_2m_mean
    daily_data["precipitation_sum"] = daily_precipitation_sum
    daily_data["wind_speed_10m_max"] = daily_wind_speed_10m_max
    daily_data["wind_direction_10m_dominant"] = daily_wind_direction_10m_dominant

    daily_dataframe = pd.DataFrame(data=daily_data)
    daily_dataframe = daily_dataframe.dropna()
    daily_dataframe["city"] = city
    return daily_dataframe


def get_hourly_weather_forecast(city, latitude, longitude):
    # latitude, longitude = get_city_coordinates(city)

    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession(".cache", expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    # Make sure all required weather variables are listed here
    # The order of variables in hourly or daily is important to assign them correctly below
    url = "https://api.open-meteo.com/v1/ecmwf"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": [
            "temperature_2m",
            "precipitation",
            "wind_speed_10m",
            "wind_direction_10m",
        ],
    }
    responses = openmeteo.weather_api(url, params=params)

    # Process first location. Add a for-loop for multiple locations or weather models
    response = responses[0]
    print(f"Coordinates {response.Latitude()}°N {response.Longitude()}°E")
    print(f"Elevation {response.Elevation()} m asl")
    print(f"Timezone {response.Timezone()} {response.TimezoneAbbreviation()}")
    print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")

    # Process hourly data. The order of variables needs to be the same as requested.

    hourly = response.Hourly()
    hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
    hourly_precipitation = hourly.Variables(1).ValuesAsNumpy()
    hourly_wind_speed_10m = hourly.Variables(2).ValuesAsNumpy()
    hourly_wind_direction_10m = hourly.Variables(3).ValuesAsNumpy()

    hourly_data = {
        "date": pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s"),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s"),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left",
        )
    }
    hourly_data["temperature_2m_mean"] = hourly_temperature_2m
    hourly_data["precipitation_sum"] = hourly_precipitation
    hourly_data["wind_speed_10m_max"] = hourly_wind_speed_10m
    hourly_data["wind_direction_10m_dominant"] = hourly_wind_direction_10m

    hourly_dataframe = pd.DataFrame(data=hourly_data)
    hourly_dataframe = hourly_dataframe.dropna()
    return hourly_dataframe


def get_city_coordinates(city_name: str):
    """
    Takes city name and returns its latitude and longitude (rounded to 2 digits after dot).
    """
    # Initialize Nominatim API (for getting lat and long of the city)
    geolocator = Nominatim(user_agent="MyApp")
    city = geolocator.geocode(city_name)

    latitude = round(city.latitude, 2)
    longitude = round(city.longitude, 2)

    return latitude, longitude


def trigger_request(url: str):
    response = requests.get(url)
    if response.status_code == 200:
        # Extract the JSON content from the response
        data = response.json()
    else:
        print("Failed to retrieve data. Status Code:", response.status_code)
        raise requests.exceptions.RequestException(response.status_code)

    return data


def get_pm25(
    aqicn_url: str,
    country: str,
    city: str,
    street: str,
    day: datetime.date,
    AQI_API_KEY: str,
):
    """
    Returns DataFrame with air quality (pm25) as dataframe
    """
    # The API endpoint URL
    url = f"{aqicn_url}/?token={AQI_API_KEY}"

    # Make a GET request to fetch the data from the API
    data = trigger_request(url)

    # if we get 'Unknown station' response then retry with city in url
    if data["data"] == "Unknown station":
        url1 = f"https://api.waqi.info/feed/{country}/{street}/?token={AQI_API_KEY}"
        data = trigger_request(url1)

    if data["data"] == "Unknown station":
        url2 = (
            f"https://api.waqi.info/feed/{country}/{city}/{street}/?token={AQI_API_KEY}"
        )
        data = trigger_request(url2)

    # Check if the API response contains the data
    if data["status"] == "ok":
        # Extract the air quality data
        aqi_data = data["data"]
        aq_today_df = pd.DataFrame()
        aq_today_df["pm25"] = [aqi_data["iaqi"].get("pm25", {}).get("v", None)]
        aq_today_df["pm25"] = aq_today_df["pm25"].astype("float32")

        aq_today_df["country"] = country
        aq_today_df["city"] = city
        aq_today_df["street"] = street
        aq_today_df["date"] = day
        aq_today_df["date"] = pd.to_datetime(aq_today_df["date"])
        aq_today_df["url"] = aqicn_url
    else:
        print(
            "Error: There may be an incorrect  URL for your Sensor or it is not contactable right now. The API response does not contain data.  Error message:",
            data["data"],
        )
        raise requests.exceptions.RequestException(data["data"])

    return aq_today_df


def plot_air_quality_forecast(
    city: str, street: str, df: pd.DataFrame, file_path: str, hindcast=False
):
    # Set style for better-looking plots
    plt.style.use("seaborn-v0_8-darkgrid")
    fig, ax = plt.subplots(figsize=(12, 7), dpi=100)
    fig.patch.set_facecolor("white")

    day = pd.to_datetime(df["date"]).dt.date
    # Plot predicted values with improved styling
    ax.plot(
        day,
        df["predicted_pm25"],
        label="Predicted PM2.5",
        color="#2196F3",
        linewidth=3,
        marker="o",
        markersize=8,
        markerfacecolor="#1976D2",
        markeredgecolor="white",
        markeredgewidth=2,
        zorder=3,
    )

    # Set the y-axis to a logarithmic scale
    ax.set_yscale("log")
    ax.set_yticks([0, 10, 25, 50, 100, 250, 500])
    ax.get_yaxis().set_major_formatter(plt.ScalarFormatter())
    ax.set_ylim(bottom=1)

    # Set the labels and title with improved styling
    ax.set_xlabel("Date", fontsize=12, fontweight="bold")
    ax.set_title(
        f"PM2.5 Forecast for {city}, {street}", fontsize=16, fontweight="bold", pad=20
    )
    ax.set_ylabel("PM2.5 (µg/m³)", fontsize=12, fontweight="bold")
    ax.grid(True, alpha=0.3, linestyle="--")

    colors = ["#00e400", "#ffff00", "#ff7e00", "#ff0000", "#8f3f97", "#7e0023"]
    labels = [
        "Good",
        "Moderate",
        "Unhealthy for Some",
        "Unhealthy",
        "Very Unhealthy",
        "Hazardous",
    ]
    ranges = [(0, 49), (50, 99), (100, 149), (150, 199), (200, 299), (300, 500)]
    for color, (start, end) in zip(colors, ranges):
        ax.axhspan(start, end, color=color, alpha=0.25, zorder=1)

    # Add a legend for the different Air Quality Categories
    patches = [
        Patch(color=colors[i], label=f"{labels[i]}: {ranges[i][0]}-{ranges[i][1]}")
        for i in range(len(colors))
    ]
    legend1 = ax.legend(
        handles=patches,
        loc="upper right",
        title="Air Quality Categories",
        fontsize=9,
        framealpha=0.95,
        edgecolor="gray",
    )

    # Aim for ~10 annotated values on x-axis, will work for both forecasts ans hindcasts
    if len(df.index) > 11:
        every_x_tick = len(df.index) / 10
        ax.xaxis.set_major_locator(MultipleLocator(every_x_tick))

    plt.xticks(rotation=45)

    if hindcast:
        ax.plot(
            day,
            df["pm25"],
            label="Actual PM2.5",
            color="#333333",
            linewidth=3,
            marker="^",
            markersize=8,
            markerfacecolor="#666666",
            markeredgecolor="white",
            markeredgewidth=2,
            zorder=3,
        )
        ax.add_artist(legend1)

    # Ensure everything is laid out neatly
    plt.tight_layout()

    # Save the figure, overwriting any existing file with the same name
    path = os.path.join(*file_path.split("/")[:-1])
    if not os.path.isdir(path):
        os.makedirs(path)
    plt.savefig(
        file_path, dpi=150, bbox_inches="tight", facecolor="white", edgecolor="none"
    )
    return plt


def check_file_path(file_path):
    my_file = Path(file_path)
    if my_file.is_file() == False:
        print(f"Error. File not found at the path: {file_path} ")
    else:
        print(f"File successfully found at the path: {file_path}")


def backfill_predictions_for_monitoring(
    name, weather_fg, air_quality_df, monitor_fg, model
):
    features_df = weather_fg.read()
    features_df = features_df.sort_values(by=["date"], ascending=True)
    features_df = features_df.tail(10)

    # Get the lagged features
    fs = weather_fg._feature_store
    feature_cols = [
        "temperature_2m_mean",
        "precipitation_sum",
        "wind_speed_10m_max",
        "wind_direction_10m_dominant",
    ]
    if name == "air_quality_lagged":
        try:
            air_quality_lagged_fg = fs.get_feature_group(name=name, version=1)
            lagged_df = air_quality_lagged_fg.read()
            lagged_df = lagged_df.sort_values(by=["date"], ascending=True)

            # Merge lagged features with weather features
            features_df = pd.merge(
                features_df,
                lagged_df[["date", "city", "pm25_lag1", "pm25_lag2", "pm25_lag3"]],
                on=["date", "city"],
                how="left",
            )

            # Fill NaN values with 0 if lagged features are not available
            features_df["pm25_lag1"] = features_df["pm25_lag1"].fillna(0)
            features_df["pm25_lag2"] = features_df["pm25_lag2"].fillna(0)
            features_df["pm25_lag3"] = features_df["pm25_lag3"].fillna(0)

            feature_cols.extend(["pm25_lag1", "pm25_lag2", "pm25_lag3"])
        except:
            # Fallback to non-lagged features if lagged feature group doesn't exist
            print("Warning: Lagged features not found, using only weather features")

            features_df["pm25_lag1"] = 0
            features_df["pm25_lag2"] = 0
            features_df["pm25_lag3"] = 0

    features_df["predicted_pm25"] = model.predict(features_df[feature_cols])
    df = pd.merge(
        features_df, air_quality_df[["date", "pm25", "street", "country"]], on="date"
    )
    df["days_before_forecast_day"] = 1
    hindcast_df = df
    df = df.drop("pm25", axis=1)
    monitor_fg.insert(df, write_options={"wait_for_job": True})
    return hindcast_df
