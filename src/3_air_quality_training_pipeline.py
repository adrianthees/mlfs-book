import json
import os
import sys
import warnings
from datetime import datetime
from pathlib import Path

import hopsworks
from sklearn.metrics import mean_squared_error, r2_score
from xgboost import XGBRegressor, plot_importance

from mlfs import config
from mlfs.airquality import util

warnings.filterwarnings("ignore")

root_dir = Path().absolute()
# Strip ~/notebooks/ccfraud from PYTHON_PATH if notebook started in one of these subdirectories
if root_dir.parts[-1:] == ('airquality',):
    root_dir = Path(*root_dir.parts[:-1])
if root_dir.parts[-1:] == ('notebooks',):
    root_dir = Path(*root_dir.parts[:-1])
root_dir = str(root_dir)
print("Local environment")

# Add the root directory to the `PYTHONPATH` to use the `recsys` Python module from the notebook.
if root_dir not in sys.path:
    sys.path.append(root_dir)
print(f"Added the following directory to the PYTHONPATH: {root_dir}")

# Set the environment variables from the file <root_dir>/.env
settings = config.HopsworksSettings(_env_file=f"{root_dir}/.env")  # type: ignore


# Check if HOPSWORKS_API_KEY env variable is set or if it is set in ~/.env
if settings.HOPSWORKS_API_KEY is not None:
    api_key = settings.HOPSWORKS_API_KEY.get_secret_value()
    os.environ['HOPSWORKS_API_KEY'] = api_key
project = hopsworks.login()
fs = project.get_feature_store()

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


# Retrieve feature groups
air_quality_fg = fs.get_feature_group(
    name='air_quality',
    version=1,
)
weather_fg = fs.get_feature_group(
    name='weather',
    version=1,
)

selected_features = air_quality_fg.select(['pm25', 'date']).join(weather_fg.select_features(), on=['city'])

feature_view = fs.get_or_create_feature_view(
    name='air_quality_fv',
    description="weather features with air quality as the target",
    version=1,
    labels=['pm25'],
    query=selected_features,
)


start_date_test_data = "2025-05-01"
# Convert string to datetime object
test_start = datetime.strptime(start_date_test_data, "%Y-%m-%d")

X_train, X_test, y_train, y_test = feature_view.train_test_split(test_start=test_start)

X_features = X_train.drop(columns=['date'])
X_test_features = X_test.drop(columns=['date'])

# Creating an instance of the XGBoost Regressor
xgb_regressor = XGBRegressor()

# Fitting the XGBoost Regressor to the training data
xgb_regressor.fit(X_features, y_train)

# Predicting target values on the test set
y_pred = xgb_regressor.predict(X_test_features)

# Calculating Mean Squared Error (MSE) using sklearn
mse = mean_squared_error(y_test.iloc[:, 0], y_pred)
print("MSE:", mse)

# Calculating R squared using sklearn
r2 = r2_score(y_test.iloc[:, 0], y_pred)
print("R squared:", r2)

df = y_test
df['predicted_pm25'] = y_pred

df['date'] = X_test['date']
df = df.sort_values(by=['date'])
df.head(5)

# Creating a directory for the model artifacts if it doesn't exist
model_dir = "air_quality_model"
if not os.path.exists(model_dir):
    os.mkdir(model_dir)
images_dir = model_dir + "/images"
if not os.path.exists(images_dir):
    os.mkdir(images_dir)


file_path = images_dir + "/pm25_hindcast.png"
plot = util.plot_air_quality_forecast(city, street, df, file_path, hindcast=True)
plot.show()

# Plotting feature importances using the plot_importance function from XGBoost
plot_importance(xgb_regressor)
feature_importance_path = images_dir + "/feature_importance.png"
plot.savefig(feature_importance_path)
plot.show()


# Saving the XGBoost regressor object as a json file in the model directory
xgb_regressor.save_model(model_dir + "/model.json")


res_dict = {
    "MSE": str(mse),
    "R squared": str(r2),
}


mr = project.get_model_registry()

# Creating a Python model in the model registry named 'air_quality_xgboost_model'

aq_model = mr.python.create_model(
    name="air_quality_xgboost_model",
    metrics=res_dict,
    feature_view=feature_view,
    description="Air Quality (PM2.5) predictor",
)

# Saving the model artifacts to the 'air_quality_model' directory in the model registry
aq_model.save(model_dir)
