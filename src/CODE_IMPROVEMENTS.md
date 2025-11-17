# Code Improvements Analysis

## Overview
This document outlines recommended improvements for the air quality prediction pipeline scripts. The code is functional but was converted from Jupyter notebooks, leaving several opportunities for enhancement.

---

## 1. Code Organization & Structure

### 1.1 Eliminate Code Duplication
**Issue**: All four scripts contain identical path normalization logic (lines 1-20).

**Recommendation**: Extract to a shared utility function.

```python
# mlfs/common/path_utils.py
def get_project_root() -> Path:
    """Get project root directory, normalizing for different execution contexts."""
    root_dir = Path().absolute()
    if root_dir.parts[-1:] in (('airquality',), ('notebooks',)):
        root_dir = Path(*root_dir.parts[:-1])
    return root_dir

def setup_python_path() -> str:
    """Setup Python path and return root directory as string."""
    root_dir = str(get_project_root())
    if root_dir not in sys.path:
        sys.path.append(root_dir)
        logger.info(f"Added to PYTHONPATH: {root_dir}")
    return root_dir
```

### 1.2 Extract Configuration Management
**Issue**: Configuration loading is scattered across files.

**Recommendation**: Create a configuration manager singleton.

```python
# mlfs/common/config_manager.py
class ConfigManager:
    _instance = None
    
    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance
    
    def __init__(self):
        self.root_dir = get_project_root()
        self.settings = config.HopsworksSettings(_env_file=f"{self.root_dir}/.env")
```

---

## 2. Error Handling

### 2.1 Replace Silent Exits
**Issue**: Many places use `exit()` or `sys.exit(1)` without proper error messages.

**Current problematic code**:
```python
if not secrets:
    exit()
```

**Improved version**:
```python
if not secrets:
    raise RuntimeError("Failed to retrieve Hopsworks secrets API")

if not location_str:
    raise ValueError("SENSOR_LOCATION_JSON secret not found in Hopsworks")
```

### 2.2 Add Try-Except Blocks for API Calls
**Issue**: API calls can fail silently or with unclear errors.

**Recommendation**:
```python
try:
    aq_today_df = util.get_pm25(aqicn_url, country, city, street, today, AQICN_API_KEY)
except requests.RequestException as e:
    logger.error(f"Failed to fetch PM2.5 data: {e}")
    raise
except ValueError as e:
    logger.error(f"Invalid response from AQICN API: {e}")
    raise
```

### 2.3 Validate Data After Fetching
**Issue**: No validation that dataframes contain expected data.

**Recommendation**:
```python
def validate_weather_data(df: pd.DataFrame) -> None:
    """Validate weather dataframe has required columns and data."""
    required_cols = ['date', 'temperature_2m_mean', 'precipitation_sum', 
                     'wind_speed_10m_max', 'wind_direction_10m_dominant']
    missing = set(required_cols) - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    
    if df.empty:
        raise ValueError("Weather dataframe is empty")
    
    if df.isnull().any().any():
        logger.warning(f"Weather data contains {df.isnull().sum().sum()} null values")
```

---

## 3. Code Quality

### 3.1 Add Type Hints
**Issue**: No type hints make code harder to understand and maintain.

**Recommendation**:
```python
from typing import Tuple, Optional
import pandas as pd

def get_location_from_secrets(secrets) -> Tuple[str, str, str, str, str, str]:
    """Retrieve location configuration from Hopsworks secrets.
    
    Returns:
        Tuple of (country, city, street, aqicn_url, latitude, longitude)
    
    Raises:
        ValueError: If required secrets are not found
    """
    location_str = secrets.get_secret("SENSOR_LOCATION_JSON").value
    if not location_str:
        raise ValueError("SENSOR_LOCATION_JSON not found")
    
    location = json.loads(location_str)
    return (
        location['country'],
        location['city'],
        location['street'],
        location['aqicn_url'],
        location['latitude'],
        location['longitude']
    )
```

### 3.2 Remove Dead Code
**Issue**: Commented-out code throughout files.

**Lines to remove**:
- File 1, lines 43, 64-66 (commented coordinates)
- File 4, line 61 (commented joblib.load)

### 3.3 Replace Magic Numbers/Strings
**Issue**: Hardcoded values scattered throughout code.

**Recommendation**:
```python
# mlfs/airquality/constants.py
PM25_MIN_VALUE = 0.0
PM25_MAX_VALUE = 500.0
WEATHER_MIN_VALUE = 0.0
WEATHER_MAX_VALUE = 1000.0

FEATURE_GROUP_VERSION = 1
MODEL_VERSION = 1

WEATHER_FEATURES = [
    'temperature_2m_mean',
    'precipitation_sum', 
    'wind_speed_10m_max',
    'wind_direction_10m_dominant'
]
```

### 3.4 Fix Inconsistent Naming
**Issue**: Mixed naming conventions (df, df_aq, weather_df, daily_df).

**Recommendation**: Use consistent, descriptive names:
- `air_quality_df` instead of `df_aq`
- `historical_weather_df` instead of `weather_df`
- `daily_forecast_df` instead of `daily_df`

---

## 4. Logging Instead of Print Statements

**Issue**: Using `print()` for all output makes it hard to control log levels.

**Recommendation**:
```python
import logging

# At the top of each script
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Replace prints with:
logger.info(f"Root dir: {root_dir}")
logger.debug(f"Found AQICN_API_KEY: {AQICN_API_KEY[:10]}...")
logger.error("AQICN_API_KEY not set in environment")
```

---

## 5. Hardcoded Values

### 5.1 File 1: Coordinates Issue
**Issue**: Lines 64-68 have hardcoded coordinates that override each other.

```python
latitude = "63.80818627371923"  # This is overwritten!
longitude = "20.340626811846885"
latitude = "55.67518549863348"  # Copenhagen
longitude = "12.569506585991263"
```

**Recommendation**: Use configuration or command-line arguments:
```python
# In settings
LATITUDE = os.getenv("AQICN_LATITUDE")
LONGITUDE = os.getenv("AQICN_LONGITUDE")

if not (LATITUDE and LONGITUDE):
    try:
        latitude, longitude = util.get_city_coordinates(city)
    except Exception as e:
        logger.error(f"Failed to get coordinates: {e}")
        raise ValueError("Please set AQICN_LATITUDE and AQICN_LONGITUDE in .env")
```

### 5.2 CSV File Path
**Issue**: Hardcoded CSV file selection.

**Recommendation**:
```python
csv_file = settings.AIR_QUALITY_DATA_FILE or f"{root_dir}/data/copenhagen.csv"
```

---

## 6. Script Architecture

### 6.1 Separate Concerns
**Issue**: Script 1 does too many things (validation, API calls, feature group creation).

**Recommendation**: Split into smaller, focused functions:

```python
def setup_secrets(project, api_key: str, location_dict: dict) -> None:
    """Store API key and location in Hopsworks secrets."""
    # Implementation

def create_expectation_suites() -> Tuple[ExpectationSuite, ExpectationSuite]:
    """Create validation suites for air quality and weather data."""
    # Implementation

def create_feature_groups(fs, aq_suite, weather_suite) -> Tuple[FeatureGroup, FeatureGroup]:
    """Create and configure feature groups in Hopsworks."""
    # Implementation

def main():
    # Orchestrate the above functions
    pass

if __name__ == "__main__":
    main()
```

### 6.2 Add Command-Line Interface
**Issue**: No way to pass parameters without editing code.

**Recommendation**:
```python
import argparse

def parse_args():
    parser = argparse.ArgumentParser(description='Backfill air quality features')
    parser.add_argument('--csv-file', help='Path to historical data CSV')
    parser.add_argument('--city', help='Override city from .env')
    parser.add_argument('--start-date', help='Start date for backfill')
    parser.add_argument('--dry-run', action='store_true', help='Validate without inserting')
    return parser.parse_args()
```

---

## 7. Pandas Best Practices

### 7.1 Avoid inplace=True
**Issue**: File 1, line 103 uses `df_aq.dropna(inplace=True)`.

**Recommendation**:
```python
# More explicit and easier to debug
df_aq = df_aq.dropna()
```

### 7.2 Chain Operations
**Issue**: Multiple separate operations on same dataframe.

**Current**:
```python
df_aq = df[['date', 'pm25']]
df_aq['pm25'] = df_aq['pm25'].astype('float32')
df_aq.dropna(inplace=True)
df_aq['country'] = country
```

**Improved**:
```python
df_aq = (
    df[['date', 'pm25']]
    .assign(pm25=lambda x: x['pm25'].astype('float32'))
    .dropna()
    .assign(
        country=country,
        city=city,
        street=street,
        url=aqicn_url
    )
)
```

---

## 8. Data Validation

### 8.1 Add Validation Helper
**Recommendation**:
```python
def validate_dataframe_schema(
    df: pd.DataFrame, 
    required_columns: List[str],
    name: str
) -> None:
    """Validate dataframe has required columns and is non-empty."""
    missing = set(required_columns) - set(df.columns)
    if missing:
        raise ValueError(f"{name} missing columns: {missing}")
    
    if df.empty:
        raise ValueError(f"{name} is empty")
    
    logger.info(f"{name} validation passed: {len(df)} rows, {len(df.columns)} columns")
```

---

## 9. Testing

### 9.1 Add Unit Tests
**Recommendation**: Create test files for each script.

```python
# tests/test_feature_backfill.py
import pytest
from unittest.mock import Mock, patch

def test_setup_secrets():
    """Test secret storage in Hopsworks."""
    # Implementation

def test_create_expectation_suites():
    """Test validation suite creation."""
    suites = create_expectation_suites()
    assert suites[0].expectation_suite_name == "aq_expectation_suite"

def test_validate_dataframe_schema():
    """Test dataframe validation."""
    df = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})
    validate_dataframe_schema(df, ['a', 'b'], 'test_df')  # Should pass
    
    with pytest.raises(ValueError):
        validate_dataframe_schema(df, ['a', 'c'], 'test_df')  # Should fail
```

---

## 10. Documentation

### 10.1 Add Docstrings
**Issue**: No function or module docstrings.

**Recommendation**:
```python
"""
Air Quality Feature Backfill Pipeline

This script performs initial backfill of historical air quality and weather data
into Hopsworks feature store. It should be run once during initial setup.

Usage:
    python 1_air_quality_feature_backfill.py --csv-file data/copenhagen.csv

Environment Variables:
    AQICN_API_KEY: API key for aqicn.org
    AQICN_COUNTRY: Country name
    AQICN_CITY: City name
    AQICN_STREET: Street/station name
"""
```

### 10.2 Add README
Create `src/README.md` explaining the pipeline:

```markdown
# Air Quality Prediction Pipeline

## Scripts

1. `1_air_quality_feature_backfill.py` - Initial data loading
2. `2_air_quality_feature_pipeline.py` - Daily feature updates
3. `3_air_quality_training_pipeline.py` - Model training
4. `4_air_quality_batch_inference.py` - Generate predictions

## Setup

1. Copy `.env.example` to `.env`
2. Fill in required API keys
3. Run backfill script
4. Schedule feature and inference pipelines
```

---

## 11. Performance Considerations

### 11.1 Use Explicit Wait Strategically
**Issue**: File 1 uses `wait=True` on weather_fg insert but not air_quality_fg.

**Recommendation**: Be consistent and add timeout:
```python
air_quality_fg.insert(df_aq, wait=True)  # Add wait
weather_fg.insert(weather_df, wait=True)
```

### 11.2 Batch Operations
**Issue**: File 4 creates plots even if no new data.

**Recommendation**: Add conditional logic:
```python
if len(batch_data) > 0:
    plt = util.plot_air_quality_forecast(city, street, batch_data, pred_file_path)
    dataset_api.upload(pred_file_path, ...)
else:
    logger.info("No new predictions to visualize")
```

---

## 12. Security

### 12.1 Don't Log Secrets
**Issue**: File 1, line 71 prints API key.

**Current**:
```python
print(f"Found AQICN_API_KEY: {AQICN_API_KEY}")
```

**Improved**:
```python
logger.info(f"Found AQICN_API_KEY: {AQICN_API_KEY[:8]}...") # Show only first 8 chars
```

### 12.2 Validate Secret Values
```python
def validate_api_key(key: str) -> None:
    """Validate API key format."""
    if not key or len(key) < 10:
        raise ValueError("Invalid API key format")
```

---

## 13. File Organization

### Recommended Structure
```
src/
├── README.md
├── pipelines/
│   ├── __init__.py
│   ├── backfill.py           # Script 1 refactored
│   ├── feature_pipeline.py   # Script 2 refactored
│   ├── training_pipeline.py  # Script 3 refactored
│   └── inference_pipeline.py # Script 4 refactored
├── common/
│   ├── __init__.py
│   ├── config.py
│   ├── logging_setup.py
│   └── validation.py
└── tests/
    ├── __init__.py
    ├── test_backfill.py
    ├── test_feature_pipeline.py
    └── fixtures/
        └── sample_data.csv
```

---

## Priority Recommendations

### High Priority (Do First)
1. ✅ Add proper error handling and logging
2. ✅ Extract duplicate path setup code
3. ✅ Fix hardcoded coordinates issue in file 1
4. ✅ Remove dead/commented code
5. ✅ Add type hints to function signatures

### Medium Priority
6. Add command-line argument parsing
7. Create unit tests
8. Add docstrings to all functions
9. Extract magic numbers to constants
10. Validate dataframes after API calls

### Low Priority (Nice to Have)
11. Refactor into class-based architecture
12. Add comprehensive logging
13. Create shared configuration manager
14. Improve pandas operations with chaining
15. Add performance monitoring

---

## Summary

The code is functional but would benefit significantly from:
- **Better error handling** (no silent failures)
- **Code reuse** (eliminate duplication)
- **Proper logging** (replace print statements)
- **Type safety** (add type hints)
- **Testing** (add unit tests)
- **Documentation** (add docstrings and README)
- **Configuration management** (centralize settings)

These improvements will make the code more maintainable, debuggable, and production-ready.