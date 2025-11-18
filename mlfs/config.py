import os
from pathlib import Path

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

COUNTRY = "denmark"
CITY = "copenhagen"
CITY_LATITUDE = "55.67518549863348"
CITY_LONGITUDE = "12.569506585991263"

SENSORS = [
    {
        "country": COUNTRY,
        "city": CITY,
        "street": "andersens",
        "aqicn_url": "https://api.waqi.info/feed/@3317",
        "latitude": CITY_LATITUDE,
        "longitude": CITY_LONGITUDE,
    },
    {
        "country": COUNTRY,
        "city": CITY,
        "street": "lundemosen",
        "aqicn_url": "https://api.waqi.info/feed/A370306",
        "latitude": CITY_LATITUDE,
        "longitude": CITY_LONGITUDE,
    },
]


class HopsworksSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    MLFS_DIR: Path = Path(__file__).parent

    # For hopsworks.login(), set as environment variables if they are not already set as env variables
    HOPSWORKS_API_KEY: SecretStr | None = None
    HOPSWORKS_PROJECT: str | None = None
    HOPSWORKS_HOST: str | None = None

    # Air Quality
    AQICN_API_KEY: SecretStr | None = None
    AQICN_COUNTRY: str | None = None
    AQICN_CITY: str | None = None
    AQICN_STREET: str | None = None
    AQICN_URL: str | None = None

    def model_post_init(self, __context):
        """Runs after the model is initialized."""
        print("HopsworksSettings initialized!")

        # Set environment variables if not already set
        if os.getenv("HOPSWORKS_API_KEY") is None:
            if self.HOPSWORKS_API_KEY is not None:
                os.environ["HOPSWORKS_API_KEY"] = (
                    self.HOPSWORKS_API_KEY.get_secret_value()
                )
        if os.getenv("HOPSWORKS_PROJECT") is None:
            if self.HOPSWORKS_PROJECT is not None:
                os.environ["HOPSWORKS_PROJECT"] = self.HOPSWORKS_PROJECT
        if os.getenv("HOPSWORKS_HOST") is None:
            if self.HOPSWORKS_HOST is not None:
                os.environ["HOPSWORKS_HOST"] = self.HOPSWORKS_HOST

        # --- Check required .env values ---
        missing = []
        # 1. HOPSWORKS_API_KEY
        api_key = self.HOPSWORKS_API_KEY or os.getenv("HOPSWORKS_API_KEY")
        if not api_key:
            missing.append("HOPSWORKS_API_KEY")

        if missing:
            raise ValueError(
                "The following required settings are missing from your environment (.env or system):\n  "
                + "\n  ".join(missing)
            )
