from pathlib import Path

from dagster import AssetKey

AIRFLOW_BASE_URL = "http://localhost:8080"
AIRFLOW_INSTANCE_NAME = "my_airflow_instance"

# Authentication credentials (lol)
USERNAME = "admin"
PASSWORD = "admin"

MIGRATION_STATE_PATH = Path(__file__).parent / "migration"

A1 = AssetKey(["a1"])
A2 = AssetKey(["a2"])
A3 = AssetKey(["a3"])
A4 = AssetKey(["a4"])
