import pytest
import pandas as pd
from airflow.models.xcom_arg import XComArg
import sys
import os

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(project_root)

from src.transform.transform import transform_api_data, transform_db_data, merge_data

class DummyTI:
    def __init__(self):
        self.store = {}

    def xcom_pull(self, key):
        return self.store.get(key)

    def xcom_push(self, key, value):
        self.store[key] = value

@pytest.fixture
def api_sample():
    return pd.DataFrame([{
        "ve_total": 2, "hour": 14, "day_week": 2, "day_weekname": "Tuesday",
        "month": 3, "monthname": "March", "year": 2024, "day": 25,
        "lgt_condname": "Daylight", "weathername": "Clear", "reljct1name": "Yes",
        "fatals": 0, "persons": 1, "harm_evname": "Motor Vehicle In-Transport",
        "man_collname": "Rear-to-Rear"
    }])

@pytest.fixture
def db_sample():
    return pd.DataFrame([{
        "num_units": 2, "crash_hour": 14, "crash_day_of_week": 2, "crash_month": 3,
        "crash_year": 2024, "crash_day_of_month": 25, "lighting_condition": "DAYLIGHT",
        "weather_condition": "CLEAR", "intersection_related_i": "Y",
        "injuries_fatal": 0, "injuries_total": 1, "first_crash_type": "REAR TO REAR",
        "crash_type": "REAR END"
    }])

def test_transform_api_data(api_sample):
    ti = DummyTI()
    ti.xcom_push("api_data", api_sample.to_json(orient="records"))
    transform_api_data(ti=ti)
    result = pd.read_json(ti.xcom_pull("transformed_api_data"))
    assert "crash_hour" in result.columns
    assert result["weather_condition"].iloc[0] == "clear"

def test_transform_db_data(db_sample):
    ti = DummyTI()
    ti.xcom_push("clean_data", db_sample.to_json(orient="records"))
    transform_db_data(ti=ti)
    result = pd.read_json(ti.xcom_pull("transformed_db_data"))
    assert "day_week_name" in result.columns
    assert result["lighting_condition"].iloc[0] == "daylight"

def test_merge_data(api_sample, db_sample):
    ti = DummyTI()
    api_transformed = api_sample.rename(columns={
        've_total': 'num_units', 'hour': 'crash_hour', 'day_week': 'crash_day_of_week',
        'day_weekname': 'day_week_name', 'month': 'crash_month', 'monthname': 'month_name',
        'year': 'crash_year', 'day': 'crash_day_of_month',
        'lgt_condname': 'lighting_condition', 'weathername': 'weather_condition',
        'reljct1name': 'intersection_related_i', 'fatals': 'injuries_fatal',
        'persons': 'injuries_total', 'harm_evname': 'crash_type',
        'man_collname': 'first_crash_type'
    })
    ti.xcom_push("transformed_api_data", api_transformed.to_json(orient="records"))
    ti.xcom_push("transformed_db_data", db_sample.to_json(orient="records"))

    merge_data(ti=ti)
    result = pd.read_json(ti.xcom_pull("merged_data"))
    assert len(result) == 2
    assert "crash_hour" in result.columns

