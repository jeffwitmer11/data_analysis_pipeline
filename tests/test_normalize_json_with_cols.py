import json
import pytest
import pandas as pd
from process import process

@pytest.fixture(scope="session")
def json_data_and_cols():
    required_cols = ["first_name", "middle_name", "last_name", "zip_code"]
    json_data = [
            {"user1":
                {"first_name": "Darlene",
                "middle_name": "Brenda",
                "last_name": "Puckett",
                "zip_code": 24065}
            },

            {"user2":
                {"age": 99,
                "email": "email@email.com",
                "info":
                    {"first_name": "Bob",
                    "middle_name": "The Maestro",
                    "last_name": "Cobb",
                    "zip_code": 56010}
                    }
            }]
    return json_data, required_cols

def test_normalize_json_with_cols_empty_dict():
    json_decoded = {}
    df = process.normalize_json_with_cols(json_decoded)
    assert df.empty

def test_normalize_json_with_cols_returns_DataFrame(json_data_and_cols):
    data, cols = json_data_and_cols
    df = process.normalize_json_with_cols(data, cols)
    assert isinstance(df, pd.DataFrame)

def test_normalize_json_with_cols_nested(json_data_and_cols):
    data, cols = json_data_and_cols
    df = process.normalize_json_with_cols(data, cols)
    assert all([item in df.columns for item in cols])

def test_normalize_json_with_cols_coalesce(json_data_and_cols):
    data, cols = json_data_and_cols
    df = process.normalize_json_with_cols(data, cols)
    assert not df[cols].isnull().any(axis=None)

