import pandas as pd
from process import process

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

