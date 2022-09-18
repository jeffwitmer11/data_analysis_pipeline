import pytest
import pandas as pd
from process import process

def test_DataFile_load_json_fails_with_text_file(temp_dir_with_json_files):
    file_path = temp_dir_with_json_files / 'json_single_encoded.txt'
    data = process.DataFile(file_path)

    with pytest.raises(Exception):
        data.load_json()

def test_DataFile_load_json_returns_pandas_df(temp_dir_with_json_files):
    file_path = temp_dir_with_json_files / 'json_double_encoded.json'
    data = process.DataFile(file_path)
    df = data.load_json()

    assert isinstance(df, pd.DataFrame)

def test_DataFile_load_json_empty_file(temp_dir_with_json_files):
    file_path = temp_dir_with_json_files / 'empty_json_file.json'
    data = process.DataFile(file_path)
    df = data.load_json()

    assert df.empty | df.isnull().all().all()

def test_DataFile_load_json_single_encoded(temp_dir_with_json_files):
    file_path = temp_dir_with_json_files / 'json_single_encoded.json'
    data = process.DataFile(file_path)
    data.load_json()

def test_DataFile_load_json_double_encoded(temp_dir_with_json_files):
    file_path = temp_dir_with_json_files / 'json_double_encoded.json'
    data = process.DataFile(file_path)
    data.load_json()

def test_DataFile_load_json_does_not_eval(temp_dir_with_json_files):
    file_path = temp_dir_with_json_files / 'malicious_file.json'
    data = process.DataFile(file_path)

    with pytest.warns(UserWarning):
        data.load_json()

def test_DataFile_load_json_invalid_json(temp_dir_with_json_files):
    file_path = temp_dir_with_json_files / 'invalid_json.json'
    data = process.DataFile(file_path)

    with pytest.warns(UserWarning):
        data.load_json()