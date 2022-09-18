import json
import pytest
import pandas as pd
from process import process

@pytest.fixture(scope="session")
def temp_dir_with_json_files(tmp_path_factory):
    """Create a temp directory with no JSON files"""
    test_dir = tmp_path_factory.mktemp("data")

    dict_not_encoded = [{"A":"foo", "B":123}, {"C": "bar", "A":"baz"}]
    encoded_data = json.dumps(dict_not_encoded)
    double_encoded_data = json.dumps(encoded_data)

      # A file with a .txt extention but has a vaild JSON format
    file_path_1 = test_dir / "json_single_encoded.txt"
    file_path_1.touch()
    with open(file_path_1, "a", encoding="utf-8") as file:
        file.write(encoded_data)

    file_path_2 = test_dir / "json_single_encoded.json"
    file_path_2.touch()
    with open(file_path_2, "a", encoding="utf-8") as file:
        file.write(encoded_data)

    file_path_3 = test_dir / "json_double_encoded.json"
    file_path_3.touch()
    with open(file_path_3, "a", encoding="utf-8") as file:
        file.write(double_encoded_data)

    file_path_4 = test_dir / "empty_json_file.json"
    file_path_4.touch()

    file_path_5 = test_dir / "malicious_file.json"
    file_path_5.touch()
    with open(file_path_5, "a", encoding="utf-8") as file:
        file.write('print("this should not execute")')

    file_path_6 = test_dir / "invalid_json.json"
    file_path_6.touch()
    with open(file_path_6, "a", encoding="utf-8") as file:
        file.write("(" + encoded_data + ")")

    return test_dir

def test_DataFile_load_json_fails_with_text_file(temp_dir_with_json_files):
    file_path = temp_dir_with_json_files / 'json_single_encoded.txt'
    data = process.DataFile(file_path)

    with pytest.raises(Exception):
        data.load_json()

def test_DataFile_load_json_returns_pandas_df(temp_dir_with_json_files):
    file_path = temp_dir_with_json_files / 'json_double_encoded.json'
    data = process.DataFile(file_path)
    df = data.load_json(file_path)

    assert isinstance(df, pd.DataFrame)

def test_DataFile_load_json_empty_file(temp_dir_with_json_files):
    file_path = temp_dir_with_json_files / 'empty_json_file.json'
    data = process.DataFile(file_path)
    df = data.load_json_to_df(file_path)

    assert df.empty | df.isnull().all().all()

def test_DataFile_load_json_single_encoded(temp_dir_with_json_files):
    file_path = temp_dir_with_json_files / 'json_single_encoded.json'
    data = process.DataFile(file_path)
    data.load_json(file_path)

def test_load_json_to_df_double_encoded(temp_dir_with_json_files):
    file_path = temp_dir_with_json_files / 'json_double_encoded.json'
    process.load_json(file_path)

def test_load_json_to_df_does_not_eval(temp_dir_with_json_files):
    file_path = temp_dir_with_json_files / 'malicious_file.json'
    data = process.DataFile(file_path)

    with pytest.raises(Exception):
        data.load_json(file_path)

def test_load_json_to_df_invalid_json(temp_dir_with_json_files):
    file_path = temp_dir_with_json_files / 'invalid_json.json'
    data = process.DataFile(file_path)

    with pytest.raises(Exception):
        data.load_json(file_path)