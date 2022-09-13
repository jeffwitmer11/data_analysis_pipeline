import pytest
import pandas as pd
import json
from process import process

# load_json_to_df
# File that is not a JSON File
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
    with open(file_path_1, "a") as file:
        file.write(encoded_data)

    file_path_2 = test_dir / "json_single_encoded.json"
    file_path_2.touch()
    with open(file_path_2, "a") as file:
        file.write(encoded_data)

    file_path_3 = test_dir / "json_double_encoded.json"
    file_path_3.touch()
    with open(file_path_3, "a") as file:
        file.write(double_encoded_data)

    file_path_4 = test_dir / "empty_json_file.json"
    file_path_4.touch()

    file_path_5 = test_dir / "malicious_file.json"
    file_path_5.touch()
    with open(file_path_5, "a") as file:
        file.write('print("this should not execute")')

    file_path_6 = test_dir / "invalid_json.json"
    file_path_6.touch()
    with open(file_path_6, "a") as file:
        file.write("(" + encoded_data + ")")

    """
    # A file with a .txt extention but has a vaild JSON format
    file_path_1 = test_dir / "file_1.txt"
    file_path_1.touch()
    file_1 = open(file_path_1, "a")
    file_1.write(r'"[{\"A\": \"foo\", \"B\": 123}, {\"C\": \"bar\", \"A\":\"baz\"}]"')
    file_1.close()

    file_path_2 = test_dir / "json_file_double_encoded.json"
    file_path_2.touch()
    with open(file_path_2, "a") as file:
        file.write(r'"[{\"A\": \"foo\", \"B\": 123}, {\"C\": \"bar\", \"A\":\"baz\"}]"')

    file_path_3 = test_dir / "empty_json_file.json"
    file_path_3.touch()

    file_path_4 = test_dir / "json_file_single_encoded.json"
    file_path_4.touch()
    with open(file_path_4, "a") as file:
        file.write(r'[{\"A\": \"foo\", \"B\": 123}, {\"C\": \"bar\", \"A\":\"baz\"}]')

    file_path_4 = test_dir / "json_file_.json"
    file_path_4.touch()
    with open(file_path_4, "a") as file:
        file.write(r'{\"A\": \"foo\", \"B\": 123}, {\"C\": \"bar\", \"A\":\"baz\"}')
"""
    return test_dir

def test_load_json_to_df_fails_with_text_file(temp_dir_with_json_files):
    file_path = temp_dir_with_json_files / 'json_single_encoded.txt'
    with pytest.raises(Exception):
        process.load_json_to_df(file_path)

def test_load_json_to_df_returns_pandas_df(temp_dir_with_json_files):
    file_path = temp_dir_with_json_files / 'json_double_encoded.json'
    df = process.load_json_to_df(file_path)
    assert isinstance(df, pd.DataFrame)

def test_load_json_to_df_empty_file(temp_dir_with_json_files):
    file_path = temp_dir_with_json_files / 'empty_json_file.json'
    df = process.load_json_to_df(file_path)
    assert df.empty

def test_load_json_to_df_single_encoded(temp_dir_with_json_files):
    file_path = temp_dir_with_json_files / 'json_single_encoded.json'
    process.load_json_to_df(file_path)

def test_load_json_to_df_double_encoded(temp_dir_with_json_files):
    file_path = temp_dir_with_json_files / 'json_double_encoded.json'
    process.load_json_to_df(file_path)

def test_load_json_to_df_does_not_eval(temp_dir_with_json_files):
    file_path = temp_dir_with_json_files / 'malicious_file.json'
    with pytest.raises(Exception):
        process.load_json_to_df(file_path)

def test_load_json_to_df_invalid_json(temp_dir_with_json_files):
    file_path = temp_dir_with_json_files / 'invalid_json.json'
    with pytest.raises(Exception):
        process.load_json_to_df(file_path)

