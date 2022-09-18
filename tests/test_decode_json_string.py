import json
import pytest
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

def test_decode_json_string_fails_with_text_file(temp_dir_with_json_files):
    file_path = temp_dir_with_json_files / 'json_single_encoded.txt'
    with pytest.raises(Exception):
        process.decode_json_string(file_path)

def test_decode_json_string_returns_decoded(temp_dir_with_json_files):
    file_path = temp_dir_with_json_files / 'json_double_encoded.json'
    df = process.decode_json_string(file_path)
    assert not isinstance(df, str)

def test_decode_json_string_empty_file(temp_dir_with_json_files):
    file_path = temp_dir_with_json_files / 'empty_json_file.json'
    df = process.decode_json_string(file_path)
    assert not bool(df)

def test_decode_json_string_single_encoded(temp_dir_with_json_files):
    file_path = temp_dir_with_json_files / 'json_single_encoded.json'
    process.decode_json_string(file_path)

def test_decode_json_string_double_encoded(temp_dir_with_json_files):
    file_path = temp_dir_with_json_files / 'json_double_encoded.json'
    process.decode_json_string(file_path)

def test_decode_json_string_does_not_eval(temp_dir_with_json_files):
    file_path = temp_dir_with_json_files / 'malicious_file.json'
    with pytest.raises(Exception):
        process.decode_json_string(file_path)

def test_decode_json_string_invalid_json(temp_dir_with_json_files):
    file_path = temp_dir_with_json_files / 'invalid_json.json'
    with pytest.raises(Exception):
        process.decode_json_string(file_path)
