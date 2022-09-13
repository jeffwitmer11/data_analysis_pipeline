import pytest
import json
import pandas as pd
from process import process

@pytest.fixture(scope="session")
def temp_dir_with_2_json_files(tmp_path_factory):
    """Create a temp directory with 2 JSON files"""
    test_dir = tmp_path_factory.mktemp("data")
    nested_dir = test_dir / "nested"
    nested_dir.mkdir()

    dict_not_encoded = [{"A":"foo", "B":123}, {"C": "bar", "A":"baz"}]
    encoded_data = json.dumps(dict_not_encoded)
    double_encoded_data = json.dumps(encoded_data)

      # A file with a .txt extention but has a vaild JSON format
    file_path_1 = test_dir / "json_single_encoded.txt"
    file_path_1.touch()
    with open(file_path_1, "a") as file:
        file.write(double_encoded_data)

    file_path_2 = nested_dir / "file_2.json"
    file_path_2.touch()

    return test_dir

def test_get_files_from_path(temp_dir_with_2_json_files):
    """Test get_files_from_path"""
    test_len = len(process.get_files_from_path(temp_dir_with_2_json_files))
    assert test_len == 2
# test_get_files_from_path
# empty directory
# just a file, not a directory supplied
# must return list
# must return valid file paths (use a regex to check)