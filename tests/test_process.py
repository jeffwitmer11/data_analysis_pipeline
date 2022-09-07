""" Unit tests of process.py """
import pytest
import pandas as pd
from process import process

@pytest.fixture(scope="session")
def temp_dir_with_2_json_files(tmp_path_factory):
    """Create a temp directory with 2 JSON files"""
    test_dir = tmp_path_factory.mktemp("data")
    nested_dir = test_dir / "nested"
    nested_dir.mkdir()

    file_path_1 = test_dir / "file_1.json"
    file_path_1.touch()
    file_1 = open(file_path_1, "a")
    file_1.write(r'"[{\"A\": \"foo\", \"B\": 123}, {\"C\": \"bar\", \"A\":\"baz\"}]"')
    file_1.close()

    file_2 = nested_dir / "file_2.json"
    file_2.touch()

    return test_dir

def test_get_files_from_path(temp_dir_with_2_json_files):
    """Test get_files_from_path"""
    test_len = len(process.get_files_from_path(temp_dir_with_2_json_files))
    assert test_len == 2

def test_load_json_to_df(temp_dir_with_2_json_files):
    """Test load_json_to_df"""
    file_path = temp_dir_with_2_json_files / 'file_1.json'
    df = process.load_json_to_df(file_path)
    assert df.shape == (2,3)

def test_determine_process_or_skip(temp_dir_with_2_json_files):
    """Test determine_process_or_skip"""
    file_path = temp_dir_with_2_json_files / 'file_1.json'
    dat = process.DataFile(file_path)

    df = pd.DataFrame({"A":[1,None,3], "B":[2,None,4]})
    res = dat.determine_process_or_skip(df)
    assert((sum(res["process"]) == 2) & (sum(res["skip"]) == 1))

# TODO: Add many more unit tests