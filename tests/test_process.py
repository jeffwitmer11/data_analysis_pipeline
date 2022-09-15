""" Unit tests of process.py """
import pytest
import os
import pandas as pd
from process import process

@pytest.fixture(scope="session")
def temp_dir_json_files(tmp_path_factory):
    """Create a temp directory with 2 JSON files"""
    test_dir = tmp_path_factory.mktemp("data")
    nested_dir = test_dir / "nested"
    nested_dir.mkdir()

    file_path_1 = test_dir / "file_1.json"
    file_path_1.touch()
    with open(file_path_1, "a") as file:
        file.write(r'"[{\"user\": {\"first_name\": \"Darlene\", \"middle_name\": \"Brenda\", \"last_name\": \"Puckett\", \"zip_code\": 24065}}]"')

    file_2 = nested_dir / "file_2.txt"
    file_2.touch()

    return test_dir


"""
def test_determine_process_or_skip(temp_dir_with_2_json_files):
    file_path = temp_dir_with_2_json_files / 'file_1.json'
    dat = process.DataFile(file_path)

    df = pd.DataFrame({"A":[1,None,3], "B":[2,None,4]})
    res = dat.determine_process_or_skip(df)
    assert((sum(res["process"]) == 2) & (sum(res["skip"]) == 1))
"""

# TODO: Add many more unit tests

@pytest.fixture(scope="session")
def test_dir(tmp_path_factory):
    """Create a temp directory with 2 JSON files"""
    test_dir = tmp_path_factory.mktemp("output")

    return test_dir

def test_process_adds_output_file(test_dir):

    output_file_path = test_dir / "processed_data.csv"
    if os.path.exists(output_file_path):
        os.remove(output_file_path)

    process.process(input_path = "tests/test_data",
                    output_file_path = output_file_path)

    assert os.path.exists(output_file_path)

def test_process_overwrites_output_file(test_dir):
    output_file_path = test_dir / "processed_data.csv"

    process.process(input_path = "tests/test_data",
                    output_file_path = output_file_path)
    first_file_time = os.path.getmtime(output_file_path)

    process.process(input_path = "tests/test_data",
                    output_file_path = output_file_path)
    second_file_time = os.path.getmtime(output_file_path)

    assert first_file_time < second_file_time

def test_process_single_record(temp_dir_json_files, test_dir):
    input_path = temp_dir_json_files
    output_file_path = test_dir / "processed_data.csv"

    process.process(input_path = input_path,
                    output_file_path = output_file_path)

def test_process_no_data(temp_dir_json_files, test_dir):
    input_path = temp_dir_json_files / "nested_dir"
    output_file_path = test_dir / "processed_data.csv"

    process.process(input_path = input_path,
                    output_file_path = output_file_path)


# TODO: Add unit test to ensure only one header exists in the output data
