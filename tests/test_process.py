""" Unit tests of process.py """
import os
import json
import pytest
import pandas as pd
from process import process

@pytest.fixture(scope="session")
def tmp_json_dir(tmp_path_factory):
    """A temporary directory with 2 files"""
    tmp_dir = tmp_path_factory.mktemp("data")

    # A JSON file that mimicks client data
    file_1 = tmp_dir / "single_record.json"
    file_1.touch()
    raw_data = [
        {"user1":
            {"first_name": "Bob",
            "middle_name": "The Maestro",
            "last_name": "Cobb",
            "zip_code": 56010}
        }]
    with open(file_1, "a", encoding="utf-8") as file:
        # Using double encoding to mimick the format of the current client's
        # data
        file.write(json.dumps(json.dumps(raw_data)))

    # Create a nested folder to test access to nested folders and how empty
    # files are handled
    nested_dir = tmp_dir / "nested"
    nested_dir.mkdir()
    file_2 = nested_dir / "empty_file.txt"
    file_2.touch()

    return tmp_dir

@pytest.fixture(scope="session")
def tmp_output_dir(tmp_path_factory):
    """A temporary directory to write output to"""
    tmp_dir = tmp_path_factory.mktemp("output")

    return tmp_dir

def test_process_adds_output_file(tmp_output_dir):
    """Processing the data should create a new file to store the output data"""

    output_file_path = tmp_output_dir / "processed_data.csv"
    if os.path.exists(output_file_path):
        os.remove(output_file_path)

    process.process(input_path = "tests/test_data",
                    output_file_path = output_file_path)

    assert os.path.exists(output_file_path)

def test_process_overwrites_output_file(tmp_output_dir):
    """If a file exists, it should be overwritten"""

    output_file_path = tmp_output_dir / "processed_data.csv"

    process.process(input_path = "tests/test_data",
                    output_file_path = output_file_path)
    first_file_time = os.path.getmtime(output_file_path)

    process.process(input_path = "tests/test_data",
                    output_file_path = output_file_path)
    second_file_time = os.path.getmtime(output_file_path)

    assert first_file_time < second_file_time

def test_process_single_record(tmp_json_dir, tmp_output_dir):
    """Should not error when on a single record in the data exists"""
    input_path = tmp_json_dir
    output_file_path = tmp_output_dir / "processed_data.csv"
    process.process(input_path = input_path,
                    output_file_path = output_file_path)

def test_process_no_data(tmp_json_dir, tmp_output_dir):
    """Should not error when no data exists"""
    input_path = tmp_json_dir / "nested_dir"
    output_file_path = tmp_output_dir / "processed_data.csv"
    process.process(input_path = input_path,
                    output_file_path = output_file_path)

def test_process_one_header(tmp_output_dir):
    """When data is appened to the output file, only one header should be exist.
    """
    output_file_path = tmp_output_dir / "processed_data.csv"
    process.process(input_path = "tests/test_data",
                    output_file_path = output_file_path)

    df = pd.read_csv(output_file_path)

    assert not any(df.isin(df.columns).all(axis=1))
