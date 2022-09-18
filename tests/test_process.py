""" Unit tests of process.py """
import os
import json
import pytest
import pandas as pd
from process import process

@pytest.fixture(scope="session")
def tmp_output_dir(tmp_path_factory):
    """A temporary directory to write output to"""
    tmp_dir = tmp_path_factory.mktemp("output")

    return tmp_dir

@pytest.mark.filterwarnings("ignore::UserWarning")
def test_process(temp_dir_with_json_files, tmp_output_dir):
    input_path = temp_dir_with_json_files
    output_file_path = tmp_output_dir / "processed_data.csv"
    process.process(input_path = input_path,
                    output_file_path = output_file_path)

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



def test_process_no_data(temp_dir_with_json_files, tmp_output_dir):
    """Should not error when no data exists"""
    input_path = temp_dir_with_json_files / "empty_dir"
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
