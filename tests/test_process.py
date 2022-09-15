""" Unit tests of process.py """
import pytest
import os
import json
import pandas as pd
from process import process

@pytest.fixture(scope="session")
def tmp_json_dir(tmp_path_factory):
    """Create a temp directory with 2 JSON files"""
    tmp_dir = tmp_path_factory.mktemp("data")
    nested_dir = tmp_dir / "nested"
    nested_dir.mkdir()

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
        file.write(json.dumps(json.dumps(raw_data)))

    file_2 = nested_dir / "empty_file.txt"
    file_2.touch()

    return tmp_dir


@pytest.fixture(scope="session")
def tmp_output_dir(tmp_path_factory):
    """Create a temp directory to write output to"""
    tmp_dir = tmp_path_factory.mktemp("output")

    return tmp_dir

def test_process_adds_output_file(tmp_output_dir):

    output_file_path = tmp_output_dir / "processed_data.csv"
    if os.path.exists(output_file_path):
        os.remove(output_file_path)

    process.process(input_path = "tests/test_data",
                    output_file_path = output_file_path)

    assert os.path.exists(output_file_path)

def test_process_overwrites_output_file(tmp_output_dir):
    output_file_path = tmp_output_dir / "processed_data.csv"

    process.process(input_path = "tests/test_data",
                    output_file_path = output_file_path)
    first_file_time = os.path.getmtime(output_file_path)

    process.process(input_path = "tests/test_data",
                    output_file_path = output_file_path)
    second_file_time = os.path.getmtime(output_file_path)

    assert first_file_time < second_file_time

def test_process_single_record(tmp_json_dir, tmp_output_dir):
    input_path = tmp_json_dir
    output_file_path = tmp_output_dir / "processed_data.csv"

    process.process(input_path = input_path,
                    output_file_path = output_file_path)

def test_process_no_data(tmp_json_dir, tmp_output_dir):
    input_path = tmp_json_dir / "nested_dir"
    output_file_path = tmp_output_dir / "processed_data.csv"

    process.process(input_path = input_path,
                    output_file_path = output_file_path)

def test_process_one_header(tmp_output_dir):
    output_file_path = tmp_output_dir / "processed_data.csv"

    process.process(input_path = "tests/test_data",
                    output_file_path = output_file_path)

    df = pd.read_csv(output_file_path)

    print((df == df.columns).all(axis=1))
    assert ~any((df == df.columns).all(axis=1))

