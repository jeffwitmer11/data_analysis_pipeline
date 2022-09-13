import pytest
import json
import pandas as pd
from process import process

@pytest.fixture(scope="session")
def temp_dir_with_2_files(tmp_path_factory):
    """Create a temp directory with 2 files"""
    test_dir = tmp_path_factory.mktemp("data")
    nested_dir = test_dir / "nested"
    nested_dir.mkdir()

    empty_dir = test_dir / "empty"
    empty_dir.mkdir()

    file_path_1 = test_dir / "file_1.txt"
    file_path_1.touch()

    file_path_2 = nested_dir / "file_2.json"
    file_path_2.touch()

    return test_dir

def test_get_files_from_path_number_of_files(temp_dir_with_2_files):
    test_len = len(process.get_files_from_path(temp_dir_with_2_files))
    assert test_len == 2

def test_get_files_from_path_empty_folder(temp_dir_with_2_files):
    file_path = temp_dir_with_2_files / "empty"
    file_list = process.get_files_from_path(file_path)
    assert not file_list

def test_get_files_from_path_file_not_folder(temp_dir_with_2_files):
    file_path = temp_dir_with_2_files / "file_1.text"
    file_list = process.get_files_from_path(file_path)
    assert not file_list

def test_get_files_from_path_returns_list(temp_dir_with_2_files):
    file_path = temp_dir_with_2_files
    file_list = process.get_files_from_path(file_path)
    assert isinstance(file_list, list)

def test_get_files_from_path_extention(temp_dir_with_2_files):
    file_path = temp_dir_with_2_files
    file_list = process.get_files_from_path(file_path, extension = ".txt")
    assert file_list[0].endswith(".txt")

def test_get_files_from_path_no_ext(temp_dir_with_2_files):
    file_path = temp_dir_with_2_files
    file_list = process.get_files_from_path(file_path, extension = ".jpg")
    assert not file_list
