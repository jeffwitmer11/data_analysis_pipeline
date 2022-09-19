import pytest
from process import process

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

    with pytest.warns(UserWarning):
        process.decode_json_string(file_path)

def test_decode_json_string_invalid_json(temp_dir_with_json_files):
    file_path = temp_dir_with_json_files / 'invalid_json.json'

    with pytest.warns(UserWarning):
        process.decode_json_string(file_path)
