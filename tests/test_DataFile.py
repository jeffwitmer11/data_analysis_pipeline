import pytest
import json
import random
from process import process

@pytest.fixture(scope="session")
def temp_dir_json_files(tmp_path_factory):
    """A temperary directory with 2 JSON files"""
    test_dir = tmp_path_factory.mktemp("data")

    raw_data = [
        {"user1":
            {"first_name": "Darlene",
            "middle_name": "Brenda",
            "last_name": "Puckett",
            "zip_code": 24065}
        },

        {"user2":
            {"age": 99,
            "email": "email@email.com",
            "info":
                {"first_name": "Bob",
                "middle_name": "c",
                "last_name": "Cobb",
                "zip_code": 56010}
                }
        }]

    file_path = test_dir / "test_file.json"
    file_path.touch()
    with open(file_path, "a", encoding="utf-8") as file:
        # Using double encoding of the data to mimick the format of the
        # currrent client's data
        file.write(json.dumps(json.dumps(raw_data)))

    return test_dir

@pytest.fixture(scope="session")
def make_test_data_file(temp_dir_json_files):
    def make_data_file():
        file_path = temp_dir_json_files / "test_file.json"
        data_file = (process.DataFile(file_path)
            .read_data()
            .process_records()
        )
        return data_file

    return make_data_file

def test_DataFile_column_names(make_test_data_file):
    data = make_test_data_file()
    test_cols = ['foo', 'bar', 'baz']
    data.required_cols = test_cols
    data = data.read_data()
    assert set(data.all_records.columns) == set(test_cols)

def test_DataFile_num_records(make_test_data_file):
    data = make_test_data_file()
    assert len(data.all_records) == 2

def test_DataFile_skipped_processed(make_test_data_file):
    data = make_test_data_file()
    assert data.num_processed + data.num_skipped == len(data.all_records)

def test_DataFile_processed_records(make_test_data_file):
    data = make_test_data_file()
    assert data.num_processed == len(data.processed_records)

def test_DataFile_metadata_for_every_record(make_test_data_file):
    data = make_test_data_file()
    assert len(data.records_info) == len(data.all_records)

def test_DataFile_metadata_joins_to_records(make_test_data_file):
    data = make_test_data_file()

    n_records = len(data.all_records)
    random_index = random.sample(range(n_records, n_records*2), n_records)

    data.all_records["index"] = random_index
    data.all_records.set_index("index")

    # Reprocess the data, need to ensure that the newly set index is retained
    # when the data is processed
    data = data.process_records()

    assert data.all_records.index.equals(data.records_info.index)

def test_DataFile_num_processed(make_test_data_file):
    data = make_test_data_file()
    assert data.num_processed == sum(data.records_info["process"])

def test_DataFile_num_skipped(make_test_data_file):
    data = make_test_data_file()
    assert data.num_skipped == sum(data.records_info["skip"])




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

def test_DataFile_load_json_fails_with_text_file(temp_dir_with_json_files):
    file_path = temp_dir_with_json_files / 'json_single_encoded.txt'
    data = process.DataFile(file_path)

    with pytest.raises(Exception):
        data.load_json()

def test_DataFile_load_json_returns_pandas_df(temp_dir_with_json_files):
    file_path = temp_dir_with_json_files / 'json_double_encoded.json'
    data = process.DataFile(file_path)
    df = data.load_json(file_path)

    assert isinstance(df, pd.DataFrame)

def test_DataFile_load_json_empty_file(temp_dir_with_json_files):
    file_path = temp_dir_with_json_files / 'empty_json_file.json'
    data = process.DataFile(file_path)
    df = data.load_json_to_df(file_path)

    assert df.empty | df.isnull().all().all()

def test_DataFile_load_json_single_encoded(temp_dir_with_json_files):
    file_path = temp_dir_with_json_files / 'json_single_encoded.json'
    data = process.DataFile(file_path)
    data.load_json(file_path)

def test_load_json_to_df_double_encoded(temp_dir_with_json_files):
    file_path = temp_dir_with_json_files / 'json_double_encoded.json'
    process.load_json(file_path)

def test_load_json_to_df_does_not_eval(temp_dir_with_json_files):
    file_path = temp_dir_with_json_files / 'malicious_file.json'
    data = process.DataFile(file_path)

    with pytest.raises(Exception):
        data.load_json(file_path)

def test_load_json_to_df_invalid_json(temp_dir_with_json_files):
    file_path = temp_dir_with_json_files / 'invalid_json.json'
    data = process.DataFile(file_path)

    with pytest.raises(Exception):
        data.load_json(file_path)
