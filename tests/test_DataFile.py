import pytest
import random
from process import process

@pytest.fixture(scope="session")
def make_test_data_file(temp_dir_with_json_files):
    def make_data_file():
        file_path = temp_dir_with_json_files / "json_double_encoded.json"
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
