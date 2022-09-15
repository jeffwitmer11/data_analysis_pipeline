import pytest
import json
import random
from process import process

@pytest.fixture(scope="session")
def temp_dir_json_files(tmp_path_factory):
    """Create a temp directory with 2 JSON files"""
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
                "middle_name": "The Maestro",
                "last_name": "Cobb",
                "zip_code": 56010}
                }
        }]

    file_path = test_dir / "test_file.json"
    file_path.touch()
    with open(file_path, "a") as file:
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

"""
def test_factory(make_test_data_file):
    my_data = make_test_data_file()
    print(my_data.all_records)
"""

def test_DataFile_column_names(make_test_data_file):
    #file_path = temp_dir_json_files / "test_file.json"
    #data = process.DataFile(file_path)
    data = make_test_data_file()
    test_cols = ['foo', 'bar', 'baz']
    data.required_cols = test_cols
    data = data.read_data()
    assert set(data.all_records.columns) == set(test_cols)

def test_DataFile_num_records(make_test_data_file):
    #file_path = temp_dir_json_files / "test_file.json"
    #data = (process.DataFile(file_path).read_data())
    data = make_test_data_file()
    assert len(data.all_records) == 2

def test_DataFile_skipped_processed(make_test_data_file):
    #file_path = temp_dir_json_files / "test_file.json"
    #data = (process.DataFile(file_path).read_data())
    data = make_test_data_file()
    assert data.num_processed + data.num_skipped == len(data.all_records)

def test_DataFile_processed_records(make_test_data_file):
    #file_path = temp_dir_json_files / "test_file.json"
    #data = (process.DataFile(file_path)
    #    .read_data()
     #   .process_records())
    #data = data.process_records()
    data = make_test_data_file()
    print(data.num_processed)
    print(data.processed_records)
    print(data.records_info)
    assert data.num_processed == len(data.processed_records)

def test_DataFile_metadata_for_every_record(make_test_data_file):
    #file_path = temp_dir_json_files / "test_file.json"
    #data = (process.DataFile(file_path)
     #   .read_data()
      #  .process_records())
    data = make_test_data_file()
    assert len(data.records_info) == len(data.all_records)

def test_DataFile_metadata_joins_to_records(temp_dir_json_files):
    file_path = temp_dir_json_files / "test_file.json"
    data = (process.DataFile(file_path)
        .read_data())

    n_records = len(data.all_records)
    random_index = random.sample(range(n_records, n_records*2), n_records)

    data.all_records["index"] = random_index
    data.all_records.set_index("index")

    data = data.process_records()

    assert data.all_records.index.equals(data.records_info.index)

def test_DataFile_num_processed(make_test_data_file):
    #file_path = temp_dir_json_files / "test_file.json"
    #data = (process.DataFile(file_path)
     #   .read_data()
      #  .process_records())
    data = make_test_data_file()
    assert data.num_processed == sum(data.records_info["process"])

def test_DataFile_num_skipped(make_test_data_file):
    #file_path = temp_dir_json_files / "test_file.json"
    #data = (process.DataFile(file_path)
     #   .read_data()
      #  .process_records())
    data = make_test_data_file()
    assert data.num_skipped == sum(data.records_info["skip"])




# DataFile write output
# DataFile set_records info
# "skip" and "process" are returned

# self.num_processed == nrow(processed_records)
# self.num_processed and self.num_skipped match records_info
