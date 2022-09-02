import pytest
from process import process

@pytest.fixture(scope="session")
def temp_dir_with_2_json_files(tmp_path_factory):
    test_dir = tmp_path_factory.mktemp("data")
    nested_dir = test_dir / "nested"
    nested_dir.mkdir()

    f1 = test_dir / "file_1.json"
    f1.touch()
    f2 = nested_dir / "file_2.json"
    f2.touch()
    return test_dir

def test_get_files_from_path(temp_dir_with_2_json_files):
    test_len = len(process.get_files_from_path(temp_dir_with_2_json_files))
    assert test_len == 2


# len processed and written data = 172073
#print(sys.path)

#sys.path 