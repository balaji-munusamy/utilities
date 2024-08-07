import pytest

from utils.file_utils import FileUtils

@pytest.fixture
def test_file_path():
    return "data/some.parquet"

def test_read_file(test_file_path):
    FileUtils.convert_parquet_to_csv(test_file_path, test_file_path.replace(".parquet", ".csv"))