"""File to test upload.py functionality"""
# to run tests: python -m pytest tests/upload_test.py -v

import pytest
from app.upload import string_num_to_int, csv_to_parquet, file_hash, parquet_to_gcs, _parquet_to_gcs
import pandas as pd
import hashlib

def test_string_num_to_int():
    assert string_num_to_int("ten") == 10
    assert string_num_to_int("4") == 4

# create a test for csv_to_parquet that takes in multiple csv file paths and converts those to parquet
def test_csv_to_parquet(tmp_path):
    # create a sample csv file
    csv_files = []
    csv_file = tmp_path / "test.csv"
    csv_file.write_text("Date,Quantity\n2021-01-01,ten\n2021-01-02,4")
    csv_files.append(str(csv_file))
    
    # convert to parquet
    output_file = tmp_path / "output.parquet"
    csv_to_parquet(csv_files, str(output_file))
    
    # read the parquet file back into a dataframe and check the values
    df = pd.read_parquet(str(output_file))
    assert df["Quantity"].tolist() == [10, 4]

def test_csv_to_parquet_invalid_file(tmp_path):
    # raises a RuntimeError because of ArrowIOError, which is caught and re-raised as RuntimeError in the function
    with pytest.raises(RuntimeError):
        csv_to_parquet(["NotAValidFile"], [])

def test_csv_to_parquet_malformed(tmp_path):
    # create a malformed csv file
    csv_files = []
    csv_file = tmp_path / "malformed.csv"
    csv_file.write_text("""
    col1,col2
    1,2
    3,4,5
    6,7""") # missing quantity value
    csv_files.append(str(csv_file))

    # raises a RuntimeError because of pd.errors.ParserError, which is caught and re-raised as RuntimeError in the function
    with pytest.raises(RuntimeError):
        csv_to_parquet([str(csv_file)], str(tmp_path / "output.parquet"))

def test_file_hash_md5(tmp_path):
    """Test standard md5 hashing of a file using a temporary file."""
    # create a dummy file
    dummy_file = tmp_path / "sub"
    dummy_file.mkdir()
    path = dummy_file / "hello.txt"
    content = b"Hello, world"
    path.write_bytes(content)

    expected_hash = hashlib.md5(content).hexdigest()
    assert(file_hash(str(path)) == expected_hash)
