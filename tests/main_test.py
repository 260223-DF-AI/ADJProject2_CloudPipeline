import pytest
import requests
from pathlib import Path
from google.cloud import bigquery, storage
from dotenv import load_dotenv
load_dotenv()

BASE_URL = "http://127.0.0.1:8000"

def test_simple_get_endpoint():
    url = f"{BASE_URL}"
    response = requests.get(url)
    assert response.status_code == 200
    data = response.json()
    assert "message" in data
    assert data["message"] == "Hello from main"
    assert isinstance(data, dict)

def test_parquet_exists_local():
    parquet_file_path_local = Path("data/sales-data.parquet")
    assert parquet_file_path_local.is_file() == True

def test_bucket_exists_gcs():
    client = storage.Client()
    bucket = 'project2_files_adj'
    BUCKET = client.get_bucket(bucket)
    assert BUCKET.exists() == True

def test_creating_table_endpoint():
    url = f"{BASE_URL}/creating_table"
    response = requests.get(url)
    assert response.status_code == 200
    data = response.json()
    assert "message" in data
    assert data["message"] == "sales-data table created"
    assert isinstance(data, dict)

def test_table_exists_gcs():
    table_path = "project2-cloudpipeline.sales_dataset.sales-data"
    try:
        client = bigquery.Client()
        table = client.get_table(table_path)
        assert table is not None
        assert table.table_id == "sales-data"
    except Exception as e:
        print(f"Error occured: {e}")

def test_sales_by_region_endpoint():
    url = f"{BASE_URL}/sales-by-region"
    response = requests.get(url)
    assert response.status_code == 200

    data = response.json()[0]
    print("Data: " + str(data))
    assert "Region" in data
    assert data["Region"] == "West"

    data = response.json()[1]
    print("Data: " + str(data))
    assert "Region" in data
    assert data["Region"] == "East"

    data = response.json()[2]
    print("Data: " + str(data))
    assert "Region" in data
    assert data["Region"] == "South"

    data = response.json()[3]
    print("Data: " + str(data))
    assert "Region" in data
    assert data["Region"] == "Midwest"

def test_total_file_length_endpoint():
    url = f"{BASE_URL}/total-file-length"
    response = requests.get(url)
    assert response.status_code == 200
    data = response.json()[0]
    assert "Total_file_length" in data
    assert data["Total_file_length"] == 1237450
    assert isinstance(data, dict)