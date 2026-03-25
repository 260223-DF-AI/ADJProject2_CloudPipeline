import requests
from google.cloud import bigquery

API_BASE_URL = "http://localhost:8000"
def run_pipeline():
    # convert csv to parquet
    try:
        resp = requests.post(f"{API_BASE_URL}/convert", timeout=100)
        resp.raise_for_status()
        #result = resp.json()
    except Exception as e:
        return (str(e))
    
    # create table in bigquery
    try:
        resp = requests.get(f"{API_BASE_URL}/creating_table", timeout=100)
        resp.raise_for_status()
        #result2 = resp.json()
    except Exception as e:
        return (str(e))
    
    return "OK"



def delete_table_bigquery():
    try:
        resp = requests.delete(f"{API_BASE_URL}/delete-table", timeout=100)
        resp.raise_for_status()
        #result = resp.json()
    except Exception as e:
        return (str(e))
    return "OK"

def query_sales_data(customerid):
    try:
        # Pass query parameters in the URL
        params = {"customerId": customerid}
        resp = requests.get(f"{API_BASE_URL}/query", params=params, timeout=100)
        resp.raise_for_status()  # Raises an exception for HTTP errors
        result = resp.json()
        return result
    except requests.RequestException as e:
        return {"error": str(e)} 
    
def get_top_N_products_by_quantity(n):
    try:
        params = {"n": n}
        resp = requests.get(f"{API_BASE_URL}/top-n-products-by-quantity", params=params, timeout=100)
        resp.raise_for_status()
        result = resp.json()
        return result
    except requests.RequestException as e:
        return {"error": str(e)}
    

def get_top_N_products_by_revenue(n):
    try:
        params = {"n": n}
        resp = requests.get(f"{API_BASE_URL}/top-n-products-by-revenue", params=params, timeout=100)
        resp.raise_for_status()
        result = resp.json()
        return result
    except requests.RequestException as e:
        return {"error": str(e)}