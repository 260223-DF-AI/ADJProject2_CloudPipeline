from fastapi import FastAPI, Query, Depends, HTTPException
from .routers import apiroutes
from .upload import csv_to_parquet, parquet_to_gcs
from dotenv import load_dotenv
from google.cloud import bigquery
import os

load_dotenv()

app = FastAPI(
    title = "ADJProject2 Cloud Pipeline API",
    description = "API for getting and posting sales data",
    version = "0.0.1"
)

app.include_router(apiroutes.router)

@app.get("/")
def get_root():
    return {"message": "Hello from main"}

def get_bq_client():
    """Connect to BigQuery"""
    with bigquery.Client() as client:
        return client

def prep_query():
    """Format the SQL query we want to run into BigQuery"""
    table_id = "project2-cloudpipeline.sales_dataset.sales-data"
    query = f"""
    SELECT StoreLocation
    FROM `{table_id}`
    WHERE TransactionID = 1
    """
    return query

@app.get("/gcs-query")
async def get_item_test(item_id: int, bq_client: bigquery.client.Client = Depends(get_bq_client)):
    """
    Handles GET requests to the gcs-query URL and queries BigQuery.
    """
    query = prep_query()
    try:
        bq_client = bigquery.Client()
        query_job = bq_client.query(query)
        result = query_job.result().to_dataframe()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"BigQuery query failed: {e}")
    return result.to_dict(orient="records")

@app.get("/top-n-products")
def get_top_N_products(n: int, bq_client: bigquery.client.Client = Depends(get_bq_client)):
    """
    Handles GET requests to the gcs-query URL and queries BigQuery.
    """
    query = f"""
            select ProductName, TotalAmount
            from `project2-cloudpipeline.sales_dataset.sales-data`
            order by TotalAmount desc
            limit {n}
            """
    try:
        bq_client = bigquery.Client()
        query_job = bq_client.query(query)
        result = query_job.result().to_dataframe()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"BigQuery query failed: {e}")
    return result.to_dict(orient="records")

@app.get("/total-file-length")
def get_total_length(bq_client: bigquery.client.Client = Depends(get_bq_client)):
    """
    Handles GET requests to the gcs-query URL and queries BigQuery.
    """
    query = f"""
            select count(*) as `Total file length`
            from `project2-cloudpipeline.sales_dataset.sales-data`
            """
    try:
        bq_client = bigquery.Client()
        query_job = bq_client.query(query)
        result = query_job.result().to_dataframe()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"BigQuery query failed: {e}")
    return result.to_dict(orient="records")

@app.post("/convert")
def post_root():
    FILE_PATHS = [os.getenv("SALES_CSV1"), os.getenv("SALES_CSV2"), os.getenv("SALES_CSV3"), os.getenv("SALES_CSV4"), os.getenv("SALES_CSV5")]
    OUTPUT_FILE = os.getenv("PARQUET_FILE")
    csv_to_parquet(FILE_PATHS, OUTPUT_FILE)
    parquet_to_gcs(OUTPUT_FILE)
    return {"message": "CSV to Parquet conversion and Parquet to GSC complete"}

# run: uvicorn app.main:app --reload