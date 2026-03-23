from fastapi import FastAPI, Query, Depends
from .routers import apiroutes
from .upload import csv_to_parquet, parquet_to_gcs, FILE_PATHS, OUTPUT_FILE
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

def get_bq_client():
    """Connect to BigQuery"""
    with bigquery.Client() as client:
        return client

def prep_query():
    """Format the SQL query we want to run into BigQuery"""
    bucket_name = os.getenv("BUCKET_NAME")
    query = """
    SELECT ...
    FROM `bigquery...`
    """
    return query

@app.get("/")
async def get_item_test(item_id: int, bq_client: bigquery.client.Client = Depends(get_bq_client)):
    query = prep_query()

    bq_client = bigquery.Client()
    query_job = bq_client.query(query)
    result = query_job.query().to_dataframe()

    return result


@app.post("/convert")
def post_root():
    csv_to_parquet(FILE_PATHS, OUTPUT_FILE)
    parquet_to_gcs(OUTPUT_FILE)
    return {"message": "CSV to Parquet conversion and Parquet to GSC complete"}

# run: uvicorn app.main:app --reload