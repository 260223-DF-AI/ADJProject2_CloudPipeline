from fastapi import FastAPI, Query, Depends, HTTPException
from .routers import apiroutes
from .upload import csv_to_parquet, parquet_to_gcs
from dotenv import load_dotenv
from google.cloud import bigquery
import os
import time

import logging

load_dotenv()

# implementing logger functionality
logger = logging.getLogger(__name__)
console_handler = logging.StreamHandler()
file_handler = logging.FileHandler('reporting.log')
fomatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')

logger.setLevel(logging.INFO)

console_handler.setFormatter(fomatter)
file_handler.setFormatter(fomatter)

logger.addHandler(console_handler)
logger.addHandler(file_handler)

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

@app.post("/convert")
def post_root():
    FILE_PATHS = [os.getenv("SALES_CSV1"), os.getenv("SALES_CSV2"), os.getenv("SALES_CSV3"), os.getenv("SALES_CSV4"), os.getenv("SALES_CSV5")]
    OUTPUT_FILE = os.getenv("PARQUET_FILE")
    csv_to_parquet(FILE_PATHS, OUTPUT_FILE)
    start = time.time()
    parquet_to_gcs(OUTPUT_FILE)
    totalTime = time.time() - start
    logger.info(f"Parquet to GCS bucket complete, time taken: {totalTime} seconds")
    return {"message": "CSV to Parquet conversion and Parquet to GCS bucket complete"}

@app.get("/creating_table")
def query():
    query_statement = f"""
                        create or replace external table `project2-cloudpipeline.sales_dataset.sales-data`
                        options (
                        format = 'PARQUET',
                        uris = ['gs://project2_files_adj/sales-data.parquet'])
                        """
    try:
        start = time.time()
        bq_client = bigquery.Client()
        query_job = bq_client.query(query_statement)
        result = query_job.result().to_dataframe()
        # return {"message": "sales-data table created"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"BigQuery query failed: {e}")
    totalTime = time.time() - start
    logger.info(f"Table Created, time taken: {totalTime} seconds")
    return {"message": "sales-data table created"}

@app.delete("/delete-table")
def remove_table():
    table_id = "project2-cloudpipeline.sales_dataset.sales-data"
    
    try:
        start = time.time()
        bq_client = bigquery.Client()
        bq_client.delete_table(table_id, not_found_ok=True)
    except Exception as e:
        logger.info(f"Exception caught: {e}")
        raise Exception
    totalTime = time.time() - start
    logger.info(f"Table deleted, time taken: {totalTime} seconds")
    return {"message": "sales-data table deleted"}

def prep_query(customerid):
    """Format the SQL query we want to run into BigQuery"""
    table_id = "project2-cloudpipeline.sales_dataset.sales-data"
    query = f"""
            SELECT CustomerID, SAFE.PARSE_DATE("%Y-%m-%d", Date) as Transaction_Date, ProductName, Quantity, TotalAmount
            FROM `{table_id}`
            WHERE CustomerID = @customerid
            order by Transaction_Date desc
            limit 10;
            """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("customerid", "STRING", customerid)
        ]
    )
    return query, job_config

@app.get("/query")
async def get_item_test(CustomerID, bq_client: bigquery.client.Client = Depends(get_bq_client)):
    """
    Handles GET requests to the gcs-query URL and queries BigQuery.
    """
    query, job_config = prep_query(CustomerID)
    try:
        start = time.time()
        bq_client = bigquery.Client()
        query_job = bq_client.query(query, job_config)
        result = query_job.result().to_dataframe()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"BigQuery query failed: {e}")
    totalTime = time.time() - start
    logger.info(f"Customer Recent Transaction Lookup, time taken: {totalTime} seconds")
    return result.to_dict(orient="records")

@app.get("/top-n-products-by-quantity")
def get_top_N_products(n: int):
    query = """
        SELECT ProductName, SUM(Quantity) AS Quantity
        FROM `project2-cloudpipeline.sales_dataset.sales-data`
        GROUP BY ProductName
        ORDER BY Quantity DESC
        LIMIT @n
    """
    try:
        start = time.time()
        bq_client = bigquery.Client()
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("n", "INT64", n)
            ]
        )
        query_job = bq_client.query(query, job_config=job_config)
        result = query_job.result().to_dataframe()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"BigQuery query failed: {e}")
    totalTime = time.time() - start
    logger.info(f"Top N Products (Quantity), time taken: {totalTime} seconds")
    return result.to_dict(orient="records")

@app.get("/top-n-products-by-revenue")
def get_top_N_products_rev(n: int):
    query = """
        SELECT ProductName, SUM(TotalAmount) AS Total_Sales
        FROM `project2-cloudpipeline.sales_dataset.sales-data`
        GROUP BY ProductName
        ORDER BY Total_Sales DESC
        LIMIT @n
    """
    try:
        start = time.time()
        bq_client = bigquery.Client()
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("n", "INT64", n)
            ]
        )
        query_job = bq_client.query(query, job_config=job_config)
        result = query_job.result().to_dataframe()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"BigQuery query failed: {e}")
    totalTime = time.time() - start
    logger.info(f"Top N Products (Revenue), time taken: {totalTime} seconds")
    return result.to_dict(orient="records")

@app.get("/sales-by-region")
def get_sales_region(bq_client: bigquery.client.Client = Depends(get_bq_client)):
    """
    Handles GET requests to the gcs-query URL and queries BigQuery for sales by region.
    """
    query = f"""
            SELECT Region, SUM(TotalAmount) AS Total_Sales
            FROM `project2-cloudpipeline.sales_dataset.sales-data`
            GROUP BY Region
            ORDER BY total_sales DESC;
            """
    try:
        start = time.time()
        bq_client = bigquery.Client()
        query_job = bq_client.query(query)
        result = query_job.result().to_dataframe()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"BigQuery query failed: {e}")
    totalTime = time.time() - start
    logger.info(f"Sales by Region Query, time taken: {totalTime} seconds")
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
        start = time.time()
        bq_client = bigquery.Client()
        query_job = bq_client.query(query)
        result = query_job.result().to_dataframe()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"BigQuery query failed: {e}")
    totalTime = time.time() - start
    logger.info(f"Total File Length Query, time taken: {totalTime} seconds")
    return result.to_dict(orient="records")